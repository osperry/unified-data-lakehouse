"""
gates/reconciliation_gate.py

Validates row count flow across Bronze, Silver, Gold layers.
FAIL: Gold > Silver, or Silver > Bronze (impossible without duplication).
WARN: total attrition exceeds historical baseline threshold.
PASS: counts decay monotonically and stay within attrition budget.
"""

import time
from typing import Optional

import duckdb

from .core import BaseGate, GateResult


class ReconciliationGate(BaseGate):
    gate_name = "reconciliation"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        bronze_table: Optional[str] = None,
        silver_table: Optional[str] = None,
        attrition_threshold: float = 0.15,
        aggregated: bool = False,
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.attrition_threshold = attrition_threshold
        self.aggregated = aggregated

    def _safe_count(self, con: duckdb.DuckDBPyConnection, table: Optional[str]) -> Optional[int]:
        if not table:
            return None
        try:
            return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return None

    def run(self) -> GateResult:
        started = time.time()
        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                gold = self._safe_count(con, self.asset_name)
                silver = self._safe_count(con, self.silver_table)
                bronze = self._safe_count(con, self.bronze_table)
        except Exception as e:
            return self._fail(f"Reconciliation error: {e}", started)

        if gold is None:
            return self._fail(f"Gold asset '{self.asset_name}' not found", started)

        details = {"bronze": bronze, "silver": silver, "gold": gold,
                   "mode": "aggregated" if self.aggregated else "row_level"}

        # For aggregated assets, Gold rows are NOT 1-to-1 with Silver. Skip the
        # Silver-Gold inversion check and the attrition rate calculation.
        if self.aggregated:
            if bronze is not None and silver is not None and silver > bronze:
                return self._fail(
                    f"Duplication: Silver ({silver}) > Bronze ({bronze})",
                    started,
                    threshold_value=bronze,
                    actual_value=silver,
                    details=details,
                )
            return self._pass(
                f"Reconciliation OK (aggregated). "
                f"Bronze={bronze}, Silver={silver}, Gold(agg)={gold}",
                started,
                record_count=gold,
                details=details,
            )

        if silver is not None and gold > silver:
            return self._fail(
                f"Data loss inversion: Gold ({gold}) > Silver ({silver})",
                started,
                threshold_value=silver,
                actual_value=gold,
                details=details,
            )
        if bronze is not None and silver is not None and silver > bronze:
            return self._fail(
                f"Duplication detected: Silver ({silver}) > Bronze ({bronze})",
                started,
                threshold_value=bronze,
                actual_value=silver,
                details=details,
            )

        if bronze and bronze > 0:
            total_attrition = (bronze - gold) / bronze
            if total_attrition > self.attrition_threshold:
                return self._warn(
                    f"Attrition {total_attrition:.1%} exceeds threshold "
                    f"{self.attrition_threshold:.1%}. Bronze={bronze}, "
                    f"Silver={silver}, Gold={gold}",
                    started,
                    threshold_value=self.attrition_threshold,
                    actual_value=total_attrition,
                    details=details,
                )

        return self._pass(
            f"Reconciliation OK. Bronze={bronze}, Silver={silver}, Gold={gold}",
            started,
            record_count=gold,
            details=details,
        )
