"""
gates/data_quality_gate.py

Validates null rates and range constraints on key columns.
FAIL: any column null rate exceeds the configured ceiling.
WARN: null rate elevated but under the failure ceiling.
PASS: all columns within bounds.
"""

import time
from typing import Dict, Optional, Tuple

import duckdb

from .core import BaseGate, GateResult


class DataQualityGate(BaseGate):
    gate_name = "data_quality"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        null_rate_warn: float = 0.02,
        null_rate_fail: float = 0.10,
        range_checks: Optional[Dict[str, Tuple[float, float]]] = None,
        critical_columns: Optional[list] = None,
        run_id: Optional[str] = None,
    ):
        """
        range_checks maps column_name -> (min_allowed, max_allowed).
        critical_columns: columns where null rate must be evaluated (default: all).
        """
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.null_rate_warn = null_rate_warn
        self.null_rate_fail = null_rate_fail
        self.range_checks = range_checks or {}
        self.critical_columns = critical_columns

    def run(self) -> GateResult:
        started = time.time()
        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                total = con.execute(f"SELECT COUNT(*) FROM {self.asset_name}").fetchone()[0]
                if total == 0:
                    return self._fail("Table is empty", started, record_count=0)

                col_rows = con.execute(f"PRAGMA table_info('{self.asset_name}')").fetchall()
                cols = [r[1] for r in col_rows]
                if self.critical_columns:
                    cols = [c for c in cols if c in self.critical_columns]

                worst_col, worst_rate = None, 0.0
                null_rates = {}
                for c in cols:
                    nulls = con.execute(
                        f'SELECT COUNT(*) FROM {self.asset_name} WHERE "{c}" IS NULL'
                    ).fetchone()[0]
                    rate = nulls / total
                    null_rates[c] = round(rate, 5)
                    if rate > worst_rate:
                        worst_rate, worst_col = rate, c

                range_violations = []
                for col, (lo, hi) in self.range_checks.items():
                    violations = con.execute(
                        f'SELECT COUNT(*) FROM {self.asset_name} '
                        f'WHERE "{col}" < ? OR "{col}" > ?',
                        [lo, hi],
                    ).fetchone()[0]
                    if violations > 0:
                        range_violations.append(
                            {"column": col, "range": [lo, hi], "violations": int(violations)}
                        )
        except Exception as e:
            return self._fail(f"Data quality check error: {e}", started)

        if range_violations:
            return self._fail(
                f"Range violations on {len(range_violations)} columns",
                started,
                record_count=total,
                details={"range_violations": range_violations, "null_rates": null_rates},
            )

        if worst_rate >= self.null_rate_fail:
            return self._fail(
                f"Null rate on '{worst_col}' = {worst_rate:.2%} exceeds fail "
                f"threshold {self.null_rate_fail:.2%}",
                started,
                record_count=total,
                threshold_value=self.null_rate_fail,
                actual_value=worst_rate,
                details={"null_rates": null_rates},
            )

        if worst_rate >= self.null_rate_warn:
            return self._warn(
                f"Null rate on '{worst_col}' = {worst_rate:.2%} exceeds warn "
                f"threshold {self.null_rate_warn:.2%}",
                started,
                record_count=total,
                threshold_value=self.null_rate_warn,
                actual_value=worst_rate,
                details={"null_rates": null_rates},
            )

        return self._pass(
            f"Data quality OK across {len(cols)} columns; max null rate {worst_rate:.2%}",
            started,
            record_count=total,
            actual_value=worst_rate,
            details={"null_rates": null_rates},
        )
