"""
gates/validation_gate.py

Confirms ML validation layer ran for this asset and produced a recommendation.
FAIL: no validation report found for this asset on the current run.
WARN: validation present but report flags REVIEW state.
PASS: validation present and recommends APPROVE.

Reads from a validation_reports table that the ML validation layer writes to.
Expected schema:
    validation_reports(asset_name VARCHAR, run_id VARCHAR, recommendation VARCHAR,
                       report JSON, created_at TIMESTAMP)
"""

import time
from typing import Optional

import duckdb

from .core import BaseGate, GateResult


class ValidationGate(BaseGate):
    gate_name = "validation"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        validation_table: str = "validation_reports",
        max_age_hours: int = 6,
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.validation_table = validation_table
        self.max_age_hours = max_age_hours

    def run(self) -> GateResult:
        started = time.time()
        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                row = con.execute(
                    f"""
                    SELECT recommendation, created_at
                    FROM {self.validation_table}
                    WHERE asset_name = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    [self.asset_name],
                ).fetchone()
        except Exception as e:
            return self._fail(
                f"Validation lookup error (table may be missing): {e}", started
            )

        if row is None:
            return self._fail(
                f"No validation report found for '{self.asset_name}'",
                started,
            )

        recommendation, created_at = row
        age_hours = (time.time() - created_at.timestamp()) / 3600

        if age_hours > self.max_age_hours:
            return self._fail(
                f"Validation report stale: {age_hours:.1f}h old "
                f"(max {self.max_age_hours}h)",
                started,
                threshold_value=self.max_age_hours,
                actual_value=age_hours,
            )

        rec = (recommendation or "").upper()
        if rec == "APPROVE":
            return self._pass(
                f"Validation report present; recommendation=APPROVE "
                f"(age {age_hours:.1f}h)",
                started,
                details={"recommendation": rec, "age_hours": round(age_hours, 2)},
            )
        if rec == "REVIEW":
            return self._warn(
                f"Validation report flags REVIEW (age {age_hours:.1f}h)",
                started,
                details={"recommendation": rec, "age_hours": round(age_hours, 2)},
            )

        return self._fail(
            f"Validation report flags '{rec}' (age {age_hours:.1f}h)",
            started,
            details={"recommendation": rec, "age_hours": round(age_hours, 2)},
        )
