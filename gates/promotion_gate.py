"""
gates/promotion_gate.py

Composite gate. Inspects ci_cd_log for the current run_id and asserts every
prior gate returned PASS. Encodes the contract for autonomous promotion.

FAIL: any prior gate FAILed, OR a prior gate is missing entirely.
WARN: any prior gate WARNed; promotion proceeds to R2 but downstream
      Promotion Agent must mark dataset as REVIEW.
PASS: all prior gates PASSed.

This is the gate the Promotion Agent reads to issue APPROVE vs REVIEW.
"""

import time
from typing import List, Optional

import duckdb

from .core import BaseGate, GateResult


REQUIRED_GATES = [
    "schema",
    "data_quality",
    "validation",
    "quarantine",
    "reconciliation",
    "cost",
    "security",
]


class PromotionGate(BaseGate):
    gate_name = "promotion"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        required_gates: Optional[List[str]] = None,
        log_table: str = "ci_cd_log",
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.required_gates = required_gates or REQUIRED_GATES
        self.log_table = log_table

    def run(self) -> GateResult:
        started = time.time()
        if not self.run_id:
            return self._fail(
                "Promotion gate requires a run_id to inspect ci_cd_log", started
            )

        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                rows = con.execute(
                    f"""
                    SELECT gate_name, status, message
                    FROM {self.log_table}
                    WHERE run_id = ? AND asset_name = ?
                      AND gate_name <> 'promotion'
                    """,
                    [self.run_id, self.asset_name],
                ).fetchall()
        except Exception as e:
            return self._fail(f"Promotion gate read error: {e}", started)

        observed = {r[0]: (r[1], r[2]) for r in rows}
        missing = [g for g in self.required_gates if g not in observed]
        if missing:
            return self._fail(
                f"Missing prior gates for run {self.run_id}: {missing}",
                started,
                details={"missing_gates": missing, "observed_gates": list(observed.keys())},
            )

        fails = [g for g, (s, _) in observed.items() if s == "fail"]
        warns = [g for g, (s, _) in observed.items() if s == "warn"]

        if fails:
            return self._fail(
                f"Prior gate FAILs: {fails}",
                started,
                details={"failing_gates": fails, "warning_gates": warns},
            )

        if warns:
            return self._warn(
                f"Prior gate WARNs: {warns}. Promote with REVIEW recommendation.",
                started,
                details={"warning_gates": warns},
            )

        return self._pass(
            f"All {len(self.required_gates)} prior gates PASSed; "
            f"asset is APPROVE-eligible",
            started,
            details={"gates_passed": list(observed.keys())},
        )
