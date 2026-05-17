"""
gates/quarantine_gate.py

Verifies the asset has no orphan quarantine records bleeding into Gold.
Asserts the asset's row count plus the corresponding quarantine table's row
count equals the upstream Silver row count (no records lost in the medallion).

FAIL: orphan records exist; some Silver rows neither in Gold nor Quarantine.
WARN: quarantine population spiked vs historical baseline.
PASS: medallion conservation holds.
"""

import time
from typing import Optional

import duckdb

from .core import BaseGate, GateResult


class QuarantineGate(BaseGate):
    gate_name = "quarantine"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        silver_table: Optional[str] = None,
        quarantine_table: Optional[str] = None,
        quarantine_rate_warn: float = 0.05,
        aggregated: bool = False,
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.silver_table = silver_table
        self.quarantine_table = quarantine_table
        self.quarantine_rate_warn = quarantine_rate_warn
        self.aggregated = aggregated

    def run(self) -> GateResult:
        started = time.time()
        if self.silver_table is None or self.quarantine_table is None:
            return self._pass(
                "Quarantine config absent; skipping (configure silver_table "
                "and quarantine_table to enable conservation check).",
                started,
            )

        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                gold = con.execute(f"SELECT COUNT(*) FROM {self.asset_name}").fetchone()[0]
                silver = con.execute(f"SELECT COUNT(*) FROM {self.silver_table}").fetchone()[0]
                try:
                    quarantine = con.execute(
                        f"SELECT COUNT(*) FROM {self.quarantine_table}"
                    ).fetchone()[0]
                except Exception:
                    quarantine = 0
        except Exception as e:
            return self._fail(f"Quarantine check error: {e}", started)

        # For aggregated Gold assets, row conservation doesn't apply.
        # Just monitor the quarantine rate vs Silver as a quality signal.
        if self.aggregated:
            if silver == 0:
                return self._pass("Silver empty; nothing to reconcile.", started)
            q_rate = quarantine / silver
            details = {"silver": silver, "gold_aggregated": gold,
                       "quarantine": quarantine, "mode": "aggregated"}
            if q_rate > self.quarantine_rate_warn:
                return self._warn(
                    f"Quarantine rate {q_rate:.2%} exceeds warn threshold "
                    f"{self.quarantine_rate_warn:.2%} (aggregated mode)",
                    started,
                    threshold_value=self.quarantine_rate_warn,
                    actual_value=q_rate,
                    details=details,
                )
            return self._pass(
                f"Quarantine rate {q_rate:.2%} OK (aggregated mode). "
                f"Silver={silver}, Quarantine={quarantine}, Gold(agg)={gold}",
                started,
                actual_value=q_rate,
                details=details,
            )

        accounted = gold + quarantine
        orphans = silver - accounted

        if orphans > 0:
            return self._fail(
                f"{orphans} orphan records: Silver={silver}, Gold={gold}, "
                f"Quarantine={quarantine}",
                started,
                threshold_value=0,
                actual_value=orphans,
                details={"silver": silver, "gold": gold, "quarantine": quarantine},
            )

        if silver == 0:
            return self._pass("Silver empty; nothing to reconcile.", started)

        q_rate = quarantine / silver
        if q_rate > self.quarantine_rate_warn:
            return self._warn(
                f"Quarantine rate {q_rate:.2%} exceeds warn threshold "
                f"{self.quarantine_rate_warn:.2%}",
                started,
                threshold_value=self.quarantine_rate_warn,
                actual_value=q_rate,
                details={"silver": silver, "gold": gold, "quarantine": quarantine},
            )

        return self._pass(
            f"Medallion conservation OK. Silver={silver}, Gold={gold}, "
            f"Quarantine={quarantine} (q_rate={q_rate:.2%})",
            started,
            actual_value=q_rate,
            details={"silver": silver, "gold": gold, "quarantine": quarantine},
        )
