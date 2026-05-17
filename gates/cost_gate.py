"""
gates/cost_gate.py

Validates that the materialized asset is within its storage budget.
FAIL: estimated Parquet size exceeds hard cap.
WARN: size exceeds soft cap.
PASS: within budget.

Estimate is based on DuckDB row count multiplied by a configurable bytes/row.
"""

import os
import time
from typing import Optional

import duckdb

from .core import BaseGate, GateResult


class CostGate(BaseGate):
    gate_name = "cost"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        soft_cap_mb: float = 500.0,
        hard_cap_mb: float = 1500.0,
        bytes_per_row_estimate: float = 256.0,
        export_path: Optional[str] = None,
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.soft_cap_mb = soft_cap_mb
        self.hard_cap_mb = hard_cap_mb
        self.bytes_per_row_estimate = bytes_per_row_estimate
        self.export_path = export_path

    def _measured_size_mb(self) -> Optional[float]:
        if self.export_path and os.path.exists(self.export_path):
            return os.path.getsize(self.export_path) / 1024 / 1024
        return None

    def run(self) -> GateResult:
        started = time.time()
        measured = self._measured_size_mb()

        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                rows = con.execute(f"SELECT COUNT(*) FROM {self.asset_name}").fetchone()[0]
        except Exception as e:
            return self._fail(f"Cost gate count error: {e}", started)

        if measured is not None:
            size_mb = measured
            source = f"measured from {self.export_path}"
        else:
            size_mb = (rows * self.bytes_per_row_estimate) / 1024 / 1024
            source = f"estimated at {self.bytes_per_row_estimate} bytes/row"

        details = {"rows": rows, "size_mb": round(size_mb, 2), "source": source}

        if size_mb > self.hard_cap_mb:
            return self._fail(
                f"Size {size_mb:.1f} MB exceeds hard cap {self.hard_cap_mb} MB ({source})",
                started,
                threshold_value=self.hard_cap_mb,
                actual_value=size_mb,
                details=details,
            )
        if size_mb > self.soft_cap_mb:
            return self._warn(
                f"Size {size_mb:.1f} MB exceeds soft cap {self.soft_cap_mb} MB ({source})",
                started,
                threshold_value=self.soft_cap_mb,
                actual_value=size_mb,
                details=details,
            )

        return self._pass(
            f"Size {size_mb:.1f} MB within budget (soft cap {self.soft_cap_mb} MB)",
            started,
            record_count=rows,
            actual_value=size_mb,
            details=details,
        )
