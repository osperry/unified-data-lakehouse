"""
gates/schema_gate.py

Validates that an asset's actual schema matches its declared contract.
FAIL: missing required columns, type mismatches.
WARN: extra columns present (additive change; review before promotion).
PASS: schema matches contract exactly.
"""

import time
from typing import Dict, Optional

import duckdb

from .core import BaseGate, GateResult


class SchemaGate(BaseGate):
    gate_name = "schema"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        expected_columns: Optional[Dict[str, str]] = None,
        run_id: Optional[str] = None,
    ):
        """
        expected_columns maps column_name -> expected_type, e.g.
            {"complaint_date": "DATE", "borough": "VARCHAR", "complaint_count": "BIGINT"}
        """
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.expected_columns = expected_columns or {}

    def run(self) -> GateResult:
        started = time.time()
        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                rows = con.execute(f"PRAGMA table_info('{self.asset_name}')").fetchall()
        except Exception as e:
            return self._fail(f"Schema introspection error: {e}", started)

        if not rows:
            return self._fail(f"Table '{self.asset_name}' not found in warehouse", started)

        actual = {r[1]: str(r[2]).upper() for r in rows}

        if not self.expected_columns:
            return self._pass(
                f"Schema readable; {len(actual)} columns. No contract supplied.",
                started,
                record_count=len(actual),
            )

        missing = [c for c in self.expected_columns if c not in actual]
        if missing:
            return self._fail(
                f"Missing required columns: {missing}",
                started,
                details={"missing_columns": missing, "actual_columns": list(actual.keys())},
            )

        mismatches = []
        for col, expected_type in self.expected_columns.items():
            if actual[col] != expected_type.upper():
                mismatches.append(f"{col} expected {expected_type}, got {actual[col]}")
        if mismatches:
            return self._fail(
                f"Type mismatches: {'; '.join(mismatches)}",
                started,
                details={"mismatches": mismatches},
            )

        extras = [c for c in actual if c not in self.expected_columns]
        if extras:
            return self._warn(
                f"Extra columns detected: {extras}. Additive change; review before promotion.",
                started,
                record_count=len(actual),
                details={"extra_columns": extras},
            )

        return self._pass(
            f"Schema valid. {len(actual)} columns; matches contract.",
            started,
            record_count=len(actual),
        )
