"""
gates/security_gate.py

Scans Gold tables for unexpected PII, credentials, or sensitive data leakage.

Two layers:
  1. Column name blocklist: column names matching sensitive patterns trigger
     FAIL unless explicitly whitelisted.
  2. Value-level regex sweep on a sample: SSN, email, credit card patterns
     appearing in non-whitelisted columns trigger WARN.

Whitelisted columns are expected sensitive columns that have governance approval
(e.g., 'tenant_email' in a billing table).
"""

import re
import time
from typing import List, Optional

import duckdb

from .core import BaseGate, GateResult


SENSITIVE_NAME_PATTERNS = [
    r"\bssn\b",
    r"social_security",
    r"\bpassword\b",
    r"\bpwd\b",
    r"api_key",
    r"secret",
    r"\btoken\b",
    r"credit_card",
    r"card_number",
    r"\bcvv\b",
    r"passport",
    r"\bdob\b",
    r"date_of_birth",
]

VALUE_PATTERNS = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "email": re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"),
    "credit_card_16": re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
}


class SecurityGate(BaseGate):
    gate_name = "security"

    def __init__(
        self,
        asset_name: str,
        warehouse_path: str,
        whitelist_columns: Optional[List[str]] = None,
        sample_rows: int = 1000,
        run_id: Optional[str] = None,
    ):
        super().__init__(asset_name, warehouse_path, run_id=run_id)
        self.whitelist_columns = set(whitelist_columns or [])
        self.sample_rows = sample_rows

    def run(self) -> GateResult:
        started = time.time()
        try:
            with duckdb.connect(self.warehouse_path, read_only=True) as con:
                col_rows = con.execute(
                    f"PRAGMA table_info('{self.asset_name}')"
                ).fetchall()
                if not col_rows:
                    return self._fail(
                        f"Table '{self.asset_name}' not found", started
                    )

                # Column-name blocklist
                sensitive_named = []
                for r in col_rows:
                    name = r[1]
                    if name in self.whitelist_columns:
                        continue
                    lower = name.lower()
                    for pat in SENSITIVE_NAME_PATTERNS:
                        if re.search(pat, lower):
                            sensitive_named.append({"column": name, "pattern": pat})
                            break

                if sensitive_named:
                    return self._fail(
                        f"Sensitive column names detected: "
                        f"{[s['column'] for s in sensitive_named]}",
                        started,
                        details={"sensitive_columns": sensitive_named},
                    )

                # Value regex sweep on VARCHAR columns only
                varchar_cols = [
                    r[1] for r in col_rows
                    if str(r[2]).upper().startswith("VARCHAR")
                    and r[1] not in self.whitelist_columns
                ]

                value_hits = []
                if varchar_cols:
                    select_clause = ", ".join(f'"{c}"' for c in varchar_cols)
                    sample = con.execute(
                        f"SELECT {select_clause} FROM {self.asset_name} "
                        f"USING SAMPLE {self.sample_rows}"
                    ).fetchall()
                    for col_idx, col in enumerate(varchar_cols):
                        for row in sample:
                            val = row[col_idx]
                            if val is None:
                                continue
                            s = str(val)
                            for kind, pat in VALUE_PATTERNS.items():
                                if pat.search(s):
                                    value_hits.append({"column": col, "kind": kind})
                                    break
                        # collapse duplicates
                value_hits = list({(v["column"], v["kind"]): v for v in value_hits}.values())
        except Exception as e:
            return self._fail(f"Security scan error: {e}", started)

        if value_hits:
            return self._warn(
                f"Sensitive value patterns in {len(value_hits)} column/type combos. "
                f"Confirm intent or move to whitelist.",
                started,
                details={"value_pattern_hits": value_hits},
            )

        return self._pass(
            "No sensitive column names or value patterns detected", started
        )
