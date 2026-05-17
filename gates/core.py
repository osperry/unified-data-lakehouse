"""
gates/core.py

Core CI/CD gate infrastructure. Defines the abstract base class, result schema,
status enum, and the shared logger that writes to ci_cd_log.

Every gate inherits BaseGate and implements .run() -> GateResult. Gates are
attached to Gold assets via Dagster asset_checks (see gates_assets.py).

Pattern:
    PASS  -> dataset is promotion-eligible from this gate's perspective
    WARN  -> upload proceeds, but Promotion Agent must downgrade to REVIEW
    FAIL  -> blocking; downstream R2 upload is skipped

All gate results stream into the ci_cd_log table for the Promotion Dashboard.
"""

import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

import duckdb


# ---------------------------------------------------------------------------
# Status + result schema
# ---------------------------------------------------------------------------

class GateStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class GateMetrics:
    duration_seconds: float
    record_count: Optional[int] = None
    threshold_value: Optional[float] = None
    actual_value: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GateResult:
    gate_name: str
    asset_name: str
    status: GateStatus
    message: str
    metrics: GateMetrics
    executed_at: datetime = field(default_factory=datetime.utcnow)
    run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "asset_name": self.asset_name,
            "status": self.status.value,
            "message": self.message,
            "metrics": self.metrics.to_dict(),
            "executed_at": self.executed_at.isoformat(),
            "run_id": self.run_id,
        }


# ---------------------------------------------------------------------------
# Base gate
# ---------------------------------------------------------------------------

class BaseGate(ABC):
    """All gates inherit this. Implement .run() to produce a GateResult."""
    gate_name: str = "base"

    def __init__(self, asset_name: str, warehouse_path: str, run_id: Optional[str] = None):
        self.asset_name = asset_name
        self.warehouse_path = warehouse_path
        self.run_id = run_id

    @abstractmethod
    def run(self) -> GateResult:
        ...

    # Helpers ----------------------------------------------------------------

    def _pass(self, message: str, started: float, **extra) -> GateResult:
        return GateResult(
            gate_name=self.gate_name,
            asset_name=self.asset_name,
            status=GateStatus.PASS,
            message=message,
            metrics=GateMetrics(duration_seconds=time.time() - started, **extra),
            run_id=self.run_id,
        )

    def _warn(self, message: str, started: float, **extra) -> GateResult:
        return GateResult(
            gate_name=self.gate_name,
            asset_name=self.asset_name,
            status=GateStatus.WARN,
            message=message,
            metrics=GateMetrics(duration_seconds=time.time() - started, **extra),
            run_id=self.run_id,
        )

    def _fail(self, message: str, started: float, **extra) -> GateResult:
        return GateResult(
            gate_name=self.gate_name,
            asset_name=self.asset_name,
            status=GateStatus.FAIL,
            message=message,
            metrics=GateMetrics(duration_seconds=time.time() - started, **extra),
            run_id=self.run_id,
        )


# ---------------------------------------------------------------------------
# ci_cd_log integration
# ---------------------------------------------------------------------------

def ensure_ci_cd_log(con: duckdb.DuckDBPyConnection, log_table: str = "ci_cd_log") -> None:
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table} (
            run_id       VARCHAR,
            asset_name   VARCHAR,
            gate_name    VARCHAR,
            status       VARCHAR,
            message      VARCHAR,
            metrics      JSON,
            executed_at  TIMESTAMP,
            logged_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def log_gate_result(warehouse_path: str, result: GateResult, log_table: str = "ci_cd_log") -> None:
    with duckdb.connect(warehouse_path) as con:
        ensure_ci_cd_log(con, log_table)
        con.execute(
            f"""
            INSERT INTO {log_table}
            (run_id, asset_name, gate_name, status, message, metrics, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                result.run_id,
                result.asset_name,
                result.gate_name,
                result.status.value,
                result.message,
                json.dumps(result.metrics.to_dict(), default=str),
                result.executed_at,
            ],
        )
