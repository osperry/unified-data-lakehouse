"""
gates

CI/CD gate library. Eight gates that validate Gold assets before R2 promotion.
Attached to assets via Dagster @asset_check (see pipelines/.../gates_assets.py).
"""

from .core import (
    BaseGate,
    GateMetrics,
    GateResult,
    GateStatus,
    ensure_ci_cd_log,
    log_gate_result,
)
from .cost_gate import CostGate
from .data_quality_gate import DataQualityGate
from .promotion_gate import PromotionGate, REQUIRED_GATES
from .quarantine_gate import QuarantineGate
from .reconciliation_gate import ReconciliationGate
from .schema_gate import SchemaGate
from .security_gate import SecurityGate
from .validation_gate import ValidationGate

__all__ = [
    "BaseGate",
    "GateMetrics",
    "GateResult",
    "GateStatus",
    "ensure_ci_cd_log",
    "log_gate_result",
    "CostGate",
    "DataQualityGate",
    "PromotionGate",
    "REQUIRED_GATES",
    "QuarantineGate",
    "ReconciliationGate",
    "SchemaGate",
    "SecurityGate",
    "ValidationGate",
]
