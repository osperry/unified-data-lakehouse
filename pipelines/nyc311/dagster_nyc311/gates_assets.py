"""
pipelines/nyc311/dagster_nyc311/gates_assets.py

Wires the 8 gates onto Gold assets as Dagster asset_checks.

Pattern: each gate is invoked from inside an @asset_check function that knows
which underlying gate class to construct and how to configure it for the given
asset. Result is converted to AssetCheckResult and ALSO logged to ci_cd_log so
the Promotion Dashboard renders it.

Asset checks attached here:
  - For each Gold asset in GOLD_ASSET_CONFIGS, all 8 gates are attached.
  - schema, data_quality, validation, quarantine, reconciliation, cost,
    security are advisory (non-blocking) until you flip blocking=True per gate.
  - promotion is the final composite; it reads ci_cd_log for the current run.
"""

import os
from datetime import datetime
from typing import Any, Dict

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    Definitions,
    asset_check,
    define_asset_job,
)

from gates import (
    CostGate,
    DataQualityGate,
    PromotionGate,
    QuarantineGate,
    ReconciliationGate,
    SchemaGate,
    SecurityGate,
    ValidationGate,
    log_gate_result,
)


WAREHOUSE_PATH_ENV = "OSP_WAREHOUSE_PATH"
DEFAULT_WAREHOUSE_PATH = "data/warehouse.duckdb"


def _warehouse() -> str:
    return os.environ.get(WAREHOUSE_PATH_ENV, DEFAULT_WAREHOUSE_PATH)


def _run_id_from_context(context: AssetCheckExecutionContext) -> str:
    return f"dagster_{context.run_id[:8]}"


def _to_check_result(result) -> AssetCheckResult:
    severity = (
        AssetCheckSeverity.ERROR
        if result.status.value == "fail"
        else AssetCheckSeverity.WARN
    )
    return AssetCheckResult(
        passed=(result.status.value == "pass"),
        severity=severity,
        description=result.message,
        metadata={
            "gate": result.gate_name,
            "status": result.status.value,
            "duration_seconds": result.metrics.duration_seconds,
            **{k: v for k, v in result.metrics.to_dict().items()
               if k not in {"duration_seconds", "details"} and v is not None},
        },
    )


# ---------------------------------------------------------------------------
# Per-asset gate config
# ---------------------------------------------------------------------------

GOLD_ASSET_CONFIGS: Dict[str, Dict[str, Any]] = {
    "gold.fct_daily_complaints": {
        "asset_key": AssetKey(["gold", "fct_daily_complaints"]),
        "schema": {
            "expected_columns": {
                "complaint_date": "DATE",
                "borough": "VARCHAR",
                "complaint_count": "BIGINT",
            },
        },
        "data_quality": {
            "null_rate_warn": 0.02,
            "null_rate_fail": 0.10,
            "critical_columns": ["complaint_date", "borough", "complaint_count"],
            "range_checks": {"complaint_count": (0, 1_000_000)},
        },
        "validation": {"validation_table": "validation_reports", "max_age_hours": 6},
        "quarantine": {
            "silver_table": "silver.stg_complaints",
            "quarantine_table": "silver.quarantine_complaints",
            "quarantine_rate_warn": 0.05,
            "aggregated": True,
        },
        "reconciliation": {
            "bronze_table": "bronze.nyc311_raw",
            "silver_table": "silver.stg_complaints",
            "aggregated": True,
        },
        "cost": {"soft_cap_mb": 100.0, "hard_cap_mb": 500.0},
        "security": {"whitelist_columns": []},
    },
    # Add more Gold assets here using the same config shape.
}


# ---------------------------------------------------------------------------
# Asset check factory
# ---------------------------------------------------------------------------

def _make_checks_for_asset(asset_table: str, cfg: Dict[str, Any]):
    """Build all 8 asset_check functions for a single Gold asset."""
    asset_key = cfg["asset_key"]

    @asset_check(asset=asset_key, name=f"{asset_table}__schema", blocking=False)
    def schema_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = SchemaGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("schema", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__data_quality", blocking=False)
    def dq_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = DataQualityGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("data_quality", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__validation", blocking=False)
    def validation_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = ValidationGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("validation", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__quarantine", blocking=False)
    def quarantine_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = QuarantineGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("quarantine", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__reconciliation", blocking=False)
    def recon_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = ReconciliationGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("reconciliation", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__cost", blocking=False)
    def cost_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = CostGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("cost", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__security", blocking=False)
    def security_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = SecurityGate(asset_table, _warehouse(), run_id=run_id, **cfg.get("security", {}))
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    @asset_check(asset=asset_key, name=f"{asset_table}__promotion", blocking=False)
    def promotion_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        run_id = _run_id_from_context(context)
        gate = PromotionGate(asset_table, _warehouse(), run_id=run_id)
        result = gate.run()
        log_gate_result(_warehouse(), result)
        return _to_check_result(result)

    return [
        schema_check, dq_check, validation_check, quarantine_check,
        recon_check, cost_check, security_check, promotion_check,
    ]


# Build every check for every configured asset.
ALL_CHECKS = []
for table, cfg in GOLD_ASSET_CONFIGS.items():
    ALL_CHECKS.extend(_make_checks_for_asset(table, cfg))


# Job + definitions
gates_job = define_asset_job(
    name="gates_job",
    selection=[cfg["asset_key"] for cfg in GOLD_ASSET_CONFIGS.values()],
    description="Re-run all gates on configured Gold assets.",
)


defs = Definitions(
    asset_checks=ALL_CHECKS,
    jobs=[gates_job],
)
