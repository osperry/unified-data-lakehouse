"""
forecasting_assets.py

Dagster wrapper around ml_validation_layer.forecasting.trend_model.

Materializes gold.fct_complaint_trend_forecast as a downstream asset of
gold.fct_daily_complaints. Fires automatically after the 9:00 AM Gold build
via a 9:35 AM schedule (after the 9:30 AM ML validation layer settles).

Includes two asset checks:
  - trend_forecast_residual_whitenoise: blocks if Ljung-Box rejects on the
    selected model for any borough (signals trend alone is insufficient).
  - trend_forecast_sic_selection_health: warns when SIC differences are tiny
    across the slate, which suggests under-identified model selection.

Wire-in:
    1. Drop this file into pipelines/nyc311/dagster_nyc311/.
    2. Add `forecasting_assets` to your Definitions object alongside existing
       gold assets. Example shown in `defs` at the bottom of this file.
    3. Confirm ml_validation_layer.forecasting is importable from this repo
       (add to PYTHONPATH or install the package via `pip install -e .`).
"""

import os
from datetime import datetime
from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from ml_validation_layer.forecasting.trend_model import run_trend_forecast


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WAREHOUSE_PATH_ENV = "OSP_WAREHOUSE_PATH"
DEFAULT_WAREHOUSE_PATH = "data/warehouse.duckdb"

SOURCE_TABLE = "gold.fct_daily_complaints"
TARGET_TABLE = "gold.fct_complaint_trend_forecast"
FORECAST_HORIZON_MONTHS = 12
PI_LEVEL = 0.95


def _resolve_warehouse() -> str:
    return os.environ.get(WAREHOUSE_PATH_ENV, DEFAULT_WAREHOUSE_PATH)


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------

@asset(
    name="fct_complaint_trend_forecast",
    key_prefix=["gold"],
    deps=[AssetKey(["gold", "fct_daily_complaints"])],
    group_name="forecasting",
    description=(
        "12-month complaint volume forecast per borough. "
        "Linear vs quadratic trend selection via SIC (Diebold Ch. 4). "
        "Writes to gold.fct_complaint_trend_forecast; logs to ci_cd_log."
    ),
    compute_kind="python",
)
def fct_complaint_trend_forecast(context: AssetExecutionContext) -> MaterializeResult:
    """Materialize the trend forecast Gold table."""
    warehouse = _resolve_warehouse()
    run_id = f"dagster_{context.run_id[:8]}_{datetime.utcnow().strftime('%H%M%S')}"

    context.log.info(f"Running trend forecast against {warehouse}")
    result = run_trend_forecast(
        warehouse_path=warehouse,
        source_table=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        forecast_horizon=FORECAST_HORIZON_MONTHS,
        pi_level=PI_LEVEL,
        run_id=run_id,
    )

    # Build a compact per-borough summary for the Dagster UI.
    borough_md_rows = []
    for br in result["borough_results"]:
        chosen = br["selected_model"]
        fit = br[f"fit_{chosen}"]
        borough_md_rows.append(
            f"| {br['borough']} | {chosen} | "
            f"{fit['aic']:.1f} | {fit['sic']:.1f} | {fit['rmse']:.1f} | "
            f"{fit['residual_whitenoise_pvalue']:.3f} |"
        )
    borough_md = "\n".join([
        "| Borough | Model | AIC | SIC | RMSE | LB p-value |",
        "| --- | --- | --- | --- | --- | --- |",
        *borough_md_rows,
    ])

    return MaterializeResult(
        metadata={
            "run_id": result["run_id"],
            "status": result["status"],
            "message": result["message"],
            "boroughs_processed": result["boroughs_processed"],
            "duration_seconds": result["duration_seconds"],
            "horizon_months": FORECAST_HORIZON_MONTHS,
            "pi_level": PI_LEVEL,
            "borough_fits": MetadataValue.md(borough_md),
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
        }
    )


# ---------------------------------------------------------------------------
# Asset checks
# ---------------------------------------------------------------------------

@asset_check(
    asset=fct_complaint_trend_forecast,
    name="trend_forecast_residual_whitenoise",
    description=(
        "Asserts Ljung-Box residuals look like white noise on the selected "
        "model for every borough. Failure means trend alone is insufficient; "
        "next step is seasonality (Diebold Ch. 5) or ARMA (Ch. 6-7)."
    ),
    blocking=False,  # advisory; do not block downstream materialization yet
)
def trend_forecast_residual_whitenoise(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    import duckdb

    warehouse = _resolve_warehouse()
    with duckdb.connect(warehouse, read_only=True) as con:
        row = con.execute("""
            SELECT run_id, status, message, metrics
            FROM ci_cd_log
            WHERE gate_name = 'trend_forecast'
            ORDER BY executed_at DESC
            LIMIT 1
        """).fetchone()

    if row is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="No trend_forecast run found in ci_cd_log.",
        )

    _, status, message, _ = row
    passed = (status == "pass")
    severity = AssetCheckSeverity.ERROR if status == "fail" else AssetCheckSeverity.WARN

    return AssetCheckResult(
        passed=passed,
        severity=severity,
        description=message,
        metadata={"status": status, "ci_cd_log_message": message},
    )


@asset_check(
    asset=fct_complaint_trend_forecast,
    name="trend_forecast_sic_selection_health",
    description=(
        "Flags when SIC differences between linear and quadratic are within "
        "the tiebreak band for every borough, which indicates the trend "
        "shape is under-identified."
    ),
    blocking=False,
)
def trend_forecast_sic_selection_health(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    import json
    import duckdb

    warehouse = _resolve_warehouse()
    with duckdb.connect(warehouse, read_only=True) as con:
        row = con.execute("""
            SELECT metrics
            FROM ci_cd_log
            WHERE gate_name = 'trend_forecast'
            ORDER BY executed_at DESC
            LIMIT 1
        """).fetchone()

    if row is None or row[0] is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="No metrics row found for trend_forecast.",
        )

    metrics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    deltas = []
    for br in metrics.get("boroughs", []):
        delta = br["fit_linear"]["sic"] - br["fit_quadratic"]["sic"]
        deltas.append(abs(delta))

    all_weak = bool(deltas) and all(d < 2.0 for d in deltas)
    return AssetCheckResult(
        passed=not all_weak,
        severity=AssetCheckSeverity.WARN,
        description=(
            "All borough SIC deltas under tiebreak threshold; trend shape "
            "is under-identified." if all_weak else
            "SIC selection has clear winners on at least one borough."
        ),
        metadata={
            "sic_deltas_abs": [round(d, 3) for d in deltas],
        },
    )


# ---------------------------------------------------------------------------
# Job + schedule
# ---------------------------------------------------------------------------

trend_forecast_job = define_asset_job(
    name="trend_forecast_job",
    selection=[
        AssetKey(["gold", "fct_complaint_trend_forecast"]),
    ],
    description="Materialize the trend forecast Gold asset and run its checks.",
)

# 9:35 AM daily, after Gold build (9:00) and ML validation layer (9:30).
trend_forecast_schedule = ScheduleDefinition(
    name="trend_forecast_daily",
    job=trend_forecast_job,
    cron_schedule="35 9 * * *",
    execution_timezone="America/Chicago",
)


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------
#
# Merge these into your existing Definitions object in
# pipelines/nyc311/dagster_nyc311/__init__.py. Example shape below.
#
defs = Definitions(
    assets=[fct_complaint_trend_forecast],
    asset_checks=[
        trend_forecast_residual_whitenoise,
        trend_forecast_sic_selection_health,
    ],
    jobs=[trend_forecast_job],
    schedules=[trend_forecast_schedule],
)
