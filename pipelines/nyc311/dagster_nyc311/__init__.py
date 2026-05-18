from dagster import Definitions, asset

from .forecasting_assets import (
    fct_complaint_trend_forecast,
    trend_forecast_residual_whitenoise,
    trend_forecast_sic_selection_health,
    trend_forecast_job,
    trend_forecast_schedule,
)

from .gates_assets import ALL_CHECKS as GATE_CHECKS, gates_job


@asset(
    key=["gold", "fct_daily_complaints"],
    group_name="gold",
    description="Existing NYC 311 Gold table in DuckDB. Registered so CI/CD gate checks can attach to it.",
)
def fct_daily_complaints():
    """
    Placeholder Dagster asset for the existing DuckDB Gold table.

    The actual table is built by the NYC 311 pipeline/dbt flow.
    This asset registration lets Dagster resolve gates_job and attach
    asset checks to gold.fct_daily_complaints.
    """
    return None


defs = Definitions(
    assets=[
        fct_daily_complaints,
        fct_complaint_trend_forecast,
    ],
    asset_checks=[
        trend_forecast_residual_whitenoise,
        trend_forecast_sic_selection_health,
        *GATE_CHECKS,
    ],
    jobs=[
        trend_forecast_job,
        gates_job,
    ],
    schedules=[
        trend_forecast_schedule,
    ],
)
