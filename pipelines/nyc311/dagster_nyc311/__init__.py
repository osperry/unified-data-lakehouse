from dagster import Definitions

from .forecasting_assets import (
    fct_complaint_trend_forecast,
    trend_forecast_residual_whitenoise,
    trend_forecast_sic_selection_health,
    trend_forecast_job,
    trend_forecast_schedule,
)

from .gates_assets import ALL_CHECKS as GATE_CHECKS, gates_job

defs = Definitions(
    assets=[
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
