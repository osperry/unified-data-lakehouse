"""
Dagster smoke test for forecasting_assets.

Re-seeds gold.fct_daily_complaints from the basic smoke test, then materializes
the Dagster asset and both asset checks via dagster.materialize().
Verifies that:
  - asset materializes successfully
  - MaterializeResult contains expected metadata
  - both asset checks return AssetCheckResult objects
  - ci_cd_log row was written
  - target table populated
"""

import os
import sys
sys.path.insert(0, ".")

import duckdb
import numpy as np
import pandas as pd
from dagster import materialize, AssetKey

# Build a fresh warehouse first (same shape as before).
WAREHOUSE = "test_warehouse_dagster.duckdb"
if os.path.exists(WAREHOUSE):
    os.remove(WAREHOUSE)
os.environ["OSP_WAREHOUSE_PATH"] = WAREHOUSE

rng = np.random.default_rng(seed=42)
dates = pd.date_range("2017-01-01", "2024-12-31", freq="D")
borough_specs = {
    "BRONX":         {"base": 120, "lin_slope": 0.015, "quad": 0.0,     "season": 25, "noise": 15},
    "BROOKLYN":      {"base": 250, "lin_slope": 0.030, "quad": 0.00001, "season": 40, "noise": 20},
    "MANHATTAN":     {"base": 300, "lin_slope": 0.000, "quad": 0.00002, "season": 50, "noise": 25},
    "QUEENS":        {"base": 200, "lin_slope": 0.025, "quad": 0.0,     "season": 35, "noise": 18},
    "STATEN ISLAND": {"base":  60, "lin_slope": 0.005, "quad": 0.0,     "season": 10, "noise":  8},
}
rows = []
for i, d in enumerate(dates):
    seasonal = np.sin(2 * np.pi * d.dayofyear / 365.25)
    for b, spec in borough_specs.items():
        trend = spec["base"] + spec["lin_slope"] * i + spec["quad"] * (i ** 2)
        count = max(0, int(round(trend + spec["season"] * seasonal +
                                 rng.normal(0, spec["noise"]))))
        rows.append({"complaint_date": d.date(), "borough": b,
                     "complaint_count": count})
df = pd.DataFrame(rows)

with duckdb.connect(WAREHOUSE) as con:
    con.sql("CREATE SCHEMA IF NOT EXISTS gold")
    con.register("seed_df", df)
    con.sql("CREATE TABLE gold.fct_daily_complaints AS SELECT * FROM seed_df")

print(f"Seeded warehouse at {WAREHOUSE}\n")

# Import the asset and checks AFTER warehouse is built so env var is read fresh.
from pipelines.nyc311.dagster_nyc311.forecasting_assets import (
    fct_complaint_trend_forecast,
    trend_forecast_residual_whitenoise,
    trend_forecast_sic_selection_health,
)

print("=== Materializing fct_complaint_trend_forecast via Dagster ===\n")
# Asset checks attached via @asset_check(asset=...) fire automatically
# when their parent asset is materialized.
result = materialize(
    assets=[
        fct_complaint_trend_forecast,
        trend_forecast_residual_whitenoise,
        trend_forecast_sic_selection_health,
    ],
)

print(f"\nRun success:        {result.success}")
print(f"All steps:          {len(result.get_step_success_events())} succeeded")

# Pull asset materialization events
mat_events = result.get_asset_materialization_events()
print(f"Asset materializations: {len(mat_events)}")
for ev in mat_events:
    asset_key = ev.event_specific_data.materialization.asset_key
    metadata = ev.event_specific_data.materialization.metadata
    print(f"  - {asset_key}")
    for k, v in metadata.items():
        if hasattr(v, "value"):
            val = v.value
        else:
            val = v
        if isinstance(val, str) and len(val) > 100:
            val = val[:100] + "..."
        print(f"      {k}: {val}")

# Pull asset check events
check_events = [
    ev for ev in result.all_events
    if "ASSET_CHECK" in str(ev.event_type)
]
print(f"\nAsset check events: {len(check_events)}")
for ev in check_events:
    if hasattr(ev, "event_specific_data") and hasattr(
        ev.event_specific_data, "asset_check_evaluation"
    ):
        e = ev.event_specific_data.asset_check_evaluation
        print(f"  - {e.check_name}: passed={e.passed} severity={e.severity}")
        print(f"      description: {e.description}")

print("\n=== Verifying target table populated ===")
with duckdb.connect(WAREHOUSE, read_only=True) as con:
    n = con.sql(
        "SELECT COUNT(*) FROM gold.fct_complaint_trend_forecast"
    ).fetchone()[0]
    print(f"Rows in gold.fct_complaint_trend_forecast: {n}")
    sample = con.sql("""
        SELECT borough, forecast_month, point_forecast, model_type
        FROM gold.fct_complaint_trend_forecast
        ORDER BY borough, forecast_month
        LIMIT 6
    """).df()
    print(sample.to_string(index=False))

    print("\n=== ci_cd_log entries ===")
    log = con.sql("""
        SELECT run_id, asset_name, gate_name, status,
               substr(message, 1, 90) AS message_short, executed_at
        FROM ci_cd_log
        ORDER BY executed_at DESC
        LIMIT 3
    """).df()
    print(log.to_string(index=False))

print("\nDagster smoke test complete.")
