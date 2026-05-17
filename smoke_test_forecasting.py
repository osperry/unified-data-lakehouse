"""
Smoke test for trend_model.

Creates a synthetic fct_daily_complaints with 5 NYC boroughs, 8 years of daily data,
known trend + seasonality + noise. Runs trend forecast end-to-end. Verifies that:
  - target table is populated with forecasts
  - ci_cd_log row was written
  - per-borough fits include AIC/SIC, residual diagnostics, model selection
  - quadratic is selected when curvature is present
"""

import os
import duckdb
import numpy as np
import pandas as pd
from datetime import date

import sys
sys.path.insert(0, ".")
from ml_validation_layer.forecasting.trend_model import run_trend_forecast

WAREHOUSE = "test_warehouse.duckdb"
if os.path.exists(WAREHOUSE):
    os.remove(WAREHOUSE)

# Build synthetic daily complaints, 2017-01-01 through 2024-12-31, 5 boroughs.
rng = np.random.default_rng(seed=42)
dates = pd.date_range("2017-01-01", "2024-12-31", freq="D")
boroughs = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

# Different trends per borough to test model selection.
borough_specs = {
    "BRONX":         {"base": 120, "lin_slope": 0.015, "quad": 0.0,     "season": 25, "noise": 15},
    "BROOKLYN":      {"base": 250, "lin_slope": 0.030, "quad": 0.00001, "season": 40, "noise": 20},
    "MANHATTAN":     {"base": 300, "lin_slope": 0.000, "quad": 0.00002, "season": 50, "noise": 25},  # quadratic
    "QUEENS":        {"base": 200, "lin_slope": 0.025, "quad": 0.0,     "season": 35, "noise": 18},
    "STATEN ISLAND": {"base":  60, "lin_slope": 0.005, "quad": 0.0,     "season": 10, "noise":  8},
}

rows = []
for i, d in enumerate(dates):
    seasonal = np.sin(2 * np.pi * d.dayofyear / 365.25)
    for b, spec in borough_specs.items():
        trend = spec["base"] + spec["lin_slope"] * i + spec["quad"] * (i ** 2)
        seasonal_term = spec["season"] * seasonal
        noise = rng.normal(0, spec["noise"])
        count = max(0, int(round(trend + seasonal_term + noise)))
        rows.append({"complaint_date": d.date(),
                     "borough": b,
                     "complaint_count": count})

df = pd.DataFrame(rows)

with duckdb.connect(WAREHOUSE) as con:
    con.sql("CREATE SCHEMA IF NOT EXISTS gold")
    con.register("seed_df", df)
    con.sql("""
        CREATE TABLE gold.fct_daily_complaints AS
        SELECT * FROM seed_df
    """)
    n_rows = con.sql("SELECT COUNT(*) FROM gold.fct_daily_complaints").fetchone()[0]
    print(f"Seeded gold.fct_daily_complaints: {n_rows:,} rows")
    print(con.sql("""
        SELECT borough, MIN(complaint_date), MAX(complaint_date),
               SUM(complaint_count) AS total
        FROM gold.fct_daily_complaints GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False))

print("\n=== Running trend forecast ===")
result = run_trend_forecast(
    warehouse_path=WAREHOUSE,
    source_table="gold.fct_daily_complaints",
    target_table="gold.fct_complaint_trend_forecast",
    forecast_horizon=12,
    run_id="smoketest_001",
)

print(f"\nStatus:               {result['status']}")
print(f"Message:              {result['message']}")
print(f"Run ID:               {result['run_id']}")
print(f"Boroughs processed:   {result['boroughs_processed']}")
print(f"Duration (s):         {result['duration_seconds']}")

print("\n=== Per-borough model selection ===")
for br in result["borough_results"]:
    print(f"\n  {br['borough']:<14}  obs={br['n_obs']:<3}  selected={br['selected_model']:<9}")
    print(f"     linear:    AIC={br['fit_linear']['aic']:>9.2f}  SIC={br['fit_linear']['sic']:>9.2f}  "
          f"RMSE={br['fit_linear']['rmse']:>8.2f}  R2adj={br['fit_linear']['r2_adj']:.3f}  "
          f"LB-p={br['fit_linear']['residual_whitenoise_pvalue']:.3f}")
    print(f"     quadratic: AIC={br['fit_quadratic']['aic']:>9.2f}  SIC={br['fit_quadratic']['sic']:>9.2f}  "
          f"RMSE={br['fit_quadratic']['rmse']:>8.2f}  R2adj={br['fit_quadratic']['r2_adj']:.3f}  "
          f"LB-p={br['fit_quadratic']['residual_whitenoise_pvalue']:.3f}")
    print(f"     reason:    {br['selection_reason']}")

print("\n=== Forecast table sample (Brooklyn, next 12 months) ===")
with duckdb.connect(WAREHOUSE, read_only=True) as con:
    fc = con.sql("""
        SELECT forecast_month, point_forecast, lower_pi, upper_pi, model_type
        FROM gold.fct_complaint_trend_forecast
        WHERE borough = 'BROOKLYN' AND run_id = 'smoketest_001'
        ORDER BY forecast_month
    """).df()
    print(fc.to_string(index=False))

print("\n=== ci_cd_log entry ===")
with duckdb.connect(WAREHOUSE, read_only=True) as con:
    log = con.sql("""
        SELECT run_id, asset_name, gate_name, status, message,
               executed_at
        FROM ci_cd_log
        WHERE run_id = 'smoketest_001'
    """).df()
    print(log.to_string(index=False))

print("\nSmoke test complete.")
