import subprocess
import sys
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition

# Use the same Python interpreter running Dagster so deps (duckdb, boto3) are available.
PYTHON = sys.executable

@asset
def fred_bronze():
    """Extract FRED macro series (UNRATE, CPIAUCSL, FEDFUNDS, GDP) -> bronze JSON files."""
    subprocess.run(
        [PYTHON, "/app/extract/fred_extract.py"],
        check=True,
    )

@asset(deps=[fred_bronze])
def fred_silver_gold():
    """Run dbt build: bronze JSON -> stg_fred_observations (silver) -> fct_macro_daily (gold) in DuckDB."""
    subprocess.run(
        [
            "dbt", "build",
            "--project-dir", "/app/models/fred_marts",
            "--profiles-dir", "/app/models/fred_marts",
        ],
        check=True,
    )

@asset(deps=[fred_silver_gold])
def fred_gold_r2():
    """Export gold.fct_macro_daily to Parquet (ZSTD) and upload to R2 osp-aviation-lakehouse/gold/."""
    subprocess.run(
        [PYTHON, "/app/models/gold_model_premium.py"],
        check=True,
    )

job = define_asset_job("fred_job", selection="*")

schedule = ScheduleDefinition(
    job=job,
    cron_schedule="0 9 * * *",   # daily 09:00 UTC — matches NYC 311 standard
)

defs = Definitions(
    assets=[fred_bronze, fred_silver_gold, fred_gold_r2],
    jobs=[job],
    schedules=[schedule],
)
