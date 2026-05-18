import subprocess
import sys
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition

# Use the same Python interpreter that's running Dagster so deps (duckdb, boto3) are available.
PYTHON = sys.executable

@asset
def nyc311_bronze():
    """Extract raw NYC 311 complaints from Socrata API -> bronze JSON files."""
    subprocess.run(
        [PYTHON, "/app/extract/nyc311_extract.py"],
        check=True,
    )

@asset(deps=[nyc311_bronze])
def nyc311_silver_gold():
    """Run dbt build: bronze -> stg_complaints (silver) -> 6 gold tables in DuckDB."""
    subprocess.run(
        [
            "dbt", "build",
            "--project-dir", "/app/models/nyc311_marts",
            "--profiles-dir", "/app/models/nyc311_marts",
        ],
        check=True,
    )

@asset(deps=[nyc311_silver_gold])
def nyc311_gold_r2():
    """Export gold tables to Parquet (ZSTD) and upload to R2 osp-aviation-lakehouse/gold/."""
    subprocess.run(
        [PYTHON, "/app/models/gold_model_premium.py"],
        check=True,
    )

job = define_asset_job("nyc311_job", selection="*")

schedule = ScheduleDefinition(
    job=job,
    cron_schedule="0 9 * * *",   # daily 09:00 UTC
)

defs = Definitions(
    assets=[nyc311_bronze, nyc311_silver_gold, nyc311_gold_r2],
    jobs=[job],
    schedules=[schedule],
)
