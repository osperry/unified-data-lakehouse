import subprocess
from dagster import asset, Definitions, define_asset_job
from dagster import ScheduleDefinition

@asset
def fred_bronze():
    subprocess.run(
        ["python", "/app/extract/fred_extract.py"],
        check=True,
    )

@asset
def nyc311_bronze():
    subprocess.run(
        ["python", "/app/extract/nyc311_extract.py"],
        check=True,
    )

@asset(deps=[fred_bronze, nyc311_bronze])
def silver_gold():
    subprocess.run(
        ["dbt", "build",
        "--project-dir", "/app/models/lakehouse_marts",
        "--profiles-dir", "/app/models/lakehouse_marts"],
        check=True,
    )

job = define_asset_job("lakehouse_job", selection="*")
schedule = ScheduleDefinition(
    job=job, cron_schedule="0 9 * * *"
)
defs = Definitions(
    assets=[fred_bronze, nyc311_bronze, silver_gold],
    jobs=[job],
    schedules=[schedule],
)
