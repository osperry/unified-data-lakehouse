import subprocess
from dagster import (
    asset, Definitions, define_asset_job,
    ScheduleDefinition, RetryPolicy, Backoff,
    DefaultScheduleStatus, DefaultSensorStatus,
    sensor, RunRequest, SensorEvaluationContext,
    AssetSelection
)
import os
import json
from datetime import datetime, timezone

BRONZE_PATH_NYC = os.getenv("NYC311_BRONZE_PATH", "data/bronze/nyc311")
BRONZE_PATH_FRED = os.getenv("FRED_BRONZE_PATH", "data/bronze/fred")
WAREHOUSE = os.getenv("WAREHOUSE_PATH", "/app/data/lakehouse.duckdb")
WATERMARK_FILE = os.path.join(BRONZE_PATH_NYC, "_watermark.json")


# ── Assets ────────────────────────────────────────────────────────────────────

@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
    )
)
def fred_bronze():
    """Extract FRED economic indicators to bronze layer."""
    subprocess.run(
        ["python", "/app/extract/fred_extract.py"],
        check=True,
    )


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
    )
)
def nyc311_bronze():
    """Extract NYC 311 complaint records to bronze layer."""
    subprocess.run(
        ["python", "/app/extract/nyc311_extract.py"],
        check=True,
    )


@asset(
    deps=[fred_bronze, nyc311_bronze],
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    )
)
def silver_gold():
    """Run dbt to build silver and gold layers."""
    subprocess.run(
        [
            "dbt", "build",
            "--project-dir", "/app/models/lakehouse_marts",
            "--profiles-dir", "/app/models/lakehouse_marts",
        ],
        check=True,
    )


# ── Jobs ──────────────────────────────────────────────────────────────────────

lakehouse_job = define_asset_job(
    "lakehouse_job",
    selection="*"
)

bronze_job = define_asset_job(
    "bronze_job",
    selection=AssetSelection.assets(fred_bronze, nyc311_bronze)
)

transform_job = define_asset_job(
    "transform_job",
    selection=AssetSelection.assets(silver_gold)
)


# ── Schedules (auto-enabled on startup) ───────────────────────────────────────

daily_full_schedule = ScheduleDefinition(
    job=lakehouse_job,
    cron_schedule="0 6 * * *",
    name="daily_full_pipeline",
    default_status=DefaultScheduleStatus.RUNNING,
)

bronze_schedule = ScheduleDefinition(
    job=bronze_job,
    cron_schedule="0 */6 * * *",
    name="bronze_refresh",
    default_status=DefaultScheduleStatus.RUNNING,
)


# ── Sensor (auto-enabled on startup) ──────────────────────────────────────────

@sensor(
    job=transform_job,
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.RUNNING,
)
def stale_bronze_sensor(context: SensorEvaluationContext):
    """
    Trigger silver/gold rebuild if bronze watermark updated
    in the last hour but silver/gold haven't been rebuilt.
    """
    if not os.path.exists(WATERMARK_FILE):
        return

    with open(WATERMARK_FILE) as f:
        watermark = json.load(f)

    updated_at_str = watermark.get("updated_at", "")
    if not updated_at_str:
        return

    updated_at = datetime.fromisoformat(updated_at_str)
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    hours_since_update = (now - updated_at).total_seconds() / 3600

    if hours_since_update < 1:
        last_cursor = context.cursor or ""
        if last_cursor != updated_at_str:
            context.update_cursor(updated_at_str)
            yield RunRequest(run_key=updated_at_str)


# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[fred_bronze, nyc311_bronze, silver_gold],
    jobs=[lakehouse_job, bronze_job, transform_job],
    schedules=[daily_full_schedule, bronze_schedule],
    sensors=[stale_bronze_sensor],
)