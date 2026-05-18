"""
gold_model_premium.py — FRED pipeline
─────────────────────────────────────────────────────────────────────────────
Exports gold table built by dbt to Parquet (ZSTD) and uploads to
Cloudflare R2.  Does NOT rebuild the table — dbt handles that in the
fred_silver_gold Dagster asset.

Table exported (built by dbt in main_gold schema):
    fct_macro_daily

Environment variables (set by Docker via .env):
    WAREHOUSE_PATH         /app/data/fred.duckdb
    PARQUET_EXPORT_DIR     /app/data/gold_export  (default)
    R2_ENDPOINT_URL        https://<accountid>.r2.cloudflarestorage.com
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_BUCKET              osp-aviation-lakehouse (default)
    R2_GOLD_PREFIX         gold (default)
─────────────────────────────────────────────────────────────────────────────
"""

import os
from dotenv import load_dotenv

# load_dotenv WITHOUT override=True so Docker-injected env vars win.
# Only fills in vars that aren't already set (useful for local dev).
load_dotenv()

WAREHOUSE         = os.getenv("WAREHOUSE_PATH",     "/app/data/fred.duckdb")
PARQUET_EXPORT_DIR = os.getenv("PARQUET_EXPORT_DIR", "/app/data/gold_export")

R2_ENDPOINT       = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID  = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY     = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET         = os.getenv("R2_BUCKET",       "osp-aviation-lakehouse")
R2_GOLD_PREFIX    = os.getenv("R2_GOLD_PREFIX",  "gold")

# Gold table built by dbt — schema is main_gold (dbt default + schema: gold)
GOLD_TABLES = [
    "fct_macro_daily",
]


def export_parquet(con, export_dir: str) -> dict:
    """Export gold table to a Parquet file. Returns {table_name: local_path}."""
    os.makedirs(export_dir, exist_ok=True)
    files = {}
    for name in GOLD_TABLES:
        qualified = f"main_gold.{name}"
        path = os.path.join(export_dir, f"{name}.parquet")
        con.execute(f"COPY {qualified} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = os.path.getsize(path) / 1_048_576
        files[name] = path
        print(f"  {qualified} → {name}.parquet  ({size_mb:.2f} MB)")
    return files


def upload_to_r2(local_files: dict) -> None:
    """Upload Parquet files to Cloudflare R2. Skips if credentials are missing."""
    missing = [k for k, v in {
        "R2_ENDPOINT_URL":     R2_ENDPOINT,
        "R2_ACCESS_KEY_ID":    R2_ACCESS_KEY_ID,
        "R2_SECRET_ACCESS_KEY": R2_SECRET_KEY,
    }.items() if not v]

    if missing:
        print(f"\n  [SKIP R2] Missing env vars: {', '.join(missing)}")
        print("  Set them in .env and re-run to enable upload.")
        return

    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("\n  [SKIP R2] boto3 not installed. Run: pip install boto3")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

    print(f"\nUploading to R2: {R2_BUCKET}/{R2_GOLD_PREFIX}/")
    for table_name, local_path in local_files.items():
        s3_key = f"{R2_GOLD_PREFIX}/{table_name}.parquet"
        s3.upload_file(
            Filename=local_path,
            Bucket=R2_BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        size_mb = os.path.getsize(local_path) / 1_048_576
        print(f"  ✓ s3://{R2_BUCKET}/{s3_key}  ({size_mb:.2f} MB)")

    print("R2 upload complete.")


def main():
    import duckdb

    print(f"Connecting to: {WAREHOUSE}")
    con = duckdb.connect(WAREHOUSE)

    # Verify gold table exists before exporting
    print("\nVerifying gold table...")
    for name in GOLD_TABLES:
        qualified = f"main_gold.{name}"
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            print(f"  {qualified}: {count:,} rows")
        except Exception as e:
            raise RuntimeError(
                f"Gold table {qualified} not found. "
                f"Run fred_silver_gold (dbt build) first. Error: {e}"
            )

    # Export to Parquet
    export_dir = os.path.abspath(PARQUET_EXPORT_DIR)
    print(f"\nExporting Parquet to: {export_dir}")
    parquet_files = export_parquet(con, export_dir)
    con.close()

    # Upload to R2
    upload_to_r2(parquet_files)

    print("\nDone.")


if __name__ == "__main__":
    main()
