"""
Gates smoke test.

Builds a synthetic warehouse with:
  - bronze.nyc311_raw
  - silver.stg_complaints (some rows dropped from bronze)
  - silver.quarantine_complaints (the dropped rows)
  - gold.fct_daily_complaints (aggregated)
  - validation_reports (one APPROVE row)

Runs all 8 gates directly (no Dagster), prints each result, and verifies the
PromotionGate composes correctly. Also tests failure paths by mutating data.
"""

import os
import sys
sys.path.insert(0, ".")

import duckdb
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from gates import (
    SchemaGate, DataQualityGate, ValidationGate, QuarantineGate,
    ReconciliationGate, CostGate, SecurityGate, PromotionGate,
    log_gate_result,
)

WAREHOUSE = "test_gates.duckdb"
if os.path.exists(WAREHOUSE):
    os.remove(WAREHOUSE)


def seed_warehouse():
    """Build a complete synthetic warehouse."""
    rng = np.random.default_rng(seed=42)
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
    boroughs = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

    # Bronze: every complaint
    bronze_rows = []
    for i, d in enumerate(dates):
        for b in boroughs:
            n = max(0, int(rng.normal(200, 30)))
            for _ in range(n):
                bronze_rows.append({
                    "complaint_id": f"C{len(bronze_rows):08d}",
                    "complaint_date": d.date(),
                    "borough": b,
                    "complaint_type": rng.choice(["Noise", "Water", "Heat"]),
                })
    bronze_df = pd.DataFrame(bronze_rows)
    print(f"Built bronze: {len(bronze_df):,} rows")

    # Silver: 95% of bronze passes cleaning. 5% goes to quarantine.
    mask_quarantine = rng.random(len(bronze_df)) < 0.05
    quarantine_df = bronze_df[mask_quarantine].copy()
    quarantine_df["defect_reason"] = "stale_address"
    silver_df = bronze_df[~mask_quarantine].copy()
    print(f"Built silver: {len(silver_df):,} rows, quarantine: {len(quarantine_df):,} rows")

    # Gold: aggregated daily by borough
    gold_df = (
        silver_df.groupby(["complaint_date", "borough"])
        .size().reset_index(name="complaint_count")
    )
    gold_df["complaint_count"] = gold_df["complaint_count"].astype("int64")
    print(f"Built gold: {len(gold_df):,} rows")

    with duckdb.connect(WAREHOUSE) as con:
        con.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        con.sql("CREATE SCHEMA IF NOT EXISTS silver")
        con.sql("CREATE SCHEMA IF NOT EXISTS gold")
        con.register("b", bronze_df)
        con.register("s", silver_df)
        con.register("q", quarantine_df)
        con.register("g", gold_df)
        con.sql("CREATE TABLE bronze.nyc311_raw AS SELECT * FROM b")
        con.sql("CREATE TABLE silver.stg_complaints AS SELECT * FROM s")
        con.sql("CREATE TABLE silver.quarantine_complaints AS SELECT * FROM q")
        con.sql("CREATE TABLE gold.fct_daily_complaints AS SELECT * FROM g")
        # Validation report
        con.sql("""
            CREATE TABLE validation_reports (
                asset_name VARCHAR,
                run_id VARCHAR,
                recommendation VARCHAR,
                report JSON,
                created_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO validation_reports VALUES
            ('gold.fct_daily_complaints', 'ml_001', 'APPROVE', '{}', ?)
        """, [datetime.utcnow() - timedelta(hours=1)])


def run_all_gates(run_id: str, asset: str = "gold.fct_daily_complaints"):
    """Run every gate, log each, return list of results."""
    gates = [
        SchemaGate(
            asset, WAREHOUSE, run_id=run_id,
            expected_columns={
                "complaint_date": "DATE",
                "borough": "VARCHAR",
                "complaint_count": "BIGINT",
            },
        ),
        DataQualityGate(
            asset, WAREHOUSE, run_id=run_id,
            null_rate_warn=0.02, null_rate_fail=0.10,
            critical_columns=["complaint_date", "borough", "complaint_count"],
            range_checks={"complaint_count": (0, 1_000_000)},
        ),
        ValidationGate(asset, WAREHOUSE, run_id=run_id, max_age_hours=6),
        QuarantineGate(
            asset, WAREHOUSE, run_id=run_id,
            silver_table="silver.stg_complaints",
            quarantine_table="silver.quarantine_complaints",
            quarantine_rate_warn=0.10,
            aggregated=True,
        ),
        ReconciliationGate(
            asset, WAREHOUSE, run_id=run_id,
            bronze_table="bronze.nyc311_raw",
            silver_table="silver.stg_complaints",
            aggregated=True,
        ),
        CostGate(asset, WAREHOUSE, run_id=run_id, soft_cap_mb=100.0, hard_cap_mb=500.0),
        SecurityGate(asset, WAREHOUSE, run_id=run_id),
    ]
    results = []
    for g in gates:
        r = g.run()
        log_gate_result(WAREHOUSE, r)
        results.append(r)
        flag = {"pass": "[PASS]", "warn": "[WARN]", "fail": "[FAIL]"}[r.status.value]
        print(f"  {flag} {r.gate_name:<16} {r.message}")

    # Promotion gate goes last, reads ci_cd_log
    promo = PromotionGate(asset, WAREHOUSE, run_id=run_id).run()
    log_gate_result(WAREHOUSE, promo)
    results.append(promo)
    flag = {"pass": "[PASS]", "warn": "[WARN]", "fail": "[FAIL]"}[promo.status.value]
    print(f"  {flag} {promo.gate_name:<16} {promo.message}")
    return results


def main():
    print("=" * 70)
    print("Phase 1: Build synthetic warehouse")
    print("=" * 70)
    seed_warehouse()

    print()
    print("=" * 70)
    print("Phase 2: Happy path - all gates run against clean data")
    print("=" * 70)
    results = run_all_gates(run_id="happy_001")
    assert all(r.status.value in ("pass", "warn") for r in results), "Unexpected FAIL on happy path"
    promotion = results[-1]
    print(f"\n  >>> Promotion verdict: {promotion.status.value.upper()}")

    print()
    print("=" * 70)
    print("Phase 3: Failure path - inject a schema violation, rerun")
    print("=" * 70)
    with duckdb.connect(WAREHOUSE) as con:
        con.sql("ALTER TABLE gold.fct_daily_complaints DROP COLUMN borough")
    fail_results = run_all_gates(run_id="fail_002")
    promotion_fail = fail_results[-1]
    assert promotion_fail.status.value == "fail", "Promotion should FAIL when schema breaks"
    print(f"\n  >>> Promotion verdict: {promotion_fail.status.value.upper()} (expected)")

    print()
    print("=" * 70)
    print("Phase 4: ci_cd_log audit")
    print("=" * 70)
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        df = con.sql("""
            SELECT run_id, gate_name, status, substr(message, 1, 60) AS msg
            FROM ci_cd_log
            ORDER BY executed_at, gate_name
        """).df()
        print(df.to_string(index=False))

    print("\nGates smoke test complete.")


if __name__ == "__main__":
    main()
