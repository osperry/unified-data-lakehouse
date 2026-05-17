#!/usr/bin/env python3
"""
NYC 311 Gold Anomaly Baseline

Purpose:
Run first-pass anomaly checks against certified Gold outputs.

This script reads local Parquet files if provided. It does not write to R2.
Outputs local CSV/JSON files under ml_validation_layer/outputs/.

Expected Gold tables:
- fct_daily_complaints.parquet
- fct_borough_summary.parquet
- fct_anomaly_summary.parquet
- fct_resolution_anomalies.parquet
- fct_property_incidents.parquet
- fct_address_risk_score.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


def safe_read_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def find_numeric_columns(df: pd.DataFrame) -> List[str]:
    return list(df.select_dtypes(include=["number"]).columns)


def zscore_anomalies(df: pd.DataFrame, table_name: str, threshold: float = 3.0) -> pd.DataFrame:
    findings = []

    numeric_cols = find_numeric_columns(df)

    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) < 10:
            continue

        mean = series.mean()
        std = series.std()

        if std == 0 or pd.isna(std):
            continue

        z = (df[col] - mean) / std
        flagged = df[z.abs() >= threshold].copy()

        for idx, row in flagged.iterrows():
            findings.append({
                "table_name": table_name,
                "row_index": idx,
                "anomaly_type": "zscore_outlier",
                "column_name": col,
                "value": row.get(col),
                "mean": mean,
                "std": std,
                "zscore": z.loc[idx],
                "severity": "medium" if abs(z.loc[idx]) < 5 else "high",
            })

    return pd.DataFrame(findings)


def iqr_anomalies(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    findings = []

    numeric_cols = find_numeric_columns(df)

    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) < 10:
            continue

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0 or pd.isna(iqr):
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        flagged = df[(df[col] < lower) | (df[col] > upper)].copy()

        for idx, row in flagged.iterrows():
            findings.append({
                "table_name": table_name,
                "row_index": idx,
                "anomaly_type": "iqr_outlier",
                "column_name": col,
                "value": row.get(col),
                "lower_bound": lower,
                "upper_bound": upper,
                "severity": "medium",
            })

    return pd.DataFrame(findings)


def summarize_table(df: pd.DataFrame, table_name: str) -> dict:
    return {
        "table_name": table_name,
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "numeric_columns": find_numeric_columns(df),
        "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
    }


def run_anomaly_baseline(gold_dir: Path, output_dir: Path) -> None:
    tables = [
        "fct_daily_complaints",
        "fct_borough_summary",
        "fct_anomaly_summary",
        "fct_resolution_anomalies",
        "fct_property_incidents",
        "fct_address_risk_score",
    ]

    all_findings = []
    summaries: Dict[str, dict] = {}
    missing_tables = []

    for table in tables:
        path = gold_dir / f"{table}.parquet"
        df = safe_read_parquet(path)

        if df is None:
            missing_tables.append(str(path))
            continue

        summaries[table] = summarize_table(df, table)

        z_df = zscore_anomalies(df, table)
        iqr_df = iqr_anomalies(df, table)

        if not z_df.empty:
            all_findings.append(z_df)
        if not iqr_df.empty:
            all_findings.append(iqr_df)

    if all_findings:
        findings_df = pd.concat(all_findings, ignore_index=True)
    else:
        findings_df = pd.DataFrame(columns=[
            "table_name", "row_index", "anomaly_type",
            "column_name", "value", "severity"
        ])

    output_dir.mkdir(parents=True, exist_ok=True)

    findings_path = output_dir / "anomaly_findings_sample.csv"
    summary_path = output_dir / "anomaly_summary.json"

    findings_df.to_csv(findings_path, index=False)

    summary = {
        "purpose": "May 16 NYC 311 Gold anomaly baseline",
        "input_gold_dir": str(gold_dir),
        "tables_reviewed": list(summaries.keys()),
        "missing_tables": missing_tables,
        "anomaly_count": int(len(findings_df)),
        "summaries": summaries,
        "promotion_recommendation": "REVIEW",
        "note": "This is an advisory Dev/Test anomaly baseline. It does not promote data or write to production R2.",
    }

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Wrote {findings_path}")
    print(f"Wrote {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gold-dir",
        default="../pipelines/nyc311/data/gold_export",
        help="Local directory containing Gold Parquet files copied/exported from R2."
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Local output directory."
    )
    args = parser.parse_args()

    run_anomaly_baseline(Path(args.gold_dir), Path(args.output_dir))


if __name__ == "__main__":
    main()
