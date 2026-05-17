#!/usr/bin/env python3
"""
Property Risk Score Profiler

Purpose:
Profile the premium Gold table fct_address_risk_score.

This validates score quality without redesigning the scoring model and without
writing to production Gold or R2.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


POSSIBLE_SCORE_COLUMNS = [
    "property_risk_score",
    "risk_score",
    "address_risk_score",
    "score",
]


def detect_score_column(df: pd.DataFrame) -> str | None:
    for col in POSSIBLE_SCORE_COLUMNS:
        if col in df.columns:
            return col

    numeric_cols = list(df.select_dtypes(include=["number"]).columns)
    for col in numeric_cols:
        min_val = df[col].min()
        max_val = df[col].max()
        if pd.notna(min_val) and pd.notna(max_val) and min_val >= 0 and max_val <= 100:
            return col

    return None


def detect_address_column(df: pd.DataFrame) -> str | None:
    for col in ["full_address", "incident_address", "address"]:
        if col in df.columns:
            return col
    return None


def detect_zip_column(df: pd.DataFrame) -> str | None:
    for col in ["zip_code", "zipcode", "postal_code"]:
        if col in df.columns:
            return col
    return None


def detect_borough_column(df: pd.DataFrame) -> str | None:
    for col in ["borough", "boro"]:
        if col in df.columns:
            return col
    return None


def profile_risk_score(input_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        findings = {
            "status": "MISSING_INPUT",
            "input_path": str(input_path),
            "message": "fct_address_risk_score parquet file was not found locally.",
            "promotion_recommendation": "REVIEW",
        }
        (output_dir / "risk_score_quality_findings.json").write_text(
            json.dumps(findings, indent=2), encoding="utf-8"
        )
        print(json.dumps(findings, indent=2))
        return

    df = pd.read_parquet(input_path)

    score_col = detect_score_column(df)
    address_col = detect_address_column(df)
    zip_col = detect_zip_column(df)
    borough_col = detect_borough_column(df)

    if score_col is None:
        findings = {
            "status": "NO_SCORE_COLUMN_DETECTED",
            "input_path": str(input_path),
            "columns": list(df.columns),
            "promotion_recommendation": "REVIEW",
        }
        (output_dir / "risk_score_quality_findings.json").write_text(
            json.dumps(findings, indent=2), encoding="utf-8"
        )
        print(json.dumps(findings, indent=2))
        return

    score = df[score_col]

    profile = {
        "table": "fct_address_risk_score",
        "input_path": str(input_path),
        "row_count": int(len(df)),
        "score_column": score_col,
        "address_column": address_col,
        "zip_column": zip_col,
        "borough_column": borough_col,
        "null_score_count": int(score.isna().sum()),
        "min_score": float(score.min()) if score.notna().any() else None,
        "max_score": float(score.max()) if score.notna().any() else None,
        "mean_score": float(score.mean()) if score.notna().any() else None,
        "median_score": float(score.median()) if score.notna().any() else None,
        "p95_score": float(score.quantile(0.95)) if score.notna().any() else None,
        "p99_score": float(score.quantile(0.99)) if score.notna().any() else None,
        "duplicate_address_count": int(df[address_col].duplicated().sum()) if address_col else None,
    }

    profile_df = pd.DataFrame([profile])
    profile_df.to_csv(output_dir / "risk_score_profile.csv", index=False)

    top_n = max(10, int(len(df) * 0.01)) if len(df) > 0 else 0
    if top_n > 0:
        top_risk = df.sort_values(score_col, ascending=False).head(top_n)
        top_risk.to_csv(output_dir / "top_risk_addresses_sample.csv", index=False)

    grouped_outputs = {}

    if zip_col:
        zip_summary = df.groupby(zip_col)[score_col].agg(["count", "mean", "median", "min", "max"]).reset_index()
        zip_summary.to_csv(output_dir / "risk_score_by_zip.csv", index=False)
        grouped_outputs["zip_summary"] = "risk_score_by_zip.csv"

    if borough_col:
        borough_summary = df.groupby(borough_col)[score_col].agg(["count", "mean", "median", "min", "max"]).reset_index()
        borough_summary.to_csv(output_dir / "risk_score_by_borough.csv", index=False)
        grouped_outputs["borough_summary"] = "risk_score_by_borough.csv"

    warnings = []

    if profile["null_score_count"] > 0:
        warnings.append("Some records have null risk scores.")

    if profile["min_score"] is not None and profile["min_score"] < 0:
        warnings.append("Risk score below 0 detected.")

    if profile["max_score"] is not None and profile["max_score"] > 100:
        warnings.append("Risk score above 100 detected.")

    findings = {
        "status": "COMPLETE",
        "profile": profile,
        "grouped_outputs": grouped_outputs,
        "warnings": warnings,
        "promotion_recommendation": "REVIEW",
        "note": "Advisory score-quality review only. This does not alter the Gold table.",
    }

    (output_dir / "risk_score_quality_findings.json").write_text(
        json.dumps(findings, indent=2), encoding="utf-8"
    )

    print(json.dumps(findings, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="../pipelines/nyc311/data/gold_export/fct_address_risk_score.parquet"
    )
    parser.add_argument("--output-dir", default="outputs")
    args = parser.parse_args()

    profile_risk_score(Path(args.input), Path(args.output_dir))


if __name__ == "__main__":
    main()
