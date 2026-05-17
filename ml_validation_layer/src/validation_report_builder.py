#!/usr/bin/env python3
"""
Validation Report Builder

Purpose:
Create a machine-readable validation report for future Validation Control and
Promotion Pipeline agents.

This report is advisory. It does not promote data or write to R2.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_csv(path)))
    except Exception:
        return 0


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_report(repo_root: Path, output_dir: Path, report_path: Path) -> dict:
    data_quality_path = output_dir / "data_quality_classification_sample.csv"
    anomaly_summary_path = output_dir / "anomaly_summary.json"
    risk_findings_path = output_dir / "risk_score_quality_findings.json"
    route_confidence_path = output_dir / "route_confidence_sample.csv"

    dq_df = pd.read_csv(data_quality_path) if data_quality_path.exists() else pd.DataFrame()

    if not dq_df.empty and "validation_status" in dq_df.columns:
        valid_count = int((dq_df["validation_status"] == "valid").sum())
        pending_count = int((dq_df["validation_status"] == "pending").sum())
        invalid_count = int((dq_df["validation_status"] == "invalid").sum())
        quarantine_count = int(dq_df["quarantine_flag"].sum()) if "quarantine_flag" in dq_df.columns else 0
    else:
        valid_count = 0
        pending_count = 0
        invalid_count = 0
        quarantine_count = 0

    anomaly_summary = load_json(anomaly_summary_path)
    risk_findings = load_json(risk_findings_path)

    blocking_failures = []

    if invalid_count > 0:
        blocking_failures.append({
            "source": "data_quality_classifier",
            "message": f"{invalid_count} invalid sample records found.",
        })

    if risk_findings.get("status") in {"MISSING_INPUT", "NO_SCORE_COLUMN_DETECTED"}:
        blocking_failures.append({
            "source": "property_risk_score_profiler",
            "message": risk_findings.get("message", risk_findings.get("status")),
        })

    warnings = []

    if anomaly_summary.get("missing_tables"):
        warnings.append({
            "source": "nyc311_anomaly_baseline",
            "message": "Some expected Gold Parquet files were not found locally.",
            "missing_tables": anomaly_summary.get("missing_tables"),
        })

    if risk_findings.get("warnings"):
        warnings.append({
            "source": "property_risk_score_profiler",
            "message": "Risk score profiler generated warnings.",
            "warnings": risk_findings.get("warnings"),
        })

    report = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_ml_validation_layer"),
        "repo_root": str(repo_root),
        "environment": "dev_test",
        "input_layer": "gold",
        "input_source": "r2://osp-aviation-lakehouse/gold/",
        "domains": [
            "nyc311",
            "fred",
            "aviation"
        ],
        "input_tables": [
            "fct_property_incidents",
            "fct_address_risk_score",
            "fct_daily_complaints",
            "fct_borough_summary",
            "fct_anomaly_summary",
            "fct_resolution_anomalies"
        ],
        "local_outputs_reviewed": {
            "data_quality_classification_sample": str(data_quality_path),
            "anomaly_summary": str(anomaly_summary_path),
            "risk_score_quality_findings": str(risk_findings_path),
            "route_confidence_sample": str(route_confidence_path),
        },
        "row_counts": {
            "data_quality_classification_sample": count_csv_rows(data_quality_path),
            "route_confidence_sample": count_csv_rows(route_confidence_path),
            "anomaly_findings_sample": count_csv_rows(output_dir / "anomaly_findings_sample.csv"),
            "risk_score_profile": count_csv_rows(output_dir / "risk_score_profile.csv"),
        },
        "valid_count": valid_count,
        "pending_count": pending_count,
        "invalid_count": invalid_count,
        "quarantine_count": quarantine_count,
        "anomaly_count": int(anomaly_summary.get("anomaly_count", 0)),
        "blocking_failures": blocking_failures,
        "warnings": warnings,
        "promotion_recommendation": "REVIEW",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "note": "Advisory Dev/Test report only. Does not promote data, write to R2, or alter production Gold.",
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    validation_summary_path = output_dir / "validation_summary.json"
    validation_summary_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default="/Users/operry/Projects/osp-lakehouse")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument(
        "--report-path",
        default="metadata/validation_reports/sample_validation_report.json"
    )
    args = parser.parse_args()

    report = build_report(
        repo_root=Path(args.repo_root),
        output_dir=Path(args.output_dir),
        report_path=Path(args.report_path),
    )

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
