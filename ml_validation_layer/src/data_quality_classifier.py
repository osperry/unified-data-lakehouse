#!/usr/bin/env python3
"""
Data Quality Classifier

Purpose:
Create a rules-first validation classifier that labels records as:
- valid
- pending
- invalid
- quarantine_candidate

This supports the future Data Washing Agent and Validation Control Agent.

It does not write to R2, promote data, or alter production Gold.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

try:
    from defect_severity_scorer import score_defects
except ImportError:
    from src.defect_severity_scorer import score_defects


VALIDATION_STATUSES = {"valid", "pending", "invalid", "quarantine_candidate"}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return pd.isna(value)
    except Exception:
        return False


def classify_row(row: Dict[str, Any], domain: str = "unknown") -> Dict[str, Any]:
    """
    Classify one record using a rules-first approach.
    """
    defects: List[str] = []

    record_id = (
        row.get("record_id")
        or row.get("complaint_id")
        or row.get("route_key")
        or row.get("id")
        or row.get("index")
    )

    source_system = row.get("source_system") or row.get("source") or domain

    validation_status_raw = row.get("validation_status")

    if _is_missing(validation_status_raw):
        # Do not punish Gold data that has no explicit validation_status field
        # unless this classifier is used against validation-control data.
        if domain in {"validation_control", "aviation"}:
            defects.append("null_validation_status")

    required_fields_by_domain = {
        "nyc311": ["complaint_id"],
        "property_risk": ["full_address"],
        "aviation": ["source_system", "callsign", "origin", "destination"],
        "validation_control": ["record_id", "validation_status"],
    }

    for field in required_fields_by_domain.get(domain, []):
        if field in row and _is_missing(row.get(field)):
            defects.append("missing_required_field")

    if bool(row.get("duplicate_key", False)):
        defects.append("duplicate_key")

    if bool(row.get("range_violation", False)):
        defects.append("range_violation")

    if bool(row.get("schema_drift", False)):
        defects.append("schema_drift")

    if bool(row.get("security_exposure", False)):
        defects.append("security_exposure")

    if domain == "aviation":
        source = str(row.get("source_system", "")).lower()
        if source == "opensky":
            defects.append("route_ambiguous")
        if _is_missing(row.get("origin")) or _is_missing(row.get("destination")):
            defects.append("route_ambiguous")
        if row.get("duration_minutes") is not None:
            try:
                duration = float(row["duration_minutes"])
                if duration <= 0 or duration > 1200:
                    defects.append("duration_outlier")
            except Exception:
                defects.append("duration_outlier")

    scored = score_defects(defects)

    if scored["severity"] == "blocking":
        validation_status = "invalid"
    elif scored["quarantine_flag"] and scored["severity"] in {"high", "medium"}:
        validation_status = "quarantine_candidate"
    elif scored["severity"] == "low":
        validation_status = "pending"
    else:
        validation_status = "valid"

    return {
        "record_id": record_id,
        "source_system": source_system,
        "domain": domain,
        "validation_status": validation_status,
        "defect_type": ",".join(scored["reason_codes"]) if scored["reason_codes"] else "",
        "severity": scored["severity"],
        "quarantine_flag": scored["quarantine_flag"],
        "recoverability_score": scored["recoverability_score"],
        "reason_codes": scored["reason_codes"],
    }


def classify_dataframe(df: pd.DataFrame, domain: str = "unknown") -> pd.DataFrame:
    records = []
    for idx, row in df.reset_index().iterrows():
        row_dict = row.to_dict()
        row_dict["index"] = idx
        records.append(classify_row(row_dict, domain=domain))
    return pd.DataFrame(records)


def create_sample_output(output_path: Path) -> None:
    sample = pd.DataFrame([
        {
            "record_id": "sample-valid-001",
            "source_system": "nyc311",
            "domain": "nyc311",
            "complaint_id": "311-1",
            "duplicate_key": False,
        },
        {
            "record_id": "sample-duplicate-001",
            "source_system": "nyc311",
            "domain": "nyc311",
            "complaint_id": "311-2",
            "duplicate_key": True,
        },
        {
            "record_id": "sample-opensky-001",
            "source_system": "OpenSky",
            "domain": "aviation",
            "callsign": "ABC123",
            "origin": None,
            "destination": "JFK",
            "duration_minutes": 850,
        },
        {
            "record_id": "sample-blocking-001",
            "source_system": "validation_control",
            "domain": "validation_control",
            "validation_status": None,
        },
    ])

    classified_parts = []
    for _, row in sample.iterrows():
        classified_parts.append(classify_row(row.to_dict(), domain=row["domain"]))

    out_df = pd.DataFrame(classified_parts)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Optional input CSV or Parquet file")
    parser.add_argument("--output", default="outputs/data_quality_classification_sample.csv")
    parser.add_argument("--domain", default="unknown")
    args = parser.parse_args()

    output_path = Path(args.output)

    if not args.input:
        create_sample_output(output_path)
        print(f"Sample classification output written to {output_path}")
        return

    input_path = Path(args.input)
    if input_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)

    out_df = classify_dataframe(df, domain=args.domain)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)
    print(f"Classification output written to {output_path}")


if __name__ == "__main__":
    main()
