#!/usr/bin/env python3
"""
Aviation Route Confidence Prototype

Purpose:
Score route confidence using source, callsign, origin, destination, duration,
payload hash, and source priority.

FlightAware is premium production-supporting evidence.
OpenSky is analytical support only unless explicitly certified.

This script does not promote aviation data or write to R2.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return pd.isna(value)
    except Exception:
        return False


def score_route(row: Dict[str, Any]) -> Dict[str, Any]:
    source = str(row.get("source_system") or row.get("source") or "").strip().lower()
    callsign = row.get("callsign")
    icao24 = row.get("icao24")
    origin = row.get("origin")
    destination = row.get("destination")
    duration = row.get("duration_minutes")
    payload_hash = row.get("payload_hash")

    score = 0.0
    reason_codes: List[str] = []

    if source == "flightaware":
        score += 0.30
        reason_codes.append("premium_source_flightaware")
    elif source == "opensky":
        score += 0.10
        reason_codes.append("opensky_analytical_support_only")
    else:
        score += 0.05
        reason_codes.append("unknown_source")

    if not missing(callsign):
        score += 0.15
    else:
        reason_codes.append("missing_callsign")

    if not missing(icao24):
        score += 0.10
    else:
        reason_codes.append("missing_icao24")

    if not missing(origin) and not missing(destination):
        score += 0.20
    else:
        reason_codes.append("missing_origin_or_destination")

    if not missing(duration):
        try:
            duration_float = float(duration)
            if 120 <= duration_float <= 900:
                score += 0.15
            elif 0 < duration_float <= 1200:
                score += 0.05
                reason_codes.append("duration_needs_review")
            else:
                reason_codes.append("duration_outlier")
        except Exception:
            reason_codes.append("duration_parse_failure")
    else:
        reason_codes.append("missing_duration")

    if not missing(payload_hash):
        score += 0.10
    else:
        reason_codes.append("missing_payload_hash")

    score = round(min(max(score, 0.0), 1.0), 4)

    if source == "opensky":
        status = "analytical_only"
    elif score >= 0.80:
        status = "certifiable"
    elif score >= 0.50:
        status = "needs_review"
    else:
        status = "invalid"

    route_key = f"{origin or 'UNK'}_{destination or 'UNK'}_{callsign or 'UNK'}"

    return {
        "route_key": route_key,
        "source_system": source or "unknown",
        "origin": origin,
        "destination": destination,
        "callsign": callsign,
        "icao24": icao24,
        "duration_minutes": duration,
        "route_confidence_score": score,
        "route_validation_status": status,
        "reason_codes": reason_codes,
    }


def score_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([score_route(row.to_dict()) for _, row in df.iterrows()])


def create_sample(output_path: Path) -> None:
    sample = pd.DataFrame([
        {
            "source_system": "FlightAware",
            "callsign": "UA845",
            "icao24": "abc123",
            "origin": "KORD",
            "destination": "SBGR",
            "duration_minutes": 610,
            "payload_hash": "hash1",
        },
        {
            "source_system": "OpenSky",
            "callsign": "UA845",
            "icao24": "abc123",
            "origin": "KORD",
            "destination": "SBGR",
            "duration_minutes": 610,
            "payload_hash": "hash2",
        },
        {
            "source_system": "OpenSky",
            "callsign": None,
            "icao24": None,
            "origin": "SBGL",
            "destination": None,
            "duration_minutes": 9999,
            "payload_hash": None,
        },
    ])

    out = score_dataframe(sample)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Optional CSV or Parquet aviation records")
    parser.add_argument("--output", default="outputs/route_confidence_sample.csv")
    args = parser.parse_args()

    output_path = Path(args.output)

    if not args.input:
        create_sample(output_path)
        print(f"Sample route confidence output written to {output_path}")
        return

    input_path = Path(args.input)
    if input_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)

    out = score_dataframe(df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    print(f"Route confidence output written to {output_path}")


if __name__ == "__main__":
    main()
