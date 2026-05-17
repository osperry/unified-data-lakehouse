#!/usr/bin/env python3
"""
Recoverability Scorer

Purpose:
Score quarantined or pending records for review priority.

Pending records are a work queue, not trash.
Invalid records remain blocked evidence for audit and contamination analysis.
"""

from __future__ import annotations

from typing import List


RECOVERABILITY_RULES = {
    "missing_optional_field": 0.90,
    "route_ambiguous": 0.70,
    "duplicate_key": 0.60,
    "duration_outlier": 0.50,
    "range_violation": 0.40,
    "schema_drift": 0.40,
    "missing_required_field": 0.30,
    "source_conflict": 0.40,
    "missing_origin_or_destination": 0.20,
    "invalid_timestamp_no_payload_hash": 0.10,
    "null_validation_status": 0.00,
    "referential_integrity_failure": 0.00,
    "reconciliation_failure": 0.00,
    "security_exposure": 0.00,
}


def score_recoverability(reason_codes: List[str]) -> float:
    if not reason_codes:
        return 1.0

    scores = [
        RECOVERABILITY_RULES.get(code, 0.50)
        for code in reason_codes
    ]

    return min(scores)


def review_bucket(score: float) -> str:
    if score >= 0.75:
        return "highly_recoverable"
    if score >= 0.50:
        return "review_recommended"
    if score > 0.00:
        return "low_recoverability"
    return "blocked"


if __name__ == "__main__":
    examples = [
        ["missing_optional_field"],
        ["route_ambiguous"],
        ["duplicate_key"],
        ["range_violation"],
        ["missing_required_field"],
        ["security_exposure"],
        ["null_validation_status"],
    ]

    for example in examples:
        score = score_recoverability(example)
        print({
            "reason_codes": example,
            "recoverability_score": score,
            "review_bucket": review_bucket(score),
        })
