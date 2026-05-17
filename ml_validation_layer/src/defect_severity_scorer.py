#!/usr/bin/env python3
"""
Defect Severity Scorer

Purpose:
Map validation defect types to severity, quarantine behavior, and recoverability hints.

This module is part of the May 16 ML validation intelligence layer.
It does not promote data, write to R2, or alter production Gold artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List


@dataclass(frozen=True)
class DefectRule:
    defect_type: str
    severity: str
    quarantine_flag: bool
    recoverability_score: float
    description: str


DEFECT_RULES: Dict[str, DefectRule] = {
    "schema_drift": DefectRule(
        "schema_drift", "high", True, 0.40,
        "Unexpected schema difference from expected contract."
    ),
    "missing_required_field": DefectRule(
        "missing_required_field", "high", True, 0.30,
        "Required field is null, blank, or absent."
    ),
    "missing_optional_field": DefectRule(
        "missing_optional_field", "low", False, 0.90,
        "Optional field is missing."
    ),
    "duplicate_key": DefectRule(
        "duplicate_key", "high", True, 0.60,
        "Duplicate business or record key."
    ),
    "range_violation": DefectRule(
        "range_violation", "medium", True, 0.40,
        "Numeric/date/duration value outside allowed range."
    ),
    "null_validation_status": DefectRule(
        "null_validation_status", "blocking", True, 0.00,
        "Record has no validation status."
    ),
    "route_ambiguous": DefectRule(
        "route_ambiguous", "high", True, 0.70,
        "Aviation route cannot be confidently resolved."
    ),
    "duration_outlier": DefectRule(
        "duration_outlier", "medium", True, 0.50,
        "Duration is implausible or statistically abnormal."
    ),
    "source_conflict": DefectRule(
        "source_conflict", "high", True, 0.40,
        "Source systems disagree materially."
    ),
    "referential_integrity_failure": DefectRule(
        "referential_integrity_failure", "blocking", True, 0.00,
        "Record does not reconcile to expected parent/reference table."
    ),
    "reconciliation_failure": DefectRule(
        "reconciliation_failure", "blocking", True, 0.00,
        "Gold totals do not reconcile to certified valid records."
    ),
    "security_exposure": DefectRule(
        "security_exposure", "blocking", True, 0.00,
        "Secrets, raw credentials, or unsafe data exposure detected."
    ),
    "cost_threshold_exceeded": DefectRule(
        "cost_threshold_exceeded", "medium", False, 0.50,
        "Artifact/storage/output exceeds approved value/cost boundary."
    ),
}


SEVERITY_RANK = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "blocking": 4,
}


def score_defects(defect_types: List[str]) -> dict:
    """
    Score a list of defect types.

    Returns:
        dict with severity, quarantine flag, recoverability score, and reason codes.
    """
    if not defect_types:
        return {
            "severity": "info",
            "quarantine_flag": False,
            "recoverability_score": 1.0,
            "reason_codes": [],
            "unknown_defects": [],
        }

    known_rules = []
    unknown_defects = []

    for defect_type in defect_types:
        rule = DEFECT_RULES.get(defect_type)
        if rule:
            known_rules.append(rule)
        else:
            unknown_defects.append(defect_type)

    if not known_rules and unknown_defects:
        return {
            "severity": "medium",
            "quarantine_flag": True,
            "recoverability_score": 0.50,
            "reason_codes": unknown_defects,
            "unknown_defects": unknown_defects,
        }

    max_rule = max(known_rules, key=lambda r: SEVERITY_RANK[r.severity])
    quarantine_flag = any(rule.quarantine_flag for rule in known_rules) or bool(unknown_defects)
    recoverability_score = min(rule.recoverability_score for rule in known_rules) if known_rules else 0.50

    return {
        "severity": max_rule.severity,
        "quarantine_flag": quarantine_flag,
        "recoverability_score": recoverability_score,
        "reason_codes": [rule.defect_type for rule in known_rules] + unknown_defects,
        "unknown_defects": unknown_defects,
    }


def list_rules() -> list[dict]:
    return [asdict(rule) for rule in DEFECT_RULES.values()]


if __name__ == "__main__":
    import json
    print(json.dumps(list_rules(), indent=2))
