# Defect Taxonomy

## Purpose

This taxonomy defines the validation defects, severity levels, quarantine rules, and recoverability signals used by the May 16 ML validation intelligence layer.

This does not promote data. It creates the defect language that future Data Washing, Validation Control, and Promotion Pipeline agents can consume.

---

# Severity Levels

| Severity | Meaning |
|---|---|
| info | Informational condition; no action required |
| low | Minor issue; record can usually remain usable |
| medium | Needs review; may be recoverable |
| high | Significant defect; likely quarantine or correction needed |
| blocking | Cannot proceed; must be blocked from promotion |

---

# Defect Types

| Defect Type | Description | Default Severity | Quarantine? |
|---|---|---:|---|
| schema_drift | Unexpected schema difference from expected contract | high | yes |
| missing_required_field | Required field is null, blank, or absent | high | yes |
| missing_optional_field | Optional field is missing | low | no |
| duplicate_key | Duplicate business or record key | high | yes |
| range_violation | Numeric/date/duration value outside allowed range | medium | yes |
| null_validation_status | Record has no validation status | blocking | yes |
| route_ambiguous | Aviation route cannot be confidently resolved | high | yes |
| duration_outlier | Duration is implausible or statistically abnormal | medium | yes |
| source_conflict | Source systems disagree materially | high | yes |
| referential_integrity_failure | Record does not reconcile to expected parent/reference table | blocking | yes |
| reconciliation_failure | Gold totals do not reconcile to certified valid records | blocking | yes |
| security_exposure | Secrets, raw credentials, or unsafe data exposure detected | blocking | yes |
| cost_threshold_exceeded | Artifact/storage/output exceeds approved value/cost boundary | medium | review |

---

# Master Document Alignment

The architecture requires row counts, null checks, duplicate checks, schema drift checks, duration/range checks, route coverage, reconciliation, quarantine, and machine-readable validation reports.

This taxonomy implements the language needed to classify those findings consistently.

---

# Hard Rules

- A record with `null_validation_status` is blocking.
- A quarantined record must not flow into Gold.
- OpenSky must not be treated as production aviation truth unless explicitly certified.
- FlightAware is the premium aviation source.
- Manual Gold inserts are not allowed.
- Pending records are a work queue, not trash.
- Invalid records remain blocked evidence for audit and contamination analysis.

