# May 16 Delivery Notes

## Purpose

This document summarizes the May 16 ML validation intelligence layer delivered under:

`/Users/operry/Projects/osp-lakehouse/ml_validation_layer/`

The work is advisory Dev/Test validation work. It does not promote data, write to R2, alter Gold artifacts, or change NYC 311/FRED production pipeline behavior.

---

# What Was Reviewed

The May 16 work reviewed the master lakehouse architecture requirements and created a first-pass ML validation layer aligned to:

- Gold-only customer-facing production
- Bronze as raw evidence
- Silver as internal transformation evidence
- Quarantine as dirty-data workbench
- Data washing and certification
- Validation defect classification
- Machine-readable validation reporting
- Future Data Washing Agent
- Future Validation Control Agent
- Future Promotion Pipeline Agent

---

# What Was Built

## Documentation

- `docs/ml_work_inventory.md`
- `docs/defect_taxonomy.md`
- `docs/output_guardrails.md`
- `docs/may16_delivery_notes.md`

## Source Code

- `src/defect_severity_scorer.py`
- `src/data_quality_classifier.py`
- `src/recoverability_scorer.py`
- `src/nyc311_anomaly_baseline.py`
- `src/property_risk_score_profiler.py`
- `src/aviation_route_confidence.py`
- `src/validation_report_builder.py`

## Outputs

- `outputs/defect_rules_preview.json`
- `outputs/data_quality_classification_sample.csv`
- `outputs/recoverability_preview.txt`
- `outputs/anomaly_findings_sample.csv`
- `outputs/anomaly_summary.json`
- `outputs/risk_score_profile.csv`
- `outputs/risk_score_quality_findings.json`
- `outputs/route_confidence_sample.csv`
- `outputs/validation_summary.json`
- `metadata/validation_reports/sample_validation_report.json`

---

# Validation Rules Implemented

The May 16 layer includes rules for:

- Schema drift
- Missing required fields
- Missing optional fields
- Duplicate keys
- Range violations
- Null validation status
- Route ambiguity
- Duration outliers
- Source conflict
- Referential integrity failure
- Reconciliation failure
- Security exposure
- Cost threshold concerns

---

# Anomaly Detection Implemented

The NYC 311 anomaly baseline supports:

- Z-score outlier detection
- IQR outlier detection
- Numeric column profiling
- Table-level row and null summaries
- Local anomaly summary generation

The initial focus is on certified Gold outputs, not raw Bronze or internal Silver.

---

# Risk Score Review Implemented

The property risk score profiler supports:

- Score column detection
- Row count
- Null score count
- Min/max/mean/median score
- 95th and 99th percentile score
- Duplicate address count
- Top-risk sample export
- ZIP-level summary when available
- Borough-level summary when available

---

# Aviation Route-Confidence Logic Implemented

The route-confidence prototype scores records using:

- Source system
- Callsign
- ICAO24
- Origin
- Destination
- Duration
- Payload hash
- Source priority

Statuses:

- `certifiable`
- `needs_review`
- `analytical_only`
- `invalid`

Rules:

- FlightAware is production-supporting if validated.
- OpenSky defaults to analytical-only unless explicitly certified.
- Ambiguous callsigns cannot be globally validated.
- Route confidence must use route, callsign, duration, and source confidence.

---

# What Records Would Be Quarantined

Records can be quarantined when they include:

- Blocking validation defects
- Missing required fields
- Duplicate keys
- Range violations
- Null validation status
- Route ambiguity
- Duration outliers
- Source conflicts
- Reconciliation failures
- Security exposure

---

# What Requires Senior Review

Senior review is required before:

- Treating validation output as a promotion gate
- Writing validation results to R2 promotion ledger
- Enforcing CI/CD hard blockers
- Certifying OpenSky data as production truth
- Changing existing NYC 311 or FRED production jobs
- Exposing validation findings to customers
- Moving from advisory reports to automated promotion decisions

---

# What Remains Out of Scope

The May 16 junior ML assignment does not include:

- Full Data Washing Agent
- Full Validation Control Agent
- Full Promotion Pipeline Agent
- CI/CD hard gate enforcement
- R2 production writes
- R2 promotion ledger writes
- Customer-facing FastAPI endpoint
- Credential scanning
- Cost gate enforcement
- Production promotion approval

---

# Final Recommendation

This May 16 layer should be treated as the first reusable validation intelligence package.

It gives future agents the structures they need:

- Defect taxonomy
- Severity scoring
- Recoverability scoring
- Anomaly baselines
- Property score profiling
- Route-confidence scoring
- Machine-readable validation report

The next step is senior review before any of this becomes part of controlled promotion or production CI/CD gates.

