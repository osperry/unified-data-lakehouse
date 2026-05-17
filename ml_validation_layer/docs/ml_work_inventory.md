
# ML Work Inventory

## Purpose

This document separates the work in the lakehouse architecture into:

1. True machine learning work

2. ML-adjacent validation intelligence

3. MLOps / agent-readiness work

4. Data engineering / platform work

5. Out-of-scope work for the May 16 junior ML engineering assignment

The purpose is to keep the May 16 work aligned to the master architecture document.

The junior ML engineer is not being asked to promote production data, alter R2 Gold artifacts, modify existing NYC 311 or FRED pipeline behavior, or build the full autonomous agents. The assignment is to build the first-pass ML validation intelligence layer that future agents can consume.

---

# 1. True Machine Learning / Statistical ML Work

## NYC 311 anomaly detection

This is true ML or statistical ML work.

The validated Gold layer includes NYC 311 customer-facing outputs such as:

- `fct_property_incidents`

- `fct_address_risk_score`

- `fct_daily_complaints`

- `fct_borough_summary`

- `fct_anomaly_summary`

- `fct_resolution_anomalies`

The ML validation layer should inspect those Gold tables for abnormal patterns.

Expected checks:

- Daily complaint-count spikes

- Borough-level complaint spikes

- Complaint-type anomalies

- Agency-level abnormal resolution time

- Address-level abnormal complaint concentration

- Resolution-time outliers

Initial methods:

- Rolling average

- Rolling standard deviation

- Z-score

- IQR

- Percentile thresholds

Stretch methods:

- Isolation Forest

- Local Outlier Factor

- Seasonal anomaly baseline

Output examples:

- `outputs/anomaly_findings_sample.csv`

- `outputs/anomaly_summary.json`

---

## Property risk score profiling

This is ML-adjacent model validation work.

The table `fct_address_risk_score` is a premium Gold table containing property risk scores from 0 to 100.

The May 16 work should profile the score, not redesign it.

Expected checks:

- Row count

- Null score count

- Duplicate address count

- Minimum score

- Maximum score

- Mean score

- Median score

- Score distribution

- Top 1% high-risk addresses

- Bottom 1% low-risk addresses

- ZIP-level distribution

- Borough-level distribution

- Outlier scores

Output examples:

- `outputs/risk_score_profile.csv`

- `outputs/top_risk_addresses_sample.csv`

- `outputs/risk_score_quality_findings.json`

---

## Aviation route-confidence scoring

This is ML-adjacent scoring work and future ML work.

FlightAware is the premium aviation source. OpenSky is analytical support only unless explicitly certified.

The May 16 work should create a route-confidence scoring prototype using:

- Source system

- Callsign

- ICAO24

- Origin

- Destination

- Timestamp completeness

- Duration

- Payload hash

- Route pair

- Historical route evidence

- Source priority

Expected statuses:

- `certifiable`

- `needs_review`

- `analytical_only`

- `invalid`

Output example:

- `outputs/route_confidence_sample.csv`

---

## Dirty-record classification baseline

This is true ML-readiness work.

The first version may be rules-first, but the structure should prepare for future supervised learning.

Expected statuses:

- `valid`

- `pending`

- `invalid`

- `quarantine_candidate`

Expected output fields:

- `record_id`

- `source_system`

- `domain`

- `validation_status`

- `defect_type`

- `severity`

- `quarantine_flag`

- `recoverability_score`

- `reason_codes`

Output example:

- `outputs/data_quality_classification_sample.csv`

---

# 2. ML-Adjacent Validation Intelligence

## Defect severity scoring

This supports the future Validation Control Agent.

Defect types:

- `schema_drift`

- `missing_required_field`

- `missing_optional_field`

- `duplicate_key`

- `range_violation`

- `null_validation_status`

- `route_ambiguous`

- `duration_outlier`

- `source_conflict`

- `referential_integrity_failure`

- `reconciliation_failure`

- `security_exposure`

- `cost_threshold_exceeded`

Severity levels:

- `info`

- `low`

- `medium`

- `high`

- `blocking`

Example rules:

- `null_validation_status` = `blocking`

- Gold reconciliation mismatch = `blocking`

- OpenSky treated as production truth = `blocking`

- Missing required field = `high`

- Duplicate key = `high`

- Missing optional field = `low`

---

## Recoverability scoring

This supports quarantine mining.

The master architecture treats pending records as a work queue, not trash. Quarantined records should be reviewed for recoverable value before promotion or rejection.

Example recoverability scoring:

- `0.90` = missing optional field only

- `0.70` = route ambiguous but callsign, duration, and source evidence exist

- `0.60` = duplicate key but payload hash differs

- `0.40` = range violation with recoverable source evidence

- `0.20` = missing origin or destination

- `0.10` = invalid timestamp and no payload hash

- `0.00` = security exposure or null validation status

---

# 3. MLOps / Agent-Readiness Work

## Machine-readable validation report

This is not a full agent. It is the contract future agents will consume.

Expected report location:

- `metadata/validation_reports/sample_validation_report.json`

Expected report fields:

- `run_id`

- `repo_root`

- `environment`

- `input_layer`

- `input_source`

- `domains`

- `input_tables`

- `row_counts`

- `valid_count`

- `pending_count`

- `invalid_count`

- `quarantine_count`

- `anomaly_count`

- `blocking_failures`

- `warnings`

- `promotion_recommendation`

- `created_at_utc`

For the May 16 assignment, `promotion_recommendation` should default to:

- `REVIEW`

The junior engineer should not issue production promotion approval.

---

## Validation summary

The May 16 delivery should include a written validation summary answering:

- What was reviewed?

- What tables were inspected?

- What validation rules were implemented?

- What anomalies were detected?

- What defects were classified?

- What records would be quarantined?

- What score-quality concerns exist?

- What route-confidence logic was created?

- What is ready for senior review?

- What remains out of scope?

Expected file:

- `docs/may16_delivery_notes.md`

---

# 4. Data Engineering / Platform Work

The following items are important to the lakehouse, but they are not the junior ML engineer’s May 16 ownership area:

- R2 upload

- Dagster scheduling

- dbt builds

- Docker service orchestration

- FRED bronze mounting into NYC 311 container

- Gold Parquet export

- Docker Compose service management

- Makefile operational targets

- Pipeline environment variables

- Existing NYC 311 and FRED production jobs

The May 16 ML validation layer may read certified Gold outputs, but it must not alter the production data pipeline.

---

# 5. Out of Scope for the May 16 Junior ML Engineer Assignment

The junior ML engineer should not own:

- Production promotion

- CI/CD hard-gate enforcement

- R2 production writes

- R2 promotion ledger writes

- Security gate implementation

- Credential scanning

- Cost gate enforcement

- Customer-facing FastAPI endpoint

- Full Data Washing Agent

- Full Validation Control Agent

- Full Promotion Pipeline Agent

- Any direct modification to `r2://osp-aviation-lakehouse/gold/`

- Any change to existing NYC 311 or FRED production pipeline behavior

---

# 6. Guardrail

The May 16 assignment operates under this rule:

> I am building the ML validation intelligence layer inside `osp-lakehouse/ml_validation_layer/`. I may read certified Gold outputs from R2. I may produce local validation reports and findings. I may not promote data, overwrite Gold, modify production R2 objects, or alter the existing NYC 311/FRED pipeline behavior.

---

# 7. May 16 Deliverables Connected to This Inventory

The May 16 work should produce:

- `docs/ml_work_inventory.md`

- `docs/defect_taxonomy.md`

- `docs/may16_delivery_notes.md`

- `src/data_quality_classifier.py`

- `src/defect_severity_scorer.py`

- `src/nyc311_anomaly_baseline.py`

- `src/property_risk_score_profiler.py`

- `src/aviation_route_confidence.py`

- `src/validation_report_builder.py`

- `metadata/validation_reports/sample_validation_report.json`

- `outputs/data_quality_classification_sample.csv`

- `outputs/anomaly_findings_sample.csv`

- `outputs/anomaly_summary.json`

- `outputs/risk_score_profile.csv`

- `outputs/risk_score_quality_findings.json`

- `outputs/route_confidence_sample.csv`

- `outputs/validation_summary.json`

