# Data Governance Framework

## Unified Data Lakehouse: FRED + NYC 311

### 1. PII Detection
No PII is collected or stored. NYC 311 complaint data contains
incident addresses (public record) but no personal identifiers.
FRED data is publicly available macroeconomic indicators.

### 2. Data Minimization
Only fields required for analysis are promoted from bronze to silver.
43 raw NYC 311 fields are reduced to 20 in silver. FRED observations
retain only series_id, date, and value.

### 3. Retention Policy
Bronze: Raw JSON files retained indefinitely as immutable audit trail.
Silver: Rebuilt on each full refresh from bronze.
Gold: Derived from silver on each build. No independent persistence.

### 4. Access Control
Bronze: Write access restricted to extraction scripts only.
Silver/Gold: Read-only for dashboard and API consumers.
DuckDB file-level access controlled by filesystem permissions.
Tiered access model: data contracts define who consumes what.

### 5. Audit Trail
Watermark files in bronze track extraction state.
dbt build logs provide full lineage of every transformation.
Each bronze file is timestamped at extraction time.
All silver/gold tables are fully reproducible from bronze.

### 6. Data Quality (Detective Controls)
dbt tests enforce constraints at the silver layer:
- unique_key uniqueness (dedup verification)
- not_null on critical fields (complaint_id, created_date, status)
- accepted_values on categorical fields (borough)
- Resolution anomaly flag (is_resolution_anomaly) surfaces
  complaints closed before they were created.

### Control Mapping

| Control | NIST RMF | ISO 42001 | CRISC |
|---|---|---|---|
| PII Detection | SI-12 | A.8.4 | Risk Assessment |
| Data Minimization | SA-8 | A.6.1.4 | Risk Response |
| Retention Policy | SI-12 | A.8.4 | Risk Monitoring |
| Access Control | AC-3 | A.6.1.2 | Risk Response |
| Audit Trail | AU-3 | A.9.3 | Risk Monitoring |
| Data Quality | SI-10 | A.8.2 | Risk Assessment |
