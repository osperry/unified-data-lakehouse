# Unified Data Lakehouse

Combines macroeconomic indicators (FRED) and urban service demand
(NYC 311) into a single DuckDB lakehouse with medallion architecture.

## Architecture

```
Bronze (raw JSON) -> Silver (cleaned, typed, deduped) -> Gold (business logic)
```

## Data Sources

| Source | Records | Date Range | Refresh |
|---|---|---|---|
| FRED API | ~21K observations | 1947-present | Full refresh |
| NYC 311 Socrata API | ~19.3M complaints | 2020-present | Incremental (watermark) |

## Gold Layer Tables

| Table | Description |
|---|---|
| fct_macro_daily | Pivoted FRED indicators by date |
| fct_daily_complaints | Daily 311 volume by borough |
| fct_borough_summary | Precinct rankings with anomaly rates |
| fct_resolution_anomalies | Individual complaints closed before created |
| fct_anomaly_summary | Aggregated anomaly patterns by agency/precinct/year |
| fct_economic_service_correlation | Monthly macro indicators joined with complaint volume |

## Stack

Python, DuckDB, dbt-core, Dagster, Streamlit, FastAPI, Docker Compose

## Governance

See [GOVERNANCE.md](GOVERNANCE.md) for data governance controls mapped
to NIST RMF, ISO 42001, and CRISC frameworks.

## Quick Start

```bash
docker compose build
docker compose run --service-ports lakehouse bash

# Inside container:
python /app/extract/fred_extract.py
python /app/extract/nyc311_extract.py
cd /app/models/lakehouse_marts && dbt build --profiles-dir .
```

## Key Finding

Analysis of 19.3M NYC 311 complaints revealed 32,527 complaints
closed before they were created. 98.9% originate from DOT
(Department of Transportation) street light condition complaints,
concentrated in Bronx and Queens precincts. Pattern declined from
25.8% anomaly rate in 2020 to 1.1% in 2026.
