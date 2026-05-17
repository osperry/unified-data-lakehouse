# Output Guardrails

## Purpose

This document defines where the May 16 ML validation layer may and may not write.

## Allowed Write Locations

The May 16 ML validation layer may write only to:

- `ml_validation_layer/outputs/`
- `ml_validation_layer/metadata/validation_reports/`
- `ml_validation_layer/docs/`

## Prohibited Write Locations

The May 16 ML validation layer must not write to:

- `r2://osp-aviation-lakehouse/gold/`
- `r2://osp-aviation-lakehouse/silver/`
- `r2://osp-aviation-lakehouse/bronze/`
- `pipelines/nyc311/data/gold_export/`
- `pipelines/fred/data/`
- Any customer-facing production path

## Production Rule

The ML validation layer is advisory Dev/Test work. It may read certified Gold outputs, classify records, profile risk scores, detect anomalies, and create validation reports.

It may not promote data, overwrite Gold, push to R2, change production artifacts, or alter existing NYC 311/FRED pipeline behavior.

## Master Architecture Alignment

- Gold is the certified customer-facing layer.
- Bronze is raw evidence.
- Silver is internal transformation evidence.
- Quarantine is never production-facing.
- No direct Mac/Docker-to-production push is allowed.
- Promotion requires controlled Dev/Test -> Staging -> Production gates.

