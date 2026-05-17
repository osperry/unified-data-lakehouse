.PHONY: up down build nyc311 fred logs-nyc311 logs-fred restart clean ps

# ── Both pipelines ─────────────────────────────────────────────────────────────
up:
	docker compose up

build:
	docker compose up --build

down:
	docker compose down

restart:
	docker compose down && docker compose up --build

# ── Individual pipelines ───────────────────────────────────────────────────────
nyc311:
	docker compose up --build nyc311

fred:
	docker compose up --build fred

# ── Logs ──────────────────────────────────────────────────────────────────────
logs-nyc311:
	docker compose logs -f nyc311

logs-fred:
	docker compose logs -f fred

# ── Housekeeping ───────────────────────────────────────────────────────────────
ps:
	docker compose ps

clean:
	docker compose down --remove-orphans
	docker container prune -f

# -----------------------------
# ML Validation Layer - May 16
# -----------------------------

ml-inventory:
	cd ml_validation_layer && cat docs/ml_work_inventory.md

ml-defects:
	cd ml_validation_layer && ../.venv/bin/python src/defect_severity_scorer.py > outputs/defect_rules_preview.json

ml-classify:
	cd ml_validation_layer && ../.venv/bin/python src/data_quality_classifier.py --output outputs/data_quality_classification_sample.csv

ml-recoverability:
	cd ml_validation_layer && ../.venv/bin/python src/recoverability_scorer.py > outputs/recoverability_preview.txt

ml-anomaly:
	cd ml_validation_layer && ../.venv/bin/python src/nyc311_anomaly_baseline.py --gold-dir ../pipelines/nyc311/data/gold_export --output-dir outputs

ml-risk-score:
	cd ml_validation_layer && ../.venv/bin/python src/property_risk_score_profiler.py --input ../pipelines/nyc311/data/gold_export/fct_address_risk_score.parquet --output-dir outputs

ml-route-confidence:
	cd ml_validation_layer && ../.venv/bin/python src/aviation_route_confidence.py --output outputs/route_confidence_sample.csv

ml-validate:
	cd ml_validation_layer && ../.venv/bin/python src/validation_report_builder.py --repo-root /Users/operry/Projects/osp-lakehouse --output-dir outputs --report-path metadata/validation_reports/sample_validation_report.json

ml-all: ml-defects ml-classify ml-recoverability ml-anomaly ml-risk-score ml-route-confidence ml-validate
	@echo "ML validation layer run complete. Review ml_validation_layer/outputs and ml_validation_layer/metadata/validation_reports."


nyc311-export-gold-local:
	mkdir -p pipelines/nyc311/data/gold_export
	WAREHOUSE_PATH="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/data/nyc311.duckdb" \
	PARQUET_EXPORT_DIR="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/gold_export" \
	R2_ENDPOINT_URL="" \
	R2_ACCESS_KEY_ID="" \
	R2_SECRET_ACCESS_KEY="" \
	.venv/bin/python pipelines/nyc311/models/gold_model_premium.py

ml-real-gold-test: nyc311-export-gold-local ml-all
	@echo "Real Gold ML validation test complete."

nyc311-export-gold-local:
	mkdir -p pipelines/nyc311/data/gold_export
	WAREHOUSE_PATH="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/data/nyc311.duckdb" \
	PARQUET_EXPORT_DIR="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/gold_export" \
	R2_ENDPOINT_URL="" \
	R2_ACCESS_KEY_ID="" \
	R2_SECRET_ACCESS_KEY="" \
	.venv/bin/python pipelines/nyc311/models/gold_model_premium.py

ml-real-gold-test: nyc311-export-gold-local ml-all
	@echo "Real Gold ML validation test complete."

ml-health-check:
	@echo "---- Blocking failures ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep -A20 '"blocking_failures"'
	@echo ""
	@echo "---- Warnings ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep -A30 '"warnings"'
	@echo ""
	@echo "---- Risk score status ----"
	@cat ml_validation_layer/outputs/risk_score_quality_findings.json | grep '"status"'
	@echo ""
	@echo "---- Missing tables ----"
	@cat ml_validation_layer/outputs/anomaly_summary.json | grep -A20 '"missing_tables"'
	@echo ""
	@echo "---- Promotion recommendation ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep '"promotion_recommendation"'

# -----------------------------
# Local Gold Export + ML Test
# -----------------------------

nyc311-export-gold-local:
	mkdir -p pipelines/nyc311/data/gold_export
	WAREHOUSE_PATH="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/data/nyc311.duckdb" \
	PARQUET_EXPORT_DIR="/Users/operry/Projects/osp-lakehouse/pipelines/nyc311/data/gold_export" \
	R2_ENDPOINT_URL="" \
	R2_ACCESS_KEY_ID="" \
	R2_SECRET_ACCESS_KEY="" \
	.venv/bin/python pipelines/nyc311/models/gold_model_premium.py

ml-health-check:
	@echo "---- Blocking failures ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep -A20 '"blocking_failures"'
	@echo ""
	@echo "---- Warnings ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep -A30 '"warnings"'
	@echo ""
	@echo "---- Risk score status ----"
	@cat ml_validation_layer/outputs/risk_score_quality_findings.json | grep '"status"'
	@echo ""
	@echo "---- Missing tables ----"
	@cat ml_validation_layer/outputs/anomaly_summary.json | grep -A20 '"missing_tables"'
	@echo ""
	@echo "---- Promotion recommendation ----"
	@cat ml_validation_layer/metadata/validation_reports/sample_validation_report.json | grep '"promotion_recommendation"'

ml-real-gold-test: nyc311-export-gold-local ml-all ml-health-check
	@echo "Real Gold ML validation test complete."
