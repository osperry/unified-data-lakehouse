# OSP Lakehouse Build - Run Instructions

Eight sequential steps. Run in order. Each step is self-contained and verifies the prior step before moving on.

Working directory: `~/Projects/osp-lakehouse/`
Mac username: `operry`
Local warehouse: `data/warehouse.duckdb` (or override via `OSP_WAREHOUSE_PATH`)


## Inventory of files delivered

```
osp-lakehouse-build/
├── gates/                                # CI/CD gates library (8 gates)
│   ├── __init__.py
│   ├── core.py                            # base interface, GateResult, ci_cd_log writer
│   ├── schema_gate.py
│   ├── data_quality_gate.py
│   ├── validation_gate.py
│   ├── quarantine_gate.py
│   ├── reconciliation_gate.py
│   ├── cost_gate.py
│   ├── security_gate.py
│   └── promotion_gate.py                  # composite gate
│
├── ml_validation_layer/
│   └── forecasting/
│       ├── __init__.py
│       └── trend_model.py                 # Diebold Ch. 4 trend forecaster
│
├── pipelines/
│   └── nyc311/
│       └── dagster_nyc311/
│           ├── forecasting_assets.py      # Dagster asset + 2 asset checks
│           └── gates_assets.py            # Dagster asset checks for all 8 gates
│
├── dashboards/
│   └── promotion_dashboard/
│       ├── app.py                         # Streamlit dashboard
│       ├── Dockerfile
│       ├── requirements.txt
│       └── docker-compose.snippet.yml     # paste into your compose file
│
├── smoke_test_forecasting.py              # standalone smoke test for trend model
├── dagster_smoke_test_forecasting.py      # Dagster smoke test for forecasting
└── gates_smoke_test.py                    # standalone smoke test for all 8 gates
```


## Step 1 - Place files in repo

```bash
cd ~/Downloads/osp-lakehouse-build

REPO=~/Projects/osp-lakehouse
mkdir -p $REPO/gates
mkdir -p $REPO/ml_validation_layer/forecasting
mkdir -p $REPO/pipelines/nyc311/dagster_nyc311
mkdir -p $REPO/dashboards/promotion_dashboard

cp gates/*.py                                          $REPO/gates/
cp ml_validation_layer/forecasting/*.py                $REPO/ml_validation_layer/forecasting/
cp pipelines/nyc311/dagster_nyc311/*.py                $REPO/pipelines/nyc311/dagster_nyc311/
cp dashboards/promotion_dashboard/*                    $REPO/dashboards/promotion_dashboard/
cp smoke_test_forecasting.py                           $REPO/
cp dagster_smoke_test_forecasting.py                   $REPO/
cp gates_smoke_test.py                                 $REPO/

cd $REPO
ls gates/
ls ml_validation_layer/forecasting/
ls pipelines/nyc311/dagster_nyc311/
ls dashboards/promotion_dashboard/
```

Expected: all directories populated.


## Step 2 - Install dependencies

```bash
cd ~/Projects/osp-lakehouse

# Add new deps
echo "statsmodels" >> requirements.in
echo "streamlit" >> requirements.in
pip-compile requirements.in
pip install -r requirements.txt
```

Verify:
```bash
python3 -c "import statsmodels, streamlit, duckdb, dagster; print('OK')"
```


## Step 3 - Smoke test the trend forecasting module (synthetic data)

```bash
cd ~/Projects/osp-lakehouse
python3 smoke_test_forecasting.py
```

Expected output: per-borough AIC/SIC/RMSE table, 12-month Brooklyn forecast, ci_cd_log entry. Creates throwaway `test_warehouse.duckdb` you can delete after.


## Step 4 - Smoke test the gates library (all 8 gates, synthetic data)

```bash
cd ~/Projects/osp-lakehouse
python3 gates_smoke_test.py
```

Expected output:
- Phase 1: synthetic bronze/silver/gold/quarantine warehouse built
- Phase 2: happy path - all 8 gates PASS, promotion gate verdict PASS
- Phase 3: schema injected failure - schema FAIL, promotion FAIL cascades correctly
- Phase 4: ci_cd_log shows 16 entries (8 gates x 2 runs)

Throwaway warehouse `test_gates.duckdb` is created. Delete after.


## Step 5 - Smoke test the Dagster forecasting wrapper

```bash
cd ~/Projects/osp-lakehouse
python3 dagster_smoke_test_forecasting.py
```

Expected output:
- `Run success: True`
- Asset materializes (1 materialization)
- Both asset checks fire (2 events)
- 60 forecast rows in `gold.fct_complaint_trend_forecast` (5 boroughs x 12 months)
- ci_cd_log entry written

Throwaway `test_warehouse_dagster.duckdb` is created. Delete after.


## Step 6 - Wire Dagster into your existing repo

Open `pipelines/nyc311/dagster_nyc311/__init__.py`. Merge the `defs` objects from both new files into your existing `Definitions(...)` call:

```python
from .forecasting_assets import (
    fct_complaint_trend_forecast,
    trend_forecast_residual_whitenoise,
    trend_forecast_sic_selection_health,
    trend_forecast_job,
    trend_forecast_schedule,
)
from .gates_assets import ALL_CHECKS as GATE_CHECKS, gates_job

defs = Definitions(
    assets=[
        # ... your existing Gold assets here
        fct_complaint_trend_forecast,
    ],
    asset_checks=[
        # ... any existing checks
        trend_forecast_residual_whitenoise,
        trend_forecast_sic_selection_health,
        *GATE_CHECKS,
    ],
    jobs=[
        # ... existing jobs
        trend_forecast_job,
        gates_job,
    ],
    schedules=[
        # ... existing schedules
        trend_forecast_schedule,
    ],
)
```


## Step 7 - Run against your real warehouse via Dagster UI

```bash
cd ~/Projects/osp-lakehouse
export OSP_WAREHOUSE_PATH=data/warehouse.duckdb
dagster dev -m pipelines.nyc311.dagster_nyc311
```

Open `http://localhost:3000`.

Test:
1. Click Materialize on `gold/fct_complaint_trend_forecast`. Both forecasting asset checks should fire. ci_cd_log row added.
2. Re-materialize any existing Gold asset that's in `GOLD_ASSET_CONFIGS` (currently `gold.fct_daily_complaints`). All 8 gate asset checks should fire. ci_cd_log rows added.
3. Open the Asset Lineage view. Confirm `fct_complaint_trend_forecast` shows as downstream of `fct_daily_complaints` with the schedule attached.

If your existing Gold tables use different column names, edit `GOLD_ASSET_CONFIGS` at the top of `gates_assets.py` to match. Add additional Gold assets by appending entries to the same dict using the same shape.


## Step 8 - Boot the Promotion Dashboard

### Option A - Local (no Docker)

```bash
cd ~/Projects/osp-lakehouse/dashboards/promotion_dashboard
OSP_WAREHOUSE_PATH=../../data/warehouse.duckdb streamlit run app.py
```

Opens `http://localhost:8501` (Streamlit default).

### Option B - Docker

Open `docker-compose.yml` at the repo root. Add the service block from `dashboards/promotion_dashboard/docker-compose.snippet.yml` under the `services:` key. Then:

```bash
cd ~/Projects/osp-lakehouse
docker compose up -d promotion_dashboard
open http://localhost:8505
```

Verify:
- Latest run summary shows pass/warn/fail counts from your real ci_cd_log
- Gate status grid renders one row per Gold asset, one column per gate
- Run timeline lists every run_id with status counts
- Drill-down expands any run's gate messages and metrics JSON

To stop:
```bash
docker compose down
```


## Cleanup of test artifacts (optional)

```bash
cd ~/Projects/osp-lakehouse
rm -f test_warehouse.duckdb test_warehouse_dagster.duckdb test_gates.duckdb
# Remove smoke tests from repo root once you've verified they work
rm -f smoke_test_forecasting.py dagster_smoke_test_forecasting.py gates_smoke_test.py
```


## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: gates` | gates/ not on PYTHONPATH | Run `pip install -e .` from repo root, or add `sys.path.insert(0, ".")` to entry script |
| All gates WARN on validation gate | No validation_reports table | ML validation layer hasn't run yet; gate correctly flags missing report |
| Quarantine gate FAILs on orphans | Aggregated Gold asset configured as row-level | Set `aggregated: True` in GOLD_ASSET_CONFIGS for that asset |
| Dashboard shows "No ci_cd_log entries" | Warehouse path wrong, or no gates have run | Verify OSP_WAREHOUSE_PATH points to the same warehouse Dagster writes to |
| Dagster UI doesn't show new assets | __init__.py not merged correctly | Check `dagster dev -m pipelines.nyc311.dagster_nyc311` finds your Definitions |


## Master document

`lakehouse_architecture_flow_report_v7.docx` includes new Sections 16 (Gates Framework), 17 (Promotion Dashboard), 18 (Trend Forecasting Layer), and action items P16-P19.
