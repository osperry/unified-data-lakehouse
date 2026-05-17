"""
dashboards/promotion_dashboard/app.py

Streamlit Promotion Dashboard. Reads ci_cd_log from the warehouse and renders:
  - Latest run summary (pass/warn/fail counts across all gates and assets)
  - Per-asset gate history grid (rows = assets, columns = gates, cells = status)
  - Run timeline with promotion verdicts over time
  - Drill-down for any selected run_id showing all gate messages and metrics

Run locally:
    OSP_WAREHOUSE_PATH=data/warehouse.duckdb streamlit run app.py

Run in Docker:
    docker compose up promotion_dashboard
    open http://localhost:8505
"""

import json
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import streamlit as st


WAREHOUSE_PATH = os.environ.get("OSP_WAREHOUSE_PATH", "data/warehouse.duckdb")
LOG_TABLE = os.environ.get("OSP_CI_CD_LOG_TABLE", "ci_cd_log")

STATUS_EMOJI = {"pass": "🟢", "warn": "🟡", "fail": "🔴"}
STATUS_COLORS = {"pass": "#16a34a", "warn": "#f59e0b", "fail": "#dc2626"}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_log() -> pd.DataFrame:
    if not os.path.exists(WAREHOUSE_PATH):
        return pd.DataFrame()
    with duckdb.connect(WAREHOUSE_PATH, read_only=True) as con:
        try:
            return con.sql(f"""
                SELECT run_id, asset_name, gate_name, status, message,
                       metrics, executed_at
                FROM {LOG_TABLE}
                ORDER BY executed_at DESC
            """).df()
        except Exception:
            return pd.DataFrame()


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="OSP Promotion Dashboard",
    page_icon="🛡️",
    layout="wide",
)

st.title("🛡️ OSP Promotion Dashboard")
st.caption(f"Warehouse: `{WAREHOUSE_PATH}`  •  Log table: `{LOG_TABLE}`")

df = load_log()

if df.empty:
    st.warning(
        "No ci_cd_log entries found yet. Run the gates or trend forecast "
        "against your warehouse, then refresh."
    )
    st.stop()


# ---------------------------------------------------------------------------
# Top summary
# ---------------------------------------------------------------------------

st.markdown("### Latest run summary")
latest_run_id = df.iloc[0]["run_id"]
latest = df[df["run_id"] == latest_run_id]
counts = latest["status"].value_counts().to_dict()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Run ID", latest_run_id)
col2.metric("🟢 PASS", counts.get("pass", 0))
col3.metric("🟡 WARN", counts.get("warn", 0))
col4.metric("🔴 FAIL", counts.get("fail", 0))


# ---------------------------------------------------------------------------
# Promotion verdicts per asset for the latest run
# ---------------------------------------------------------------------------

st.markdown("### Promotion verdict per asset (latest run)")
promotion_rows = latest[latest["gate_name"] == "promotion"]
if promotion_rows.empty:
    st.info("No promotion gate results in latest run.")
else:
    verdict_df = promotion_rows[["asset_name", "status", "message"]].copy()
    verdict_df["verdict"] = verdict_df["status"].map(
        lambda s: f"{STATUS_EMOJI.get(s, '')} {s.upper()}"
    )
    st.dataframe(
        verdict_df[["asset_name", "verdict", "message"]],
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Per-asset gate grid
# ---------------------------------------------------------------------------

st.markdown("### Gate status grid (latest run per asset)")
latest_per_asset = (
    df.sort_values("executed_at", ascending=False)
    .groupby(["asset_name", "gate_name"], as_index=False)
    .first()
)
grid = latest_per_asset.pivot_table(
    index="asset_name",
    columns="gate_name",
    values="status",
    aggfunc="first",
)


def _style_cell(v):
    if pd.isna(v):
        return "background-color: #f3f4f6;"
    return f"background-color: {STATUS_COLORS.get(v, '#fff')}; color: white;"


styled = grid.style.map(_style_cell)
st.dataframe(styled, use_container_width=True)


# ---------------------------------------------------------------------------
# Run timeline
# ---------------------------------------------------------------------------

st.markdown("### Run timeline")
timeline = (
    df.groupby("run_id")
    .agg(
        executed_at=("executed_at", "min"),
        n_pass=("status", lambda s: (s == "pass").sum()),
        n_warn=("status", lambda s: (s == "warn").sum()),
        n_fail=("status", lambda s: (s == "fail").sum()),
        n_assets=("asset_name", "nunique"),
        n_gates=("gate_name", "count"),
    )
    .reset_index()
    .sort_values("executed_at", ascending=False)
)
st.dataframe(timeline, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Drill-down
# ---------------------------------------------------------------------------

st.markdown("### Drill-down")
selected_run = st.selectbox("Pick a run_id", options=timeline["run_id"].tolist())
if selected_run:
    sub = df[df["run_id"] == selected_run].copy()
    sub["status_emoji"] = sub["status"].map(STATUS_EMOJI)
    st.write(f"**{len(sub)} gate results for run `{selected_run}`**")
    for _, row in sub.iterrows():
        with st.expander(
            f"{row['status_emoji']} {row['gate_name']} on `{row['asset_name']}` "
            f"— {row['status'].upper()}"
        ):
            st.write(row["message"])
            if row["metrics"]:
                try:
                    m = json.loads(row["metrics"]) if isinstance(row["metrics"], str) else row["metrics"]
                    st.json(m)
                except Exception:
                    st.code(str(row["metrics"]))
            st.caption(f"executed_at: {row['executed_at']}")
