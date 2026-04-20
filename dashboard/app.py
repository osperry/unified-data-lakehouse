import streamlit as st
import duckdb
import pandas as pd
import os

st.set_page_config(page_title="Unified Data Lakehouse", layout="wide")
st.title("Unified Data Lakehouse Dashboard")
st.caption("NYC 311 Complaints + FRED Macroeconomic Indicators")

db_path = os.getenv("WAREHOUSE_PATH", "/app/data/lakehouse.duckdb")
con = duckdb.connect(db_path, read_only=True)

# ── Overview Metrics ──────────────────────────────────────────────────────────
total = con.execute("SELECT count(*) FROM main_silver.stg_complaints").fetchone()[0]
avg_res = con.execute(
    "SELECT round(avg(avg_resolution_days), 1) FROM main_gold.fct_daily_complaints"
).fetchone()[0]
anomaly_total = con.execute(
    "SELECT count(*) FROM main_gold.fct_resolution_anomalies"
).fetchone()[0]
precincts = con.execute(
    "SELECT count(distinct police_precinct) FROM main_gold.fct_borough_summary"
).fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Complaints", f"{total:,}")
c2.metric("Avg Resolution Days", avg_res)
c3.metric("Resolution Anomalies", f"{anomaly_total:,}")
c4.metric("Active Precincts", precincts)

st.divider()

# ── Daily Complaint Volume ────────────────────────────────────────────────────
st.subheader("Daily Complaint Volume by Borough")

boroughs = con.execute(
    "SELECT DISTINCT borough FROM main_gold.fct_daily_complaints ORDER BY borough"
).fetchdf()["borough"].tolist()

selected_boroughs = st.multiselect(
    "Filter by Borough",
    boroughs,
    default=[b for b in boroughs if b != "UNSPECIFIED"]
)

if selected_boroughs:
    placeholders = ",".join(["?" for _ in selected_boroughs])
    daily = con.execute(
        f"""SELECT complaint_date, borough, total_complaints
            FROM main_gold.fct_daily_complaints
            WHERE borough IN ({placeholders})
            ORDER BY complaint_date""",
        selected_boroughs
    ).fetchdf()

    daily_pivot = daily.pivot(
        index="complaint_date", columns="borough", values="total_complaints"
    ).fillna(0)
    st.line_chart(daily_pivot)
else:
    st.info("Select at least one borough.")

st.divider()

# ── Precinct Rankings ─────────────────────────────────────────────────────────
st.subheader("Precinct Priority Rankings")
st.caption("Precincts with 1,000+ complaints. Ranked by open rate descending.")

precinct_df = con.execute(
    """SELECT priority_rank, police_precinct, borough,
              total_complaints, open_rate_pct, avg_resolution_days,
              anomaly_count, anomaly_rate_pct, top_complaint_type
       FROM main_gold.fct_borough_summary
       ORDER BY priority_rank"""
).fetchdf()

st.dataframe(precinct_df, use_container_width=True, hide_index=True)

st.divider()

# ── Resolution Anomalies ─────────────────────────────────────────────────────
st.subheader("Resolution Anomalies")
st.caption("Complaints closed before they were created.")

col_a, col_b = st.columns(2)

with col_a:
    st.write("**Anomalies by Year**")
    by_year = con.execute(
        """SELECT complaint_year as year, sum(anomaly_count) as anomalies
           FROM main_gold.fct_anomaly_summary
           GROUP BY complaint_year
           ORDER BY complaint_year"""
    ).fetchdf()
    st.bar_chart(by_year.set_index("year"))

with col_b:
    st.write("**Top Agencies**")
    by_agency = con.execute(
        """SELECT agency, sum(anomaly_count) as anomalies
           FROM main_gold.fct_anomaly_summary
           GROUP BY agency
           ORDER BY anomalies DESC
           LIMIT 10"""
    ).fetchdf()
    st.bar_chart(by_agency.set_index("agency"))

st.write("**Anomaly Detail (worst cases)**")
anomaly_detail = con.execute(
    """SELECT complaint_id, created_date, closed_date, resolution_days,
              complaint_type, agency, borough, police_precinct
       FROM main_gold.fct_resolution_anomalies
       WHERE resolution_days BETWEEN -1000 AND -1
       ORDER BY resolution_days
       LIMIT 50"""
).fetchdf()
st.dataframe(anomaly_detail, use_container_width=True, hide_index=True)

st.divider()

# ── Economic Correlation ──────────────────────────────────────────────────────
st.subheader("Economic Indicators vs. Service Demand")
st.caption("Monthly FRED macro data overlaid with NYC 311 complaint volume.")

econ = con.execute(
    """SELECT month_date, total_complaints, anomaly_count,
              avg_resolution_days, unemployment_rate, cpi, fed_funds_rate
       FROM main_gold.fct_economic_service_correlation
       ORDER BY month_date"""
).fetchdf()

col_e1, col_e2 = st.columns(2)

with col_e1:
    st.write("**Complaint Volume vs. Unemployment**")
    econ_chart1 = econ[["month_date", "total_complaints", "unemployment_rate"]].dropna()
    if not econ_chart1.empty:
        chart1 = econ_chart1.set_index("month_date")
        st.line_chart(chart1)

with col_e2:
    st.write("**Resolution Days vs. Fed Funds Rate**")
    econ_chart2 = econ[["month_date", "avg_resolution_days", "fed_funds_rate"]].dropna()
    if not econ_chart2.empty:
        chart2 = econ_chart2.set_index("month_date")
        st.line_chart(chart2)

st.divider()

# ── FRED Macro Indicators ────────────────────────────────────────────────────
st.subheader("FRED Macroeconomic Indicators")

macro = con.execute(
    """SELECT observation_date, unemployment_rate, cpi, fed_funds_rate, gdp
       FROM main_gold.fct_macro_daily
       WHERE observation_date >= '2020-01-01'
       ORDER BY observation_date"""
).fetchdf()

indicator = st.selectbox(
    "Select Indicator",
    ["unemployment_rate", "cpi", "fed_funds_rate", "gdp"]
)

macro_filtered = macro[["observation_date", indicator]].dropna()
if not macro_filtered.empty:
    st.line_chart(macro_filtered.set_index("observation_date"))

st.divider()
st.caption("Data: NYC Open Data (311 Complaints) | FRED (Federal Reserve Economic Data)")
st.caption("Pipeline: Python + DuckDB + dbt + Dagster | Dashboard: Streamlit")

con.close()
