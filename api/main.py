from fastapi import FastAPI, Query
from typing import Optional
import duckdb
import os

app = FastAPI(
    title="Unified Data Lakehouse API",
    description="REST API serving gold layer tables from the FRED + NYC 311 lakehouse",
    version="1.0.0"
)

DB_PATH = os.getenv("WAREHOUSE_PATH", "/app/data/lakehouse.duckdb")


def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


@app.get("/complaints/daily")
def daily_complaints(
    borough: Optional[str] = Query(None, description="Filter by borough"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000)
):
    con = get_con()
    query = "SELECT * FROM main_gold.fct_daily_complaints WHERE 1=1"
    params = []
    if borough:
        query += " AND borough = ?"
        params.append(borough.upper())
    if start:
        query += " AND complaint_date >= ?"
        params.append(start)
    if end:
        query += " AND complaint_date <= ?"
        params.append(end)
    query += " ORDER BY complaint_date DESC LIMIT ?"
    params.append(limit)
    rows = con.execute(query, params).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


@app.get("/precincts/rankings")
def precinct_rankings():
    con = get_con()
    rows = con.execute(
        """SELECT * FROM main_gold.fct_borough_summary
           ORDER BY priority_rank"""
    ).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


@app.get("/anomalies/summary")
def anomaly_summary(
    agency: Optional[str] = Query(None, description="Filter by agency"),
    year: Optional[int] = Query(None, description="Filter by year"),
    limit: int = Query(100, ge=1, le=5000)
):
    con = get_con()
    query = "SELECT * FROM main_gold.fct_anomaly_summary WHERE 1=1"
    params = []
    if agency:
        query += " AND agency = ?"
        params.append(agency.upper())
    if year:
        query += " AND complaint_year = ?"
        params.append(year)
    query += " ORDER BY anomaly_count DESC LIMIT ?"
    params.append(limit)
    rows = con.execute(query, params).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


@app.get("/anomalies/detail")
def anomaly_detail(
    agency: Optional[str] = Query(None, description="Filter by agency"),
    borough: Optional[str] = Query(None, description="Filter by borough"),
    limit: int = Query(50, ge=1, le=1000)
):
    con = get_con()
    query = """SELECT complaint_id, created_date, closed_date, resolution_days,
                      complaint_type, agency, borough, police_precinct
               FROM main_gold.fct_resolution_anomalies WHERE 1=1"""
    params = []
    if agency:
        query += " AND agency = ?"
        params.append(agency.upper())
    if borough:
        query += " AND borough = ?"
        params.append(borough.upper())
    query += " ORDER BY resolution_days LIMIT ?"
    params.append(limit)
    rows = con.execute(query, params).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


@app.get("/macro/daily")
def macro_daily(
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000)
):
    con = get_con()
    query = "SELECT * FROM main_gold.fct_macro_daily WHERE 1=1"
    params = []
    if start:
        query += " AND observation_date >= ?"
        params.append(start)
    if end:
        query += " AND observation_date <= ?"
        params.append(end)
    query += " ORDER BY observation_date DESC LIMIT ?"
    params.append(limit)
    rows = con.execute(query, params).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


@app.get("/correlation/monthly")
def economic_correlation():
    con = get_con()
    rows = con.execute(
        """SELECT * FROM main_gold.fct_economic_service_correlation
           ORDER BY month_date"""
    ).fetchdf()
    con.close()
    return rows.to_dict(orient="records")
