"""
trend_model.py

Trend forecasting for NYC 311 complaint volume.

Fits linear and quadratic trend models per borough on monthly-aggregated counts,
selects the better model via SIC (Schwarz Information Criterion, consistent per
Diebold Ch. 4), and produces a 12-month point forecast with 95% prediction intervals.

Output writes to gold.fct_complaint_trend_forecast. Run summary is logged to
ci_cd_log so the Promotion Dashboard picks it up alongside gate results.

Reference:
    Diebold, F.X. "Forecasting in Economics, Business, Finance and Beyond,"
    Chapter 4 - Modeling and Forecasting Trend.

Wiring:
    - Reads:  gold.fct_daily_complaints
    - Writes: gold.fct_complaint_trend_forecast
    - Logs:   ci_cd_log (gate_name = 'trend_forecast')
    - Shape:  returns metadata dict, designed to be a Dagster @asset return value.

Usage:
    from ml_validation_layer.forecasting.trend_model import run_trend_forecast

    result = run_trend_forecast(
        warehouse_path="data/warehouse.duckdb",
        source_table="gold.fct_daily_complaints",
        target_table="gold.fct_complaint_trend_forecast",
        forecast_horizon=12,
    )
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox


# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_SOURCE = "gold.fct_daily_complaints"
DEFAULT_TARGET = "gold.fct_complaint_trend_forecast"
DEFAULT_HORIZON = 12              # months ahead
DEFAULT_PI_LEVEL = 0.95           # prediction interval coverage
LJUNG_BOX_LAGS = 12               # white noise check window
LJUNG_BOX_ALPHA = 0.05            # reject white noise below this p-value
SIC_TIEBREAK_BAND = 2.0           # standard SIC delta threshold for "strong evidence"
MIN_OBS_DEFAULT = 24              # require at least 2 years of monthly data per borough


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------

@dataclass
class ModelFit:
    """Summary of one fitted trend model."""
    model_type: str                       # "linear" or "quadratic"
    aic: float
    sic: float
    mse: float
    rmse: float
    r2_adj: float
    n_params: int
    residual_whitenoise_pvalue: float     # Ljung-Box p-value
    residual_whitenoise_pass: bool        # True if residuals look like white noise

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BoroughResult:
    """Trend fit + forecast metadata for a single borough."""
    borough: str
    n_obs: int
    fit_linear: ModelFit
    fit_quadratic: ModelFit
    selected_model: str                   # "linear" or "quadratic"
    selection_reason: str

    def to_dict(self) -> dict:
        return {
            "borough": self.borough,
            "n_obs": self.n_obs,
            "fit_linear": self.fit_linear.to_dict(),
            "fit_quadratic": self.fit_quadratic.to_dict(),
            "selected_model": self.selected_model,
            "selection_reason": self.selection_reason,
        }


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_monthly_complaints(
    warehouse_path: str,
    source_table: str = DEFAULT_SOURCE,
    date_col: str = "complaint_date",
    borough_col: str = "borough",
    count_col: str = "complaint_count",
) -> pd.DataFrame:
    """
    Load fct_daily_complaints and aggregate to monthly totals per borough.

    Returns
    -------
    pd.DataFrame with columns:
        borough        VARCHAR
        month_start    DATE (first day of the month)
        monthly_count  BIGINT
        t              INT (1..N time index per borough)
    """
    sql = f"""
        SELECT
            {borough_col} AS borough,
            DATE_TRUNC('month', {date_col}) AS month_start,
            SUM({count_col})::BIGINT       AS monthly_count
        FROM {source_table}
        WHERE {borough_col} IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    with duckdb.connect(warehouse_path, read_only=True) as con:
        df = con.sql(sql).df()

    df["t"] = df.groupby("borough").cumcount() + 1
    return df


# ---------------------------------------------------------------------------
# Trend fitting
# ---------------------------------------------------------------------------

def _design_matrix(t: np.ndarray, degree: int) -> np.ndarray:
    """Build OLS design matrix for a polynomial trend of given degree."""
    t = t.astype(float)
    if degree == 1:
        return np.column_stack([np.ones_like(t), t])
    if degree == 2:
        return np.column_stack([np.ones_like(t), t, t**2])
    raise ValueError(f"Unsupported trend degree: {degree}")


def _fit_ols_trend(y: np.ndarray, t: np.ndarray, degree: int):
    """Fit y on a polynomial trend in t via OLS."""
    X = _design_matrix(t, degree)
    return sm.OLS(y.astype(float), X).fit()


def _summarize_fit(result, model_type: str) -> ModelFit:
    """Convert a statsmodels result into a ModelFit summary."""
    resid = result.resid
    n = len(resid)

    # Ljung-Box white noise test on residuals.
    lags = min(LJUNG_BOX_LAGS, max(1, n // 5))
    try:
        lb = acorr_ljungbox(resid, lags=[lags], return_df=True)
        lb_pvalue = float(lb["lb_pvalue"].iloc[0])
    except Exception:
        lb_pvalue = float("nan")

    mse = float(np.mean(resid**2))
    return ModelFit(
        model_type=model_type,
        aic=float(result.aic),
        sic=float(result.bic),               # statsmodels labels SIC as "bic"
        mse=mse,
        rmse=float(np.sqrt(mse)),
        r2_adj=float(result.rsquared_adj),
        n_params=int(result.df_model + 1),   # +1 for the intercept
        residual_whitenoise_pvalue=lb_pvalue,
        residual_whitenoise_pass=(not np.isnan(lb_pvalue)) and (lb_pvalue > LJUNG_BOX_ALPHA),
    )


def _select_model(linear_fit: ModelFit, quadratic_fit: ModelFit) -> tuple[str, str]:
    """
    Choose between linear and quadratic via SIC. Consistent criterion per Diebold.
    Tiebreak inside +/- SIC_TIEBREAK_BAND prefers parsimony (linear).
    """
    delta = linear_fit.sic - quadratic_fit.sic
    if delta > SIC_TIEBREAK_BAND:
        return "quadratic", f"SIC favors quadratic by {delta:.2f} (strong evidence)"
    if delta < -SIC_TIEBREAK_BAND:
        return "linear", f"SIC favors linear by {-delta:.2f} (strong evidence)"
    return "linear", f"SIC delta {delta:.2f} within tiebreak band; prefer parsimony"


# ---------------------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------------------

def _forecast_horizon(
    y: np.ndarray,
    t: np.ndarray,
    degree: int,
    horizon: int,
    pi_level: float,
) -> pd.DataFrame:
    """Refit chosen model and produce h-step forecasts with prediction intervals."""
    result = _fit_ols_trend(y, t, degree)
    t_future = np.arange(t.max() + 1, t.max() + 1 + horizon)
    X_future = _design_matrix(t_future, degree)
    pred = result.get_prediction(X_future)
    summary = pred.summary_frame(alpha=1.0 - pi_level)
    return pd.DataFrame({
        "t_future": t_future,
        "point_forecast": summary["mean"].to_numpy(),
        "lower_pi": summary["obs_ci_lower"].to_numpy(),
        "upper_pi": summary["obs_ci_upper"].to_numpy(),
    })


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _ensure_target_table(con: duckdb.DuckDBPyConnection, target_table: str) -> None:
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            run_id           VARCHAR,
            borough          VARCHAR,
            forecast_month   DATE,
            point_forecast   DOUBLE,
            lower_pi         DOUBLE,
            upper_pi         DOUBLE,
            pi_level         DOUBLE,
            model_type       VARCHAR,
            aic              DOUBLE,
            sic              DOUBLE,
            rmse             DOUBLE,
            generated_at     TIMESTAMP
        )
    """)


def _ensure_ci_cd_log(con: duckdb.DuckDBPyConnection, log_table: str = "ci_cd_log") -> None:
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table} (
            run_id       VARCHAR,
            asset_name   VARCHAR,
            gate_name    VARCHAR,
            status       VARCHAR,
            message      VARCHAR,
            metrics      JSON,
            executed_at  TIMESTAMP,
            logged_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _write_forecast(
    con: duckdb.DuckDBPyConnection,
    target_table: str,
    run_id: str,
    borough: str,
    last_observed_month: pd.Timestamp,
    forecast_df: pd.DataFrame,
    fit: ModelFit,
    pi_level: float,
) -> None:
    """Append forecast rows for one borough."""
    months_ahead = (forecast_df["t_future"].to_numpy()
                    - forecast_df["t_future"].iloc[0] + 1)
    forecast_months = [last_observed_month + pd.DateOffset(months=int(m))
                       for m in months_ahead]

    out = pd.DataFrame({
        "run_id": run_id,
        "borough": borough,
        "forecast_month": forecast_months,
        "point_forecast": forecast_df["point_forecast"],
        "lower_pi": forecast_df["lower_pi"],
        "upper_pi": forecast_df["upper_pi"],
        "pi_level": pi_level,
        "model_type": fit.model_type,
        "aic": fit.aic,
        "sic": fit.sic,
        "rmse": fit.rmse,
        "generated_at": datetime.utcnow(),
    })

    con.register("forecast_df_tmp", out)
    con.sql(f"INSERT INTO {target_table} SELECT * FROM forecast_df_tmp")
    con.unregister("forecast_df_tmp")


def _log_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    asset_name: str,
    status: str,
    message: str,
    metrics: dict,
    log_table: str = "ci_cd_log",
) -> None:
    """Log a run summary to ci_cd_log so the Promotion Dashboard surfaces it."""
    con.execute(f"""
        INSERT INTO {log_table}
        (run_id, asset_name, gate_name, status, message, metrics, executed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id,
        asset_name,
        "trend_forecast",
        status,
        message,
        json.dumps(metrics, default=str),
        datetime.utcnow(),
    ])


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_trend_forecast(
    warehouse_path: str,
    source_table: str = DEFAULT_SOURCE,
    target_table: str = DEFAULT_TARGET,
    forecast_horizon: int = DEFAULT_HORIZON,
    pi_level: float = DEFAULT_PI_LEVEL,
    run_id: Optional[str] = None,
    date_col: str = "complaint_date",
    borough_col: str = "borough",
    count_col: str = "complaint_count",
    min_obs_required: int = MIN_OBS_DEFAULT,
) -> dict:
    """
    End-to-end trend forecast across all boroughs.

    Returns a metadata dict containing run_id, status, message, per-borough
    fit summaries, and timing. The same metadata is JSON-serialized into the
    ci_cd_log row so the Promotion Dashboard can render it.
    """
    if run_id is None:
        run_id = f"trend_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    start = time.time()

    df = load_monthly_complaints(
        warehouse_path,
        source_table,
        date_col=date_col,
        borough_col=borough_col,
        count_col=count_col,
    )
    if df.empty:
        raise RuntimeError(f"No data returned from {source_table}")

    borough_results: list[BoroughResult] = []

    with duckdb.connect(warehouse_path) as con:
        _ensure_target_table(con, target_table)
        _ensure_ci_cd_log(con)

        # Idempotent: clear prior rows for this run_id.
        con.execute(f"DELETE FROM {target_table} WHERE run_id = ?", [run_id])

        for borough, sub in df.groupby("borough"):
            if len(sub) < min_obs_required:
                continue

            y = sub["monthly_count"].to_numpy()
            t = sub["t"].to_numpy()
            last_month = pd.Timestamp(sub["month_start"].max())

            linear_result = _fit_ols_trend(y, t, degree=1)
            quadratic_result = _fit_ols_trend(y, t, degree=2)

            linear_fit = _summarize_fit(linear_result, "linear")
            quadratic_fit = _summarize_fit(quadratic_result, "quadratic")

            selected, reason = _select_model(linear_fit, quadratic_fit)
            chosen_fit = linear_fit if selected == "linear" else quadratic_fit
            chosen_degree = 1 if selected == "linear" else 2

            forecast_df = _forecast_horizon(y, t, chosen_degree,
                                            forecast_horizon, pi_level)

            _write_forecast(con, target_table, run_id, borough,
                            last_month, forecast_df, chosen_fit, pi_level)

            borough_results.append(BoroughResult(
                borough=borough,
                n_obs=len(sub),
                fit_linear=linear_fit,
                fit_quadratic=quadratic_fit,
                selected_model=selected,
                selection_reason=reason,
            ))

        duration = time.time() - start
        metrics = {
            "duration_seconds": round(duration, 3),
            "horizon_months": forecast_horizon,
            "pi_level": pi_level,
            "boroughs_processed": len(borough_results),
            "boroughs": [br.to_dict() for br in borough_results],
        }

        any_residual_fail = any(
            (br.selected_model == "linear"
             and not br.fit_linear.residual_whitenoise_pass)
            or (br.selected_model == "quadratic"
                and not br.fit_quadratic.residual_whitenoise_pass)
            for br in borough_results
        )

        if not borough_results:
            status = "fail"
            message = "No borough met min_obs_required threshold"
        elif any_residual_fail:
            status = "warn"
            message = ("One or more boroughs show non-white-noise residuals. "
                       "Trend alone is insufficient; consider adding seasonality "
                       "or moving to ARMA (Diebold Ch. 5-7).")
        else:
            status = "pass"
            message = f"Trend forecast complete for {len(borough_results)} boroughs"

        _log_run(con, run_id, target_table, status, message, metrics)

    return {
        "run_id": run_id,
        "status": status,
        "message": message,
        "target_table": target_table,
        "boroughs_processed": len(borough_results),
        "duration_seconds": round(duration, 3),
        "borough_results": [br.to_dict() for br in borough_results],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="NYC 311 trend forecast (Diebold Ch. 4 pattern)"
    )
    parser.add_argument("--warehouse", required=True,
                        help="Path to DuckDB warehouse file")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument("--pi-level", type=float, default=DEFAULT_PI_LEVEL)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--date-col", default="complaint_date")
    parser.add_argument("--borough-col", default="borough")
    parser.add_argument("--count-col", default="complaint_count")
    parser.add_argument("--min-obs", type=int, default=MIN_OBS_DEFAULT)
    args = parser.parse_args()

    result = run_trend_forecast(
        warehouse_path=args.warehouse,
        source_table=args.source,
        target_table=args.target,
        forecast_horizon=args.horizon,
        pi_level=args.pi_level,
        run_id=args.run_id,
        date_col=args.date_col,
        borough_col=args.borough_col,
        count_col=args.count_col,
        min_obs_required=args.min_obs,
    )
    print(json.dumps(result, indent=2, default=str))
