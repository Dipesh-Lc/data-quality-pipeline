#src/monitoring/anomaly_detection.py
"""Utilities for detecting statistical anomalies in transaction data."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger

log = get_logger(__name__)
cfg = load_config()
_AD = cfg["anomaly"]


#  Result model
# ---------------------------------------------------------------------------------------

@dataclass
class AnomalyResult:
    detector: str
    triggered: bool
    severity: str
    message: str
    metric_value: float = 0.0
    threshold: float = 0.0
    details: dict = field(default_factory=dict)
    detected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    )

    @property
    def label(self) -> str:
        return "ANOMALY" if self.triggered else "NORMAL"

    def __str__(self) -> str:
        return f"[{self.label}] {self.detector}: {self.message}"


#  Helpers
# ---------------------------------------------------------------------------------------

def _zscore(series: pd.Series) -> pd.Series:
    """Return z-scores for a numeric series."""
    mu = series.mean()
    std = series.std(ddof=1)
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - mu) / std


#  Detectors
# ---------------------------------------------------------------------------------------

def detect_daily_volume_anomaly(
    df: pd.DataFrame,
    date_col: str = "InvoiceDate",
    z_threshold: float | None = None,
    rolling_window: int | None = None,
) -> list[AnomalyResult]:
    """Flag dates where daily transaction count is unusually high or low."""
    z_threshold = z_threshold or _AD["daily_volume"]["z_score_threshold"]
    rolling_window = rolling_window or _AD["daily_volume"]["rolling_window"]

    results: list[AnomalyResult] = []

    if date_col not in df.columns:
        return results

    daily = (
        df.assign(_date=pd.to_datetime(df[date_col]).dt.normalize())
        .groupby("_date")
        .size()
        .rename("tx_count")
        .sort_index()
    )

    if len(daily) < 3:
        log.warning("Not enough daily data points for volume anomaly detection.")
        return results

    rolling_mean = daily.rolling(window=rolling_window, min_periods=1).mean()
    rolling_std = daily.rolling(window=rolling_window, min_periods=1).std(ddof=1).fillna(1)
    z_scores = (daily - rolling_mean) / rolling_std.replace(0, 1)

    anomalous_dates = z_scores[z_scores.abs() > z_threshold]
    for date, z in anomalous_dates.items():
        severity = "critical" if abs(z) > z_threshold * 1.5 else "high"
        results.append(
            AnomalyResult(
                detector="daily_volume_zscore",
                triggered=True,
                severity=severity,
                message=(
                    f"Date {date.date() if hasattr(date, 'date') else date}: "
                    f"volume={int(daily[date])}  z={z:.2f}"
                ),
                metric_value=float(daily[date]),
                threshold=float(z_threshold),
                details={
                    "date": str(date),
                    "z_score": round(float(z), 3),
                    "tx_count": int(daily[date]),
                    "rolling_mean": round(float(rolling_mean[date]), 1),
                },
            )
        )

    if not results:
        results.append(
            AnomalyResult(
                detector="daily_volume_zscore",
                triggered=False,
                severity="low",
                message="Daily transaction volume within normal range.",
                threshold=float(z_threshold),
            )
        )

    log.info(
        "Daily volume anomalies detected=%d  threshold=%.1f",
        len([r for r in results if r.triggered]),
        z_threshold,
    )
    return results


def detect_amount_anomaly(
    df: pd.DataFrame,
    amount_col: str = "UnitPrice",
    z_threshold: float | None = None,
) -> list[AnomalyResult]:
    """Flag transactions with unusually high or low values."""
    z_threshold = z_threshold or _AD["transaction_amount"]["z_score_threshold"]

    results: list[AnomalyResult] = []
    if amount_col not in df.columns:
        return results

    series = pd.to_numeric(df[amount_col], errors="coerce").dropna()
    z_scores = _zscore(series)
    outliers = z_scores[z_scores.abs() > z_threshold]

    n_outliers = len(outliers)
    results.append(
        AnomalyResult(
            detector=f"amount_zscore.{amount_col}",
            triggered=n_outliers > 0,
            severity="high" if n_outliers > 50 else "medium",
            message=(
                f"{n_outliers} rows with |z| > {z_threshold} in {amount_col}."
                if n_outliers
                else f"No amount anomalies in {amount_col}."
            ),
            metric_value=float(n_outliers),
            threshold=float(z_threshold),
            details={
                "outlier_count": n_outliers,
                "mean": round(float(series.mean()), 4),
                "std": round(float(series.std()), 4),
                "max_value": round(float(series.max()), 4),
            },
        )
    )
    log.info("Amount anomalies  column=%s  outliers=%d", amount_col, n_outliers)
    return results


def detect_cancellation_spike(
    df: pd.DataFrame,
    status_col: str = "Status",
    cancel_value: str = "cancelled",
    threshold_pct: float = 0.20,
) -> AnomalyResult:
    """Flag when cancellation rate exceeds the threshold."""
    if status_col not in df.columns:
        return AnomalyResult(
            detector="cancellation_spike",
            triggered=False,
            severity="low",
            message=f"Column {status_col} not found — skipped.",
        )

    total = max(len(df), 1)
    cancelled = (df[status_col] == cancel_value).sum()
    rate = cancelled / total

    triggered = rate > threshold_pct
    log.info("Cancellation rate=%.2f%%  threshold=%.0f%%", rate * 100, threshold_pct * 100)
    return AnomalyResult(
        detector="cancellation_spike",
        triggered=triggered,
        severity="high" if triggered else "low",
        message=f"Cancellation rate: {rate:.1%} (threshold: {threshold_pct:.0%}).",
        metric_value=round(rate, 4),
        threshold=threshold_pct,
        details={"cancelled": int(cancelled), "total": total},
    )


def detect_null_rate_spike(
    df: pd.DataFrame,
    critical_cols: list[str],
    threshold: float | None = None,
) -> list[AnomalyResult]:
    """Flag critical columns whose null rate exceeds the threshold."""
    threshold = threshold or _AD["null_rate_threshold"]
    results = []

    for col in critical_cols:
        if col not in df.columns:
            continue
        null_rate = df[col].isna().mean()
        triggered = null_rate > threshold
        results.append(
            AnomalyResult(
                detector=f"null_rate_spike.{col}",
                triggered=triggered,
                severity="high" if triggered else "low",
                message=f"Null rate in {col}: {null_rate:.2%}.",
                metric_value=round(null_rate, 4),
                threshold=threshold,
            )
        )
    return results


#  Runner
# ---------------------------------------------------------------------------------------

def run_anomaly_detection(
    df_fact: pd.DataFrame,
    run_date: str = "",
) -> list[AnomalyResult]:
    """Run all anomaly detectors and return the combined results."""
    results: list[AnomalyResult] = []

    results += detect_daily_volume_anomaly(df_fact)
    results += detect_amount_anomaly(df_fact, "UnitPrice")
    results.append(detect_cancellation_spike(df_fact))
    results += detect_null_rate_spike(df_fact, ["CustomerID", "InvoiceDate", "UnitPrice"])

    triggered = [r for r in results if r.triggered]
    log.info(
        "Anomaly detection complete  total=%d  triggered=%d  run_date=%s",
        len(results),
        len(triggered),
        run_date,
    )
    return results


def results_to_df(results: list[AnomalyResult], run_date: str = "") -> pd.DataFrame:
    """Convert anomaly results to a DataFrame."""
    return pd.DataFrame(
        [
            {
                "run_date": run_date,
                "detector": r.detector,
                "label": r.label,
                "triggered": r.triggered,
                "severity": r.severity,
                "message": r.message,
                "metric_value": r.metric_value,
                "threshold": r.threshold,
                "detected_at": r.detected_at,
            }
            for r in results
        ]
    )