#src/monitoring/quality_checks.py

"""
Data quality monitoring
=======================
Runs rule-based checks against *loaded* DataFrames.

Each check returns a QualityResult that can be:
  - logged
  - stored in dq_results table
  - included in reports

Checks included
---------------
- Duplicate primary keys in warehouse tables
- Orphan transactions (unknown CustomerID)
- Null rates in critical columns
- Row count expectations
- Unexpected Status values
- LineTotal sign consistency (negative quantity → negative line total)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger

log = get_logger(__name__)
cfg = load_config()


#  Result model
# ---------------------------------------------------------------------------------------

@dataclass
class QualityResult:
    check: str
    passed: bool
    severity: str
    message: str
    rows_affected: int = 0
    table: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    checked_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    )

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"

    def __str__(self) -> str:
        return f"[{self.status}] {self.check}: {self.message}"


#  Individual checks
# ---------------------------------------------------------------------------------------

def check_pk_duplicates(df: pd.DataFrame, table: str, key_col: str) -> QualityResult:
    """Check for duplicate values in a key column."""
    dupes = df[key_col].duplicated().sum() if key_col in df.columns else 0
    return QualityResult(
        check=f"pk_duplicates.{key_col}",
        table=table,
        passed=dupes == 0,
        severity="error",
        message=f"{dupes} duplicate values in {key_col}.",
        rows_affected=int(dupes),
    )


def check_orphan_transactions(
    df_tx: pd.DataFrame,
    df_cu: pd.DataFrame,
    tx_key: str = "CustomerID",
    cu_key: str = "CustomerID",
) -> QualityResult:
    """Check for transactions without a matching customer."""
    known = set(df_cu[cu_key].dropna().astype(str))
    tx_ids = df_tx[tx_key].dropna().astype(str)
    orphans = (~tx_ids.isin(known)).sum()
    return QualityResult(
        check="orphan_transactions",
        table="fct_transactions",
        passed=orphans == 0,
        severity="warning",
        message=f"{orphans} transactions have no matching customer.",
        rows_affected=int(orphans),
    )


def check_null_rates(
    df: pd.DataFrame,
    table: str,
    critical_cols: list[str],
    threshold: float | None = None,
) -> list[QualityResult]:
    """Check null rates for critical columns."""
    threshold = threshold or cfg["anomaly"]["null_rate_threshold"]
    results = []

    for col in critical_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        null_rate = null_count / max(len(df), 1)
        results.append(
            QualityResult(
                check=f"null_rate.{col}",
                table=table,
                passed=null_rate <= threshold,
                severity="error" if null_rate > 0.10 else "warning",
                message=f"Null rate in {col}: {null_rate:.2%} ({null_count} rows).",
                rows_affected=int(null_count),
                details={"null_rate": round(null_rate, 4), "threshold": threshold},
            )
        )
    return results


def check_row_count_expectation(
    df: pd.DataFrame,
    table: str,
    min_rows: int,
    max_rows: int | None = None,
) -> QualityResult:
    """Check whether row count is within the expected range."""
    n = len(df)
    passed = n >= min_rows and (max_rows is None or n <= max_rows)
    return QualityResult(
        check="row_count_expectation",
        table=table,
        passed=passed,
        severity="error",
        message=(
            f"Row count {n:,} is outside expected range "
            f"[{min_rows:,}, {max_rows or '∞'}]."
        )
        if not passed
        else f"Row count {n:,} within expected range.",
        rows_affected=0,
        details={"row_count": n, "min_rows": min_rows, "max_rows": max_rows},
    )


def check_valid_status(
    df: pd.DataFrame,
    table: str,
    col: str = "Status",
    valid_values: list[str] | None = None,
) -> QualityResult:
    """Check for unexpected status values."""
    valid_values = valid_values or ["completed", "cancelled", "refunded"]
    if col not in df.columns:
        return QualityResult(
            check=f"valid_status.{col}",
            table=table,
            passed=True,
            severity="info",
            message=f"Column {col} not present — skipped.",
        )

    invalid = (~df[col].isin(valid_values)).sum()
    return QualityResult(
        check=f"valid_status.{col}",
        table=table,
        passed=invalid == 0,
        severity="warning",
        message=f"{invalid} rows with unexpected Status value.",
        rows_affected=int(invalid),
        details={"valid_values": valid_values},
    )


def check_line_total_sign(df: pd.DataFrame, table: str) -> QualityResult:
    """Check that negative quantity does not have a positive line total."""
    if not {"Quantity", "LineTotal"}.issubset(df.columns):
        return QualityResult(
            check="line_total_sign",
            table=table,
            passed=True,
            severity="info",
            message="Quantity or LineTotal column missing — skipped.",
        )

    neg_qty = df["Quantity"] < 0
    pos_total = df["LineTotal"] > 0
    mismatches = (neg_qty & pos_total).sum()
    return QualityResult(
        check="line_total_sign",
        table=table,
        passed=mismatches == 0,
        severity="warning",
        message=f"{mismatches} rows where negative Quantity has positive LineTotal.",
        rows_affected=int(mismatches),
    )


#  Runner
# ---------------------------------------------------------------------------------------

def run_quality_checks(
    df_tx: pd.DataFrame,
    df_cu: pd.DataFrame,
    df_fact: pd.DataFrame,
    run_date: str = "",
) -> list[QualityResult]:
    """Run all quality checks and return the results."""
    results: list[QualityResult] = []

    results.append(check_pk_duplicates(df_cu, "stg_customers", "CustomerID"))
    results += check_null_rates(df_cu, "stg_customers", ["CustomerID", "Country"])
    results.append(check_row_count_expectation(df_cu, "stg_customers", min_rows=100))

    results.append(check_pk_duplicates(df_tx, "stg_transactions", "InvoiceNo"))
    if "StockCode" in df_tx.columns:
        compound_key = df_tx["InvoiceNo"].astype(str) + "|" + df_tx["StockCode"].astype(str)
        compound_dupes = compound_key.duplicated().sum()
        results.append(
            QualityResult(
                check="pk_duplicates.InvoiceNo_StockCode",
                table="stg_transactions",
                passed=compound_dupes == 0,
                severity="error",
                message=f"{compound_dupes} duplicate (InvoiceNo, StockCode) pairs.",
                rows_affected=int(compound_dupes),
            )
        )

    results += check_null_rates(
        df_tx,
        "stg_transactions",
        ["InvoiceNo", "InvoiceDate", "UnitPrice"],
    )
    results.append(check_valid_status(df_tx, "stg_transactions"))
    results.append(check_row_count_expectation(df_tx, "stg_transactions", min_rows=500))

    results.append(check_orphan_transactions(df_fact, df_cu))
    results.append(check_line_total_sign(df_fact, "fct_transactions"))

    fails = [r for r in results if not r.passed]
    log.info(
        "Quality checks complete  total=%d  passed=%d  failed=%d  run_date=%s",
        len(results),
        len(results) - len(fails),
        len(fails),
        run_date,
    )
    for r in fails:
        fn = log.error if r.severity == "error" else log.warning
        fn("  %s", r)

    return results


def results_to_df(results: list[QualityResult], run_date: str = "") -> pd.DataFrame:
    """Convert quality check results to a DataFrame."""
    return pd.DataFrame(
        [
            {
                "run_date": run_date,
                "table": r.table,
                "check": r.check,
                "status": r.status,
                "severity": r.severity,
                "message": r.message,
                "rows_affected": r.rows_affected,
                "checked_at": r.checked_at,
            }
            for r in results
        ]
    )
