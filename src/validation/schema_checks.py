# src/validation/schema_checks.py

"""
Validation layer
================
Two levels:

1. Schema validation  - structural: are the right columns present and non-empty?
2. Content validation = semantic:   are values within expected ranges?

Functions return a list of ValidationResult dataclasses so callers can log,
store, or raise as appropriate.
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
class ValidationResult:
    check: str
    passed: bool
    severity: str
    message: str
    rows_affected: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"

    def __str__(self) -> str:
        tag = "✓" if self.passed else "✗"
        return f"[{self.status}] {tag} {self.check}: {self.message}"


#  Schema checks
# ---------------------------------------------------------------------------------------

def check_not_empty(df: pd.DataFrame, label: str) -> ValidationResult:
    """Check that a DataFrame is not empty."""
    passed = len(df) > 0
    return ValidationResult(
        check=f"{label}.not_empty",
        passed=passed,
        severity="error",
        message=f"DataFrame has {len(df)} rows.",
    )


def check_required_columns(
    df: pd.DataFrame,
    label: str,
    required: list[str] | None = None,
) -> ValidationResult:
    """Check that all required columns are present."""
    required = required or cfg["schemas"][label]["required_columns"]
    missing = [c for c in required if c not in df.columns]
    passed = len(missing) == 0
    return ValidationResult(
        check=f"{label}.required_columns",
        passed=passed,
        severity="error",
        message=f"Missing columns: {missing}" if missing else "All required columns present.",
        details={"missing": missing},
    )


def check_no_extra_columns(
    df: pd.DataFrame,
    label: str,
) -> ValidationResult:
    """Check for unexpected columns."""
    schema = cfg["schemas"].get(label, {})
    allowed = set(schema.get("required_columns", [])) | set(schema.get("optional_columns", []))
    extra = [c for c in df.columns if c not in allowed] if allowed else []
    return ValidationResult(
        check=f"{label}.no_extra_columns",
        passed=len(extra) == 0,
        severity="warning",
        message=f"Extra columns: {extra}" if extra else "No unexpected columns.",
        details={"extra": extra},
    )


#  Content checks
# ---------------------------------------------------------------------------------------

def check_primary_key_unique(
    df: pd.DataFrame,
    label: str,
    key_col: str,
) -> ValidationResult:
    """Check that a key column contains unique values."""
    dupes = df[key_col].duplicated().sum() if key_col in df.columns else 0
    passed = dupes == 0
    return ValidationResult(
        check=f"{label}.pk_unique.{key_col}",
        passed=passed,
        severity="error",
        message=f"{dupes} duplicate values in {key_col}.",
        rows_affected=int(dupes),
    )


def check_no_nulls(
    df: pd.DataFrame,
    label: str,
    columns: list[str],
) -> list[ValidationResult]:
    """Check that selected columns do not contain nulls."""
    results = []
    for col in columns:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        null_pct = null_count / max(len(df), 1)
        passed = null_count == 0
        results.append(
            ValidationResult(
                check=f"{label}.no_nulls.{col}",
                passed=passed,
                severity="error" if null_pct > 0.05 else "warning",
                message=f"{null_count} nulls ({null_pct:.1%}) in column {col}.",
                rows_affected=int(null_count),
                details={"null_pct": round(null_pct, 4)},
            )
        )
    return results


def check_positive_values(
    df: pd.DataFrame,
    label: str,
    col: str,
    allow_zero: bool = False,
) -> ValidationResult:
    """Check that a numeric column contains positive values."""
    if col not in df.columns:
        return ValidationResult(
            check=f"{label}.positive.{col}",
            passed=True,
            severity="info",
            message=f"Column {col} not present — skipped.",
        )

    series = pd.to_numeric(df[col], errors="coerce")
    bad = (series < 0).sum() if allow_zero else (series <= 0).sum()
    passed = int(bad) == 0
    return ValidationResult(
        check=f"{label}.positive.{col}",
        passed=passed,
        severity="warning",
        message=f"{bad} rows with {'negative' if allow_zero else 'non-positive'} {col}.",
        rows_affected=int(bad),
    )


def check_no_future_dates(
    df: pd.DataFrame,
    label: str,
    col: str,
) -> ValidationResult:
    """Check that a date column does not contain future values."""
    if col not in df.columns:
        return ValidationResult(
            check=f"{label}.no_future.{col}",
            passed=True,
            severity="info",
            message=f"Column {col} not present — skipped.",
        )

    now = pd.Timestamp.now(tz=timezone.utc).tz_localize(None)
    series = pd.to_datetime(df[col], errors="coerce")
    future = (series > now).sum()
    passed = int(future) == 0
    return ValidationResult(
        check=f"{label}.no_future.{col}",
        passed=passed,
        severity="error",
        message=f"{future} future-dated rows in {col}.",
        rows_affected=int(future),
    )


#  Convenience runners
# ---------------------------------------------------------------------------------------

def validate_transactions(df: pd.DataFrame) -> list[ValidationResult]:
    """Run validation checks for transactions data."""
    results = [
        check_not_empty(df, "transactions"),
        check_required_columns(df, "transactions"),
        check_no_extra_columns(df, "transactions"),
        check_primary_key_unique(df, "transactions", "InvoiceNo"),
        check_positive_values(df, "transactions", "UnitPrice", allow_zero=True),
        check_no_future_dates(df, "transactions", "InvoiceDate"),
    ]
    results += check_no_nulls(df, "transactions", ["InvoiceNo", "StockCode", "InvoiceDate"])
    _log_results(results, "transactions")
    return results


def validate_customers(df: pd.DataFrame) -> list[ValidationResult]:
    """Run validation checks for customers data."""
    results = [
        check_not_empty(df, "customers"),
        check_required_columns(df, "customers"),
        check_primary_key_unique(df, "customers", "CustomerID"),
    ]
    results += check_no_nulls(df, "customers", ["CustomerID", "Country"])
    _log_results(results, "customers")
    return results


def _log_results(results: list[ValidationResult], label: str) -> None:
    """Log validation summary and failures."""
    fails = [r for r in results if not r.passed]
    log.info("Validation  dataset=%s  checks=%d  failures=%d", label, len(results), len(fails))
    for r in fails:
        lvl = log.error if r.severity == "error" else log.warning
        lvl("  %s", r)


def results_to_df(results: list[ValidationResult], run_date: str = "") -> pd.DataFrame:
    """Convert validation results to a flat DataFrame."""
    return pd.DataFrame(
        [
            {
                "run_date": run_date,
                "check": r.check,
                "status": r.status,
                "severity": r.severity,
                "message": r.message,
                "rows_affected": r.rows_affected,
                "checked_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            }
            for r in results
        ]
    )