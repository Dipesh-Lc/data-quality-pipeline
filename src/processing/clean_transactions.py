# src/processing/clean_transactions.py

"""
Transaction cleaning
====================
Input:  raw transactions DataFrame
Output: cleaned + rejected DataFrames → data/interim/

Cleaning rules
--------------
- Strip whitespace
- Parse InvoiceDate to datetime
- Coerce UnitPrice / Quantity to numeric; quarantine non-parseable
- Quarantine rows where UnitPrice < 0 (returns with negative price are kept; price=0 OK)
- Quarantine future-dated invoices
- Capitalise Description
- Normalise Status to lowercase
- Deduplicate on (InvoiceNo, StockCode)
- Derive LineTotal = Quantity x UnitPrice
"""
from __future__ import annotations

from datetime import timezone

import pandas as pd

from src.utils.logger import get_logger
from src.utils.paths import INTERIM

log = get_logger(__name__)

_STATUS_MAP = {
    "complete":   "completed",
    "done":       "completed",
    "cancel":     "cancelled",
    "void":       "cancelled",
    "refund":     "refunded",
    "return":     "refunded",
}


def clean_transactions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns
    -------
    (clean_df, rejected_df)
    """
    log.info("Cleaning transactions  raw_rows=%d", len(df))
    df = df.copy()

    reasons: list[pd.Series] = []  # boolean masks of bad rows

    # 1. Strip whitespace
    str_cols = df.select_dtypes(include=["object","string"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # 2. Parse InvoiceDate
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    reasons.append(df["InvoiceDate"].isna().rename("bad_date"))

    # 3. Coerce numeric columns
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["Quantity"]  = pd.to_numeric(df["Quantity"],  errors="coerce")
    reasons.append(df["UnitPrice"].isna().rename("bad_unit_price"))
    reasons.append(df["Quantity"].isna().rename("bad_quantity"))

    # 4. Quarantine negative UnitPrice (not the same as negative Quantity = return)
    reasons.append((df["UnitPrice"] < 0).rename("negative_price"))

    # 5. Quarantine future-dated invoices
    now = pd.Timestamp.now(tz=timezone.utc).tz_localize(None)
    reasons.append((df["InvoiceDate"] > now).rename("future_date"))

    # Build combined rejection mask
    reject_mask = pd.concat(reasons, axis=1).any(axis=1)
    rejected    = df[reject_mask].copy()
    rejected["_reject_reason"] = (
        pd.concat(reasons, axis=1)[reject_mask]
        .apply(lambda row: "|".join(row.index[row].tolist()), axis=1)
    )
    df = df[~reject_mask].copy()

    # 6. Capitalise Description
    if "Description" in df.columns:
        df["Description"] = df["Description"].str.title()

    # 7. Normalise Status
    if "Status" in df.columns:
        df["Status"] = (
            df["Status"].str.lower().str.strip()
            .replace(_STATUS_MAP)
        )

    # 8. Deduplicate on natural key
    before_dedup = len(df)
    df           = df.drop_duplicates(subset=["InvoiceNo", "StockCode"], keep="first")
    dupes_removed = before_dedup - len(df)
    if dupes_removed:
        log.warning("Removed %d duplicate (InvoiceNo, StockCode) rows", dupes_removed)

    # 9. Derive LineTotal
    df["LineTotal"] = (df["Quantity"] * df["UnitPrice"]).round(4)

    # 10. Extract date parts for analytics
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["TxYear"]   = df["InvoiceDate"].dt.year
    df["TxMonth"]  = df["InvoiceDate"].dt.month
    df["TxDay"]    = df["InvoiceDate"].dt.day
    df["TxDOW"]    = df["InvoiceDate"].dt.day_name()   # day of week

    log.info(
        "Transactions cleaned  clean=%d  rejected=%d  dupes_removed=%d",
        len(df), len(rejected), dupes_removed,
    )
    return df, rejected


def save_clean_transactions(df: pd.DataFrame, rejected: pd.DataFrame) -> None:
    out = INTERIM / "transactions_clean.csv"
    df.to_csv(out, index=False)
    log.info("Clean transactions saved  path=%s  rows=%d", out, len(df))

    if len(rejected):
        rej_out = INTERIM / "rejected_transactions.csv"
        rejected.to_csv(rej_out, index=False)
        log.warning("Rejected transactions  path=%s  rows=%d", rej_out, len(rejected))
