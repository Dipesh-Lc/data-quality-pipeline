#src/processing/clean_customers.py

"""
Customer cleaning
=================
Input:  raw customers DataFrame
Output: cleaned DataFrame → data/interim/customers_clean.csv

Rules applied
-------------
- Strip leading/trailing whitespace from all string columns
- Normalise Country to title-case
- Parse SignupDate to datetime
- Drop fully-duplicate rows; flag partial duplicates on CustomerID
- Normalise Segment values to lowercase
- Cast IsActive to bool where possible
"""
from __future__ import annotations

import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger
from src.utils.paths import INTERIM

log = get_logger(__name__)
cfg = load_config()

_COUNTRY_MAP: dict[str, str] = {
    # Abbreviations that survive .title() unchanged
    "Uk":             "United Kingdom",
    "Usa":            "United States",
    "Us":             "United States",
    "Eire":           "Ireland",
}


def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean raw customers data.

    Returns
    -------
    (clean_df, rejected_df)
        clean_df    - records that passed all cleaning rules
        rejected_df - records quarantined due to missing CustomerID
    """
    log.info("Cleaning customers  raw_rows=%d", len(df))
    df = df.copy()

    # 1. Strip whitespace from strings
    str_cols = df.select_dtypes(include=["object","string"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # 2. Normalise Country
    if "Country" in df.columns:
        df["Country"] = (
            df["Country"]
            .str.strip()
            .str.title()
            .replace(_COUNTRY_MAP)
        )

    # 3. Parse SignupDate
    if "SignupDate" in df.columns:
        df["SignupDate"] = pd.to_datetime(df["SignupDate"], errors="coerce")

    # 4. Normalise Segment to lowercase
    if "Segment" in df.columns:
        df["Segment"] = df["Segment"].str.lower().str.strip()

    # 5. Normalise IsActive
    if "IsActive" in df.columns:
        df["IsActive"] = df["IsActive"].map(
            {True: True, False: False, "true": True, "false": False,
             "1": True, "0": False, 1: True, 0: False}
        )

    # 6. Quarantine rows without CustomerID
    rejected = df[df["CustomerID"].isna()].copy()
    df       = df[df["CustomerID"].notna()].copy()

    # 7. Remove fully-duplicate rows (keep first)
    before_dedup = len(df)
    df           = df.drop_duplicates(subset=["CustomerID"], keep="first")
    removed_dupes = before_dedup - len(df)
    if removed_dupes:
        log.warning("Removed %d duplicate CustomerID rows", removed_dupes)

    log.info(
        "Customers cleaned  clean=%d  rejected=%d  dupes_removed=%d",
        len(df), len(rejected), removed_dupes,
    )
    return df, rejected


def save_clean_customers(df: pd.DataFrame, rejected: pd.DataFrame) -> None:
    out = INTERIM / "customers_clean.csv"
    df.to_csv(out, index=False)
    log.info("Clean customers saved  path=%s  rows=%d", out, len(df))

    if len(rejected):
        rej_out = INTERIM / "rejected_customers.csv"
        rejected.to_csv(rej_out, index=False)
        log.warning("Rejected customers saved  path=%s  rows=%d", rej_out, len(rejected))
