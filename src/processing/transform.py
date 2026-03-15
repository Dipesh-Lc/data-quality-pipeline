# src/processing/transform.py

"""
Transformation layer
====================
Input:  clean transactions + customers + holidays (from interim)
Output: analytics-ready fact table → data/processed/fct_transactions.csv

Steps
-----
1. Join transactions → customers (LEFT join; flag orphans)
2. Enrich with public holiday flag
3. Derive customer_tenure_days
4. Standardise country to ISO-2 for dashboard use
5. Compute transaction_week_number
6. Write processed output
"""
from __future__ import annotations

import pandas as pd

from src.utils.logger import get_logger
from src.utils.paths import INTERIM, PROCESSED

log = get_logger(__name__)

# Minimal country → ISO-2 mapping (extend as needed)
_COUNTRY_ISO2: dict[str, str] = {
    "United Kingdom": "GB",
    "Germany":        "DE",
    "France":         "FR",
    "Netherlands":    "NL",
    "Australia":      "AU",
    "Spain":          "ES",
    "Switzerland":    "CH",
    "Belgium":        "BE",
    "Portugal":       "PT",
    "Norway":         "NO",
    "Italy":          "IT",
    "Denmark":        "DK",
    "Sweden":         "SE",
    "Finland":        "FI",
    "Austria":        "AT",
    "United States":  "US",
    "Japan":          "JP",
    "Canada":         "CA",
    "Singapore":      "SG",
    "Brazil":         "BR",
    "Ireland":        "IE",
}


def _load_interim(filename: str) -> pd.DataFrame | None:
    path = INTERIM / filename
    if not path.exists():
        log.warning("Interim file not found: %s", path)
        return None
    df = pd.read_csv(path, low_memory=False)
    log.info("Loaded %s  rows=%d", filename, len(df))
    return df


def join_transactions_customers(
    df_tx: pd.DataFrame,
    df_cu: pd.DataFrame,
) -> pd.DataFrame:
    """LEFT join transactions onto customers; flag unmatched rows."""
    # Ensure key types match
    df_tx = df_tx.copy()
    df_cu = df_cu.copy()
    df_tx["CustomerID"] = df_tx["CustomerID"].astype("str").str.strip()
    df_cu["CustomerID"] = df_cu["CustomerID"].astype("str").str.strip()

    # Rename overlapping columns before join
    cu_cols = {
        "Country":    "Customer_Country",
        "SignupDate": "SignupDate",
        "Segment":    "CustomerSegment",
        "IsActive":   "CustomerIsActive",
    }
    df_cu = df_cu.rename(columns={k: v for k, v in cu_cols.items() if k in df_cu.columns})

    df = df_tx.merge(
        df_cu[["CustomerID"] + list(cu_cols.values())],
        on="CustomerID",
        how="left",
        suffixes=("", "_cust"),
    )

    orphans = df["CustomerSegment"].isna().sum()
    log.info(
        "Join complete  tx_rows=%d  orphan_transactions=%d (%.1f%%)",
        len(df), orphans, 100 * orphans / max(len(df), 1),
    )
    df["is_known_customer"] = df["CustomerSegment"].notna()
    return df


def enrich_holidays(
    df: pd.DataFrame,
    df_holidays: pd.DataFrame | None,
) -> pd.DataFrame:
    """Flag each transaction as occurring on a public holiday."""
    df = df.copy()
    df["is_holiday"]   = False
    df["holiday_name"] = None

    if df_holidays is None or df_holidays.empty:
        log.warning("No holiday data available — is_holiday will be False for all rows.")
        return df

    df_holidays = df_holidays.copy()
    df_holidays["date"] = pd.to_datetime(df_holidays["date"]).dt.normalize()

    # Map transaction country → ISO-2
    if "Country" in df.columns:
        df["_iso2"] = df["Country"].map(_COUNTRY_ISO2)
    else:
        df["_iso2"] = None

    df["_tx_date"] = pd.to_datetime(df["InvoiceDate"]).dt.normalize()

    # Merge on (date, country_code)
    holiday_lookup = df_holidays[["date", "country_code", "name"]].rename(
        columns={"name": "holiday_name_match"}
    )
    df = df.merge(
        holiday_lookup,
        left_on=["_tx_date", "_iso2"],
        right_on=["date", "country_code"],
        how="left",
        suffixes=("", "_h"),
    )
    df["is_holiday"]   = df["holiday_name_match"].notna()
    df["holiday_name"] = df["holiday_name_match"]
    df.drop(columns=["_iso2", "_tx_date", "holiday_name_match",
                     "date_h", "country_code"], errors="ignore", inplace=True)

    flagged = df["is_holiday"].sum()
    log.info("Holiday enrichment  flagged=%d transactions", flagged)
    return df


def derive_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add analytical derived columns."""
    df = df.copy()

    # Customer tenure
    if "SignupDate" in df.columns:
        df["SignupDate"]           = pd.to_datetime(df["SignupDate"], errors="coerce")
        df["InvoiceDate"]          = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        df["customer_tenure_days"] = (
            df["InvoiceDate"] - df["SignupDate"]
        ).dt.days.clip(lower=0)

    # Week number
    if "InvoiceDate" in df.columns:
        df["TxWeek"] = pd.to_datetime(df["InvoiceDate"]).dt.isocalendar().week.astype(int)

    # Country ISO
    if "Country" in df.columns:
        df["CountryISO2"] = df["Country"].map(_COUNTRY_ISO2)

    return df


def run_transform(
    df_tx: pd.DataFrame | None = None,
    df_cu: pd.DataFrame | None = None,
    df_holidays: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Full transformation pipeline. Loads from interim if DataFrames not passed."""
    if df_tx is None:
        df_tx = _load_interim("transactions_clean.csv")
    if df_cu is None:
        df_cu = _load_interim("customers_clean.csv")
    if df_holidays is None:
        df_holidays = _load_interim("holidays.csv") if (INTERIM / "holidays.csv").exists() else None

    if df_tx is None:
        raise RuntimeError("Cannot transform: clean transactions file not found.")

    df = join_transactions_customers(df_tx, df_cu if df_cu is not None else pd.DataFrame())
    df = enrich_holidays(df, df_holidays)
    df = derive_columns(df)

    # Save processed output
    PROCESSED.mkdir(parents=True, exist_ok=True)
    out = PROCESSED / "fct_transactions.csv"
    df.to_csv(out, index=False)
    log.info("Fact table saved  path=%s  rows=%d", out, len(df))
    return df
