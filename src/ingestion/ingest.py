#src/ingestion/ingest.py
"""Ingestion helpers for copying source files and fetching holiday data."""
from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from src.utils.config import load_config
from src.utils.logger import get_logger
from src.utils.paths import SAMPLES, dated_raw_dir

log = get_logger(__name__)
cfg = load_config()


# File ingestion
# -------------------------------------------------------------

def ingest_csv(source_path: Path, dest_dir: Path, label: str) -> pd.DataFrame:
    """Copy a CSV to the raw directory and return it as a DataFrame."""
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    dest_path = dest_dir / source_path.name
    shutil.copy2(source_path, dest_path)

    df = pd.read_csv(dest_path, low_memory=False)
    log.info(
        "Ingested %s  rows=%d  cols=%d  dest=%s",
        label, len(df), len(df.columns), dest_path,
    )
    return df


def ingest_transactions(
    run_date: str | None = None,
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, Path]:
    """Ingest transactions data and return the DataFrame and raw file path."""
    run_date = run_date or date.today().isoformat()
    dest_dir = dated_raw_dir(run_date)

    src = (source_dir or SAMPLES) / cfg["sources"]["transactions"]["filename"]
    df  = ingest_csv(src, dest_dir, "transactions")
    return df, dest_dir / src.name


def ingest_customers(
    run_date: str | None = None,
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, Path]:
    """Ingest customers data and return the DataFrame and raw file path."""
    run_date = run_date or date.today().isoformat()
    dest_dir = dated_raw_dir(run_date)

    src = (source_dir or SAMPLES) / cfg["sources"]["customers"]["filename"]
    df  = ingest_csv(src, dest_dir, "customers")
    return df, dest_dir / src.name


# Holidays API
# -------------------------------------------------------------

def fetch_holidays(
    countries: list[str] | None = None,
    year: int | None = None,
    run_date: str | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    """Fetch public holidays and save them to the raw directory."""
    run_date  = run_date  or date.today().isoformat()
    api_cfg   = cfg["holidays_api"]
    countries = countries or api_cfg["countries"]
    year      = year or api_cfg.get("year") or int(run_date[:4])
    base_url  = api_cfg["base_url"]
    timeout   = timeout   or api_cfg["timeout_s"]

    records: list[dict] = []
    for cc in countries:
        url = f"{base_url}/PublicHolidays/{year}/{cc}"
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            for h in resp.json():
                records.append(
                    {
                        "country_code": cc,
                        "date":         h.get("date"),
                        "local_name":   h.get("localName"),
                        "name":         h.get("name"),
                        "is_global":    h.get("global", True),
                    }
                )
            log.info("Holidays fetched  country=%s  count=%d", cc, len(records))
        except requests.RequestException as exc:
            log.warning("Could not fetch holidays for %s: %s", cc, exc)

    df = pd.DataFrame(records)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])

    dest_dir = dated_raw_dir(run_date)
    out_path = dest_dir / "holidays.csv"
    df.to_csv(out_path, index=False)
    log.info("Holidays saved  rows=%d  path=%s", len(df), out_path)

    return df


# Ingestion metadata
# -------------------------------------------------------------

def write_ingestion_manifest(
    run_date: str,
    tx_rows: int,
    cu_rows: int,
    holiday_rows: int,
) -> None:
    """Write a JSON manifest for the ingestion run."""
    dest_dir = dated_raw_dir(run_date)
    manifest = {
        "run_date":     run_date,
        "ingested_at":  datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "transactions": tx_rows,
        "customers":    cu_rows,
        "holidays":     holiday_rows,
    }
    path = dest_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    log.info("Manifest written  path=%s", path)