# src/warehouse/load.py

"""
Warehouse loading layer
=======================
Loads cleaned DataFrames into Postgres using pandas + SQLAlchemy.

Table strategy
--------------
  stg_customers       - cleaned customers
  stg_transactions    - cleaned transactions
  fct_transactions    - joined / enriched fact table
  dq_results          - quality check results (appended each run)

Each load is a full replace for staging tables and append-or-replace for the
fact table (configurable).  Row counts are logged for audit.
"""
# src/warehouse/load.py

"""Load cleaned DataFrames into warehouse tables."""
from __future__ import annotations

from typing import Literal

import pandas as pd
from sqlalchemy.engine import Engine

from src.utils.config import load_config
from src.utils.logger import get_logger
from src.warehouse.db import get_engine, ping

log = get_logger(__name__)
cfg = load_config()

_SCHEMA = cfg["database"]["schema"]
_BATCH_SIZE = cfg["database"]["batch_size"]


def _load_df(
    df: pd.DataFrame,
    table: str,
    engine: Engine,
    if_exists: Literal["replace", "append", "fail"] = "replace",
    schema: str = _SCHEMA,
) -> int:
    """Write a DataFrame to a warehouse table and return the loaded row count."""
    if df.empty:
        log.warning("Skipping empty DataFrame for table '%s'", table)
        return 0

    # Convert problematic types before writing to avoid SQLAlchemy errors (e.g. datetimetz)
    df = df.copy()
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=_BATCH_SIZE,
        method="multi",
    )
    log.info("Loaded  table=%s.%s  rows=%d  if_exists=%s", schema, table, len(df), if_exists)
    return len(df)


def load_customers(
    df: pd.DataFrame,
    engine: Engine | None = None,
) -> int:
    """Load customers into the staging table."""
    engine = engine or get_engine()
    return _load_df(df, "stg_customers", engine, if_exists="replace")


def load_transactions(
    df: pd.DataFrame,
    engine: Engine | None = None,
) -> int:
    """Load transactions into the staging table."""
    engine = engine or get_engine()
    return _load_df(df, "stg_transactions", engine, if_exists="replace")


def load_fact_transactions(
    df: pd.DataFrame,
    engine: Engine | None = None,
    if_exists: Literal["replace", "append"] = "replace",
) -> int:
    """Load fact transactions into the warehouse."""
    engine = engine or get_engine()
    return _load_df(df, "fct_transactions", engine, if_exists=if_exists)


def load_dq_results(
    df: pd.DataFrame,
    engine: Engine | None = None,
) -> int:
    """Append data quality results without overwriting history."""
    engine = engine or get_engine()
    return _load_df(df, "dq_results", engine, if_exists="append")


def load_holidays(
    df: pd.DataFrame,
    engine: Engine | None = None,
) -> int:
    """Load holidays into the dimension table."""
    engine = engine or get_engine()
    return _load_df(df, "dim_holidays", engine, if_exists="replace")


def run_all_loads(
    df_customers: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_fact: pd.DataFrame,
    df_holidays: pd.DataFrame | None = None,
    engine: Engine | None = None,
) -> dict[str, int]:
    """Load all warehouse tables and return row counts."""
    engine = engine or get_engine()

    if not ping(engine):
        raise ConnectionError("Cannot reach database — aborting load.")

    counts: dict[str, int] = {}
    counts["stg_customers"] = load_customers(df_customers, engine)
    counts["stg_transactions"] = load_transactions(df_transactions, engine)
    counts["fct_transactions"] = load_fact_transactions(df_fact, engine)
    if df_holidays is not None and not df_holidays.empty:
        counts["dim_holidays"] = load_holidays(df_holidays, engine)

    log.info("All loads complete: %s", counts)
    return counts