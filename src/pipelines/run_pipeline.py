#src/pipelines/run_pipeline.py
"""
Pipeline runner
===============
Executes all stages in order:

  1. Ingestion
  2. Validation
  3. Cleaning
  4. Transformation
  5. Warehouse load   (skipped if DB unavailable, with warning)
  6. Quality checks
  7. Anomaly detection
  8. Reporting

Usage
-----
  python -m src.pipelines.run_pipeline
  python -m src.pipelines.run_pipeline --run-date 2023-01-15
  make run
"""
from __future__ import annotations

import argparse
import sys
import traceback
from datetime import date
from pathlib import Path

import pandas as pd

from src.ingestion.ingest import (
    fetch_holidays,
    ingest_customers,
    ingest_transactions,
    write_ingestion_manifest,
)
from src.monitoring.anomaly_detection import results_to_df as anomaly_to_df
from src.monitoring.anomaly_detection import run_anomaly_detection
from src.monitoring.quality_checks import results_to_df as dq_to_df
from src.monitoring.quality_checks import run_quality_checks
from src.monitoring.reporting import save_reports
from src.processing.clean_customers import clean_customers, save_clean_customers
from src.processing.clean_transactions import clean_transactions, save_clean_transactions
from src.processing.transform import run_transform
from src.utils.config import load_config
from src.utils.logger import get_logger
from src.utils.paths import INTERIM, ensure_dirs
from src.validation.schema_checks import (
    results_to_df as val_to_df,
    validate_customers,
    validate_transactions,
)

log = get_logger(__name__)
cfg = load_config()

#  Helpers
# ------------------------------------------------------------------------------

def _try_db_load(
    df_cu:   pd.DataFrame,
    df_tx:   pd.DataFrame,
    df_fact: pd.DataFrame,
    df_hol:  pd.DataFrame | None,
) -> dict[str, int]:
    """Attempt warehouse load; return empty dict if DB is not available."""
    try:
        from src.warehouse.db import ping, get_engine
        from src.warehouse.load import run_all_loads
        engine = get_engine()
        if not ping(engine):
            log.warning("Database not reachable — skipping warehouse load.")
            return {}
        return run_all_loads(df_cu, df_tx, df_fact, df_hol, engine)
    except Exception as exc:
        log.warning("Warehouse load skipped (%s: %s)", type(exc).__name__, exc)
        return {}


#  Main pipeline
# ------------------------------------------------------------------------------

def run_pipeline(
    run_date: str | None = None,
    fetch_api: bool = True,
    source_dir: str | None = None,
) -> int:
    """
    Execute the full pipeline.

    Parameters
    ----------
    run_date:   ISO date for this run (default: today).
    fetch_api:  Whether to call the Nager.Date holidays API.
    source_dir: Path to the directory containing ``transactions.csv`` and
                ``customers.csv``.  Defaults to ``data/samples/`` so the
                pipeline works out of the box.  Pass a real data directory
                to run against live files without editing source code.

    Returns
    -------
    0  - success
    1  - completed with warnings / non-critical failures
    2  - critical failure (pipeline aborted)
    """
    run_date = run_date or date.today().isoformat()
    ensure_dirs()

    log.info("=" * 60)
    log.info("Pipeline START  run_date=%s", run_date)
    log.info("=" * 60)

    exit_code = 0

    try:
        # Stage 1: Ingestion 
        log.info("Stage 1/7: INGESTION")
        src_path = Path(source_dir) if source_dir else None
        df_tx_raw, _ = ingest_transactions(run_date, source_dir=src_path)
        df_cu_raw, _ = ingest_customers(run_date, source_dir=src_path)

        df_hol = pd.DataFrame()
        if fetch_api:
            try:
                df_hol = fetch_holidays(run_date=run_date)
            except Exception as exc:
                log.warning("Holiday API fetch failed (%s) — continuing without holidays.", exc)

        write_ingestion_manifest(run_date, len(df_tx_raw), len(df_cu_raw), len(df_hol))

        ingestion_meta = {
            "transactions": len(df_tx_raw),
            "customers":    len(df_cu_raw),
            "holidays":     len(df_hol),
        }

        # Stage 2: Validation 
        log.info("Stage 2/7: VALIDATION")
        val_results  = validate_transactions(df_tx_raw)
        val_results += validate_customers(df_cu_raw)

        critical_val_failures = [r for r in val_results if not r.passed and r.severity == "error"]
        if critical_val_failures:
            log.error(
                "Critical validation failures detected (%d). Pipeline will continue with warnings.",
                len(critical_val_failures),
            )
            exit_code = max(exit_code, 1)

        # Stage 3: Cleaning 
        log.info("Stage 3/7: CLEANING")
        df_cu_clean, df_cu_rejected  = clean_customers(df_cu_raw)
        df_tx_clean, df_tx_rejected  = clean_transactions(df_tx_raw)

        save_clean_customers(df_cu_clean, df_cu_rejected)
        save_clean_transactions(df_tx_clean, df_tx_rejected)

        if not df_hol.empty:
            df_hol.to_csv(INTERIM / "holidays.csv", index=False)

        # Stage 4: Transformation 
        log.info("Stage 4/7: TRANSFORMATION")
        df_fact = run_transform(df_tx_clean, df_cu_clean, df_hol if not df_hol.empty else None)

        # Stage 5: Warehouse load 
        log.info("Stage 5/7: WAREHOUSE LOAD")
        load_counts = _try_db_load(df_cu_clean, df_tx_clean, df_fact, df_hol if not df_hol.empty else None)

        # Stage 6: Quality checks 
        log.info("Stage 6/7: QUALITY MONITORING")
        dq_results  = run_quality_checks(df_tx_clean, df_cu_clean, df_fact, run_date)
        dq_df       = dq_to_df(dq_results, run_date)

        if load_counts:
            try:
                from src.warehouse.load import load_dq_results
                from src.warehouse.db import get_engine
                load_dq_results(dq_df, get_engine())
            except Exception:
                pass

        dq_failures = sum(1 for r in dq_results if not r.passed)
        if dq_failures:
            exit_code = max(exit_code, 1)

        # Stage 7: Anomaly detection 
        log.info("Stage 7/7: ANOMALY DETECTION + REPORTING")
        ad_results = run_anomaly_detection(df_fact, run_date)
        triggered  = sum(1 for r in ad_results if r.triggered)
        if triggered:
            exit_code = max(exit_code, 1)

        # Reporting 
        report_paths = save_reports(
            run_date            = run_date,
            ingestion_meta      = ingestion_meta,
            validation_results  = val_results,
            quality_results     = dq_results,
            anomaly_results     = ad_results,
            load_counts         = load_counts,
        )

    except Exception as exc:
        log.error("Pipeline ABORTED with critical error: %s", exc)
        log.debug(traceback.format_exc())
        return 2

    status = {0: "SUCCESS", 1: "COMPLETED WITH WARNINGS", 2: "FAILED"}[exit_code]
    log.info("=" * 60)
    log.info("Pipeline DONE  status=%s  run_date=%s", status, run_date)
    log.info("=" * 60)
    return exit_code


#  CLI entry point
# ---------------------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Data Quality Pipeline")
    p.add_argument("--run-date",    default=None, help="ISO date for this run (default: today)")
    p.add_argument("--no-api",      action="store_true", help="Skip holiday API call")
    p.add_argument(
        "--source-dir",
        default=None,
        help=(
            "Directory containing transactions.csv and customers.csv "
            "(default: data/samples/). Use this to run against real data."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(run_pipeline(
        run_date=args.run_date,
        fetch_api=not args.no_api,
        source_dir=args.source_dir,
    ))
