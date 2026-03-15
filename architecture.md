# Architecture

## Overview

The data quality pipeline is a layered Python application that ingests transactional data, cleans and enriches it, loads it into a Postgres warehouse, and continuously monitors data health.

```
Raw Sources
  ├─ transactions.csv    (UCI Online Retail II style)
  └─ customers.csv       (CRM export)
  └─ Nager.Date API      (public holidays)
       │
       ▼
┌─────────────┐
│  Ingestion  │  src/ingestion/ingest.py
│             │  Copies raw files into data/raw/<date>/
│             │  Calls holidays API
│             │  Writes manifest.json
└──────┬──────┘
       │  raw DataFrames
       ▼
┌─────────────┐
│ Validation  │  src/validation/schema_checks.py
│             │  Schema (columns present, types OK)
│             │  Content (nulls, PK uniqueness, date ranges)
│             │  Returns ValidationResult list
└──────┬──────┘
       │  validated DataFrames (pipeline continues even on warnings)
       ▼
┌─────────────┐
│  Cleaning   │  src/processing/clean_customers.py
│             │  src/processing/clean_transactions.py
│             │  Normalisation, deduplication, type coercion
│             │  Quarantines rejected rows → data/interim/rejected_*.csv
└──────┬──────┘
       │  clean DataFrames → data/interim/
       ▼
┌──────────────┐
│ Transforma-  │  src/processing/transform.py
│    tion      │  JOIN transactions ← customers
│              │  ENRICH with holiday flags
│              │  DERIVE tenure, week number, ISO country code
└──────┬───────┘
       │  analytics-ready DataFrame → data/processed/fct_transactions.csv
       ▼
┌─────────────┐
│  Warehouse  │  src/warehouse/db.py     (SQLAlchemy engine)
│    Load     │  src/warehouse/load.py   (pandas .to_sql)
│             │  stg_customers, stg_transactions
│             │  fct_transactions, dim_holidays
└──────┬──────┘
       │  tables populated
       ▼
┌─────────────────┐
│    Monitoring   │  src/monitoring/quality_checks.py
│  Quality checks │  Orphan FK check, null rates, row counts,
│                 │  PK duplicates, status values, sign consistency
└──────┬──────────┘
       │  QualityResult list → dq_results table
       ▼
┌─────────────────┐
│    Monitoring   │  src/monitoring/anomaly_detection.py
│ Anomaly detect. │  Z-score daily volume
│                 │  Outlier unit prices
│                 │  Cancellation rate spike
│                 │  Null rate spike
└──────┬──────────┘
       │  AnomalyResult list
       ▼
┌─────────────┐
│  Reporting  │  src/monitoring/reporting.py
│             │  reports/quality_report.md
│             │  reports/quality_report.html
└─────────────┘
```

## Component Breakdown

### `src/ingestion/`
Single responsibility: **fetch and persist raw data**. No cleaning, no transformation. Uses dated subdirectories (`data/raw/2026-03-14/`) to ensure reproducibility — raw data is never overwritten.

### `src/validation/`
Structural and semantic checks on *raw* data before any cleaning happens. Returns `ValidationResult` objects. Critical failures log an error but do not abort the pipeline; this is intentional because partial data is better than no data for most analytics workloads.

### `src/processing/`
Three modules with a clean separation of concerns:

| Module                | Responsibility |
|:----------------------|:---------------|
| `clean_customers.py`  | Normalise CRM data, quarantine missing IDs |
| `clean_transactions.py` | Type coercion, rejected-row isolation, derived fields |
| `transform.py`        | JOIN + enrichment + feature engineering |

### `src/warehouse/`
Thin SQLAlchemy wrapper. `db.py` owns the engine and connection helpers. `load.py` owns the table-loading logic. This separation means you can swap the DB layer without touching pipeline logic.

### `src/monitoring/`
Two distinct monitoring concepts:

- **Quality checks** (`quality_checks.py`) -- rule-based, deterministic. Run after load. Ask "is something *wrong*?"
- **Anomaly detection** (`anomaly_detection.py`) -- statistical, probabilistic. Ask "is something *unusual*?"

This distinction matters: a null in a required column is always wrong; an unusually high transaction volume might be a flash sale, not an error.

### `src/pipelines/run_pipeline.py`
Orchestrator only. No business logic lives here. Each stage returns data or results; the orchestrator passes them to the next stage. The DB load is wrapped in a try-except so the pipeline completes and generates a report even when Postgres is not available.

## Data Flow (files)

```
data/
  raw/<date>/
    transactions.csv      ← copy of source
    customers.csv         ← copy of source
    holidays.csv          ← API response
    manifest.json         ← ingestion metadata
  interim/
    transactions_clean.csv
    customers_clean.csv
    rejected_transactions.csv
    rejected_customers.csv
    holidays.csv
  processed/
    fct_transactions.csv  ← final analytics table
```

## Design Decisions

**Why LEFT JOIN for transactions → customers?**
Guest checkouts (no customer ID) are valid transactions and must not be silently dropped. They are flagged with `is_known_customer = False` for downstream filtering.

**Why quarantine rejected rows rather than drop them?**
Rejected rows in `data/interim/rejected_*.csv` preserve audit trail. The pipeline operator can inspect them, fix the source system, and re-run.

**Why does the pipeline not abort on validation failures?**
Real-world pipelines rarely receive perfect data. Aborting on the first validation failure would mean the analytics team gets *no* data on days when the upstream system has even minor issues — which is often worse than getting slightly imperfect data with a clear quality report attached.

The deliberate design choice here is *observable over brittle*: the pipeline continues, the cleaning layer resolves what it can, rejected rows are quarantined with their rejection reason, and the exit code is raised to `1` (warning) rather than `0` (success) so any orchestrator or CI system can still detect that attention is needed. Only a structural error that makes processing genuinely impossible — such as a completely missing required column — triggers exit code `2` and aborts early.

This mirrors how mature data platforms handle quality: SLAs are met by delivering data with documented caveats, not by refusing to deliver until data is perfect. The `dq_results` table and `quality_report.html` give stakeholders full visibility into exactly what was wrong and how it was handled.

**Why SQLAlchemy + pandas `.to_sql()` rather than raw COPY?**
Simplicity and portability. For high-volume production use, replace with `psycopg2.copy_expert` or a dedicated loader.

**Why Z-score anomaly detection rather than a model?**
Z-scores are interpretable, require no training data, and are explainable to non-technical stakeholders. They are a solid V1 baseline. A rolling-window variant handles seasonality to a reasonable degree.

