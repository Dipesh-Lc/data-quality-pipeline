# Data Quality Pipeline

A production-style ETL pipeline with automated data quality monitoring, anomaly detection, and a self-updating HTML dashboard -- built around UCI Online Retail II–style transaction data.

---

## Business Problem

An analytics team relies on daily transaction and customer exports to power reporting dashboards and downstream ML pipelines. But incoming files are inconsistent, so duplicate records, missing values, future-dated rows, and casing variations are routine. Without a cleaning and monitoring layer, bad data propagates silently into production reports, and nobody notices until a stakeholder spots a wrong number.

This pipeline solves that. It standardises inputs, quarantines bad records with a full audit trail, and automatically flags quality issues and statistical anomalies before data reaches any downstream system.

A company receives daily transaction and customer exports from separate operational systems. Before these can be trusted for analytics, reporting, or ML, they need to be:

- **Validated** -- do the files contain the expected columns, types, and value ranges?
- **Cleaned** -- are duplicates, bad prices, future-dated rows, and casing inconsistencies resolved?
- **Joined** -- can transactions be linked back to customers?
- **Monitored** -- are today's numbers within the normal range, or did something break upstream?

This pipeline automates all of that in a single run of the script, then generates a fully self-contained HTML dashboard and report that reflect the actual run, no manual edits required.

---

## Architecture

```
Raw Sources
  ├─ transactions.csv   (UCI Online Retail II style)
  ├─ customers.csv      (CRM export)
  └─ Nager.Date API     (public holidays — optional enrichment)
       │
       ▼
 Ingestion → Validation → Cleaning → Transformation
                                          │
                                          ▼
                              (Optional) Postgres Warehouse
                     ┌────────────────────────────────┐
                     │  stg_customers                 │
                     │  stg_transactions              │
                     │  fct_transactions  (fact table)│
                     │  dim_holidays                  │
                     │  dq_results        (audit log) │
                     └────────────────────────────────┘
                                          │
                              Quality Checks + Anomaly Detection
                                          │
                                          ▼
                              reports/quality_report.html
                              reports/dashboard.html
```

Full component details -> [`docs/architecture.md`](docs/architecture.md)

---

## Pipeline Stages

| # | Stage | Module | Output |
|:-:|:------|:-------|:-------|
| 1 | **Ingestion** | `src/ingestion/ingest.py` | `data/raw/<date>/` + manifest |
| 2 | **Validation** | `src/validation/schema_checks.py` | `ValidationResult` list |
| 3 | **Cleaning** | `src/processing/clean_*.py` | `data/interim/` |
| 4 | **Transformation** | `src/processing/transform.py` | `data/processed/fct_transactions.csv` |
| 5 | **Warehouse load** | `src/warehouse/load.py` | Postgres tables (skipped gracefully if no DB) |
| 6 | **Quality checks** | `src/monitoring/quality_checks.py` | `dq_results` table / in-memory |
| 7 | **Anomaly detection** | `src/monitoring/anomaly_detection.py` | `AnomalyResult` list |
| 8 | **Reporting** | `src/monitoring/reporting.py` | `reports/quality_report.md` + `.html` |

After running the pipeline, two additional scripts generate the visual dashboard:

```
python reports/generate_figures.py   # produces 10 PNG charts in reports/figures/
python reports/build_dashboard.py    # assembles dashboard.html from live data
```

`build_dashboard.py` reads all its data directly from the pipeline output files — every KPI, table row, check result, and anomaly status is computed at runtime. Re-running it after a new pipeline run will always reflect the latest data.

---

## Datasets

### Primary: UCI Online Retail II (simulated)

`data/samples/generate_samples.py` generates realistic CSV data with intentional quality issues injected:

- ~1% rows with null `UnitPrice`
- ~0.5% duplicate invoice rows
- ~0.3% future-dated invoices
- ~2% customers with inconsistent `Country` casing
- ~1% customers with null `SignupDate`

### Enrichment: Nager.Date Public Holidays API

```
GET https://date.nager.at/api/v3/PublicHolidays/{year}/{countryCode}
```

No API key required. Adds `is_holiday` and `holiday_name` columns to the fact table. The pipeline continues without error if the API is unreachable.

---

## Quality Checks

### Validation (pre-cleaning, on raw data)

| Check | Severity |
|:------|:--------:|
| DataFrame not empty | error |
| Required columns present | error |
| Primary key unique | error |
| No nulls in required columns | error / warning |
| UnitPrice ≥ 0 | warning |
| InvoiceDate not in the future | error |

### DQ Checks (post-load, on cleaned / warehouse data)

| Check | Severity |
|:------|:--------:|
| PK duplicates in staging tables | error |
| Compound key (InvoiceNo + StockCode) unique | error |
| Orphan transactions (no matching customer) | warning |
| Null rate in critical columns | error / warning |
| Row count within expected range | error |
| Valid Status values | warning |
| LineTotal sign consistency | warning |

---

## Anomaly Detection

| Detector | Method | Trigger |
|:---------|:-------|:--------|
| Daily transaction volume | Z-score, 7-day rolling window | \|z\| > 3 |
| Unit price outliers | Z-score across all rows | \|z\| > 4 |
| Cancellation rate spike | Threshold | rate > 20% |
| Null rate spike | Threshold | null rate > 5% in critical column |

---

## Project Structure

```
data-quality-pipeline/
├── configs/
│   ├── config.yaml          # All thresholds, paths, schemas, API settings
│   └── logging.yaml         # Rotating file + console handlers
├── data/
│   ├── raw/                 # Immutable raw copies (dated subdirs) + manifest.json
│   ├── interim/             # Cleaned data + rejected rows
│   ├── processed/           # Analytics-ready fact table (fct_transactions.csv)
│   └── samples/             # Sample data generator
├── docs/
│   ├── architecture.md      # Component design + data flow
│   └── pipeline_steps.md    # Data contracts + business rules
├── notebooks/
│   ├── 01_data_understanding.ipynb
│   └── 02_quality_analysis.ipynb
├── reports/
│   ├── build_dashboard.py   # Assembles dashboard.html from live pipeline outputs
│   ├── generate_figures.py  # Produces 10 chart PNGs from fct_transactions.csv
│   ├── figures/             # Generated chart images (01_*.png … 10_*.png)
│   ├── dashboard.html       # Self-contained interactive dashboard
│   ├── quality_report.md    # Markdown pipeline run summary
│   └── quality_report.html  # Styled HTML pipeline run report
├── sql/
│   ├── schema.sql           # Table DDL for the optional Postgres warehouse
│   ├── staging.sql          # Staging layer queries
│   └── quality_checks.sql   # Ad-hoc DQ SQL queries
├── src/
│   ├── ingestion/           # Fetch + persist raw data, write manifest
│   ├── validation/          # Schema + content checks on raw data
│   ├── processing/          # Cleaning (transactions, customers) + transformation
│   ├── warehouse/           # DB connection + loading (optional)
│   ├── monitoring/          # Quality checks, anomaly detection, reporting
│   ├── pipelines/           # Orchestrator (run_pipeline.py)
│   └── utils/               # Logger, config loader, path constants
├── tests/                   # unittest suite
├── artifacts/logs/          # Rotating pipeline logs
├── environment.yml          # Conda environment
└── pyproject.toml           # pip dependencies + tool config

```

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/your-username/data-quality-pipeline.git
cd data-quality-pipeline
pip install -r requirements.txt  
#OR 
conda env create -f environment.yml;  conda activate dq-pipeline

# 2. Generate sample data
python data/samples/generate_samples.py

# 3. Run the full pipeline
# (Postgres load is skipped automatically if no DB is configured)
python -m src.pipelines.run_pipeline

# 4. Generate charts and dashboard 
python reports/generate_figures.py
python reports/build_dashboard.py

# 5. Open the reports
# Windows:
Start-Process reports\dashboard.html

#Mac/Linux
open reports/dashboard.html

```

To run against your own data instead of the generated samples:

```bash
python -m src.pipelines.run_pipeline -- --source-dir /path/to/your/data
```

---

## Configuration

All pipeline behaviour is controlled by `configs/config.yaml`.

```yaml
anomaly:
  daily_volume:
    z_score_threshold: 3.0    # adjust sensitivity
    rolling_window:    7      # days of history
  transaction_amount:
    z_score_threshold: 4.0
  null_rate_threshold: 0.05   # flag if > 5% nulls in critical column

holidays_api:
  countries: [GB, DE, FR, US]
  year: 2023                  # fallback; overridden automatically by run_date
```

### Optional: Postgres warehouse

Create a `.env` file from the provided example and set your database connection:

```bash
cp .env.example .env
# Option A — single URL (recommended):
# DB_URL=postgresql://user:pass@localhost:5432/dqpipeline
#
# Option B — individual parts:
# DB_HOST=localhost  DB_PORT=5432  DB_NAME=dqpipeline
# DB_USER=pipeline_user  DB_PASSWORD=pipeline_pass
```

If no database is configured the pipeline skips the warehouse load gracefully and still produces all reports.

---

## Running Tests

```bash
python -m unittest discover -s tests -p "test_*.py" -v          # full unittest suite
python -m unittest discover -s tests -p "test_*.py"    # quiet mode (no verbose output)
```

---

## Dashboard

![Dashboard Overview](docs/images/dashboard_overview.png)

The dashboard [`dashboard.html`](https://Dipesh-Lc.github.io/data-quality-pipeline/dashboard.html) is a fully self-contained single-file page with four tabs:

- **Overview** -- KPI cards (total transactions, revenue, unique customers, cancellation rate, rejected rows, etc.) plus three key charts
- **Charts** -- all 10 generated figures
- **Quality & Anomalies** -- live validation check results, DQ check results, and anomaly detector output, all computed from the latest pipeline run
- **Pipeline** -- stage-by-stage run statistics and a data flow file table with actual row counts

Because `build_dashboard.py` reads directly from the pipeline output files, running it after any pipeline run always produces a fully up-to-date dashboard.

---

## Sample Output

After a full run the pipeline produces a Markdown report, an HTML report, and a self-contained dashboard. Here is a representative excerpt from `quality_report.md`:

```
# Data Quality Pipeline 
Run date: 2026-03-14  |  Generated: 2026-03-14 20:52 UTC

## 1. Ingestion Summary
| Source       | Rows loaded |
|:-------------|------------:|
| transactions |       5,025 |
| customers    |       3,351 |
| holidays     |          34 |

## 3. Validation Checks  (14 checks — 4 failures)
| Check                             | Status  | Severity | Rows |
|:----------------------------------|:-------:|:--------:|-----:|
| transactions.pk_unique.InvoiceNo  | ❌ FAIL | error    |   94 |
| transactions.positive.UnitPrice   | ❌ FAIL | warning  |   15 |
| transactions.no_future.InvoiceDate| ❌ FAIL | error    |   18 |
| customers.pk_unique.CustomerID    | ❌ FAIL | error    |   20 |
| … all other checks …              | ✅ PASS |          |    0 |

## 4. Data Quality Checks  (13 checks -- 0 failures after cleaning)
All checks PASS on cleaned data.

## 5. Anomaly Detection  (6 detectors -- 0 triggered)
No anomalies detected.

## Overall Status
⚠️  PIPELINE RUN NEEDS ATTENTION
    4 validation failures detected -- all resolved by cleaning layer
```
![Quality Checks](docs/images/dashboard_quality.png)
The dashboard (`reports/dashboard.html`) presents the same information visually across four tabs -- Overview KPIs, Charts, Quality & Anomalies, and Pipeline run statistics, and is fully regenerated from live data every time Pipeline is run.
![Anomaly](docs/images/dashboard_quality_anomaly.png)



Pipeline log output (`artifacts/logs/pipeline.log`):

```
2026-03-14 20:52:05 | INFO  | run_pipeline | Pipeline START  run_date=2026-03-14
2026-03-14 20:52:05 | INFO  | run_pipeline | Stage 1/7: INGESTION
2026-03-14 20:52:05 | INFO  | ingest       | Ingested transactions  rows=5,025  cols=10
2026-03-14 20:52:05 | INFO  | ingest       | Ingested customers     rows=3,351  cols=5
2026-03-14 20:52:05 | INFO  | run_pipeline | Stage 2/7: VALIDATION
2026-03-14 20:52:05 | ERROR | schema_checks| [FAIL] transactions.pk_unique.InvoiceNo: 94 duplicate values
2026-03-14 20:52:05 | INFO  | run_pipeline | Stage 3/7: CLEANING
2026-03-14 20:52:06 | INFO  | clean_tx     | Transactions cleaned  clean=4,947  rejected=49  dupes_removed=29
2026-03-14 20:52:06 | INFO  | run_pipeline | Stage 4/7: TRANSFORMATION
2026-03-14 20:52:06 | INFO  | transform    | Fact table saved  rows=4,947  cols=25
2026-03-14 20:52:06 | INFO  | run_pipeline | Stage 5/7: WAREHOUSE LOAD
2026-03-14 20:52:06 | WARN  | run_pipeline | Warehouse load skipped (no DB configured)
2026-03-14 20:52:06 | INFO  | run_pipeline | Stage 6/7: QUALITY MONITORING
2026-03-14 20:52:06 | INFO  | quality      | Quality checks complete  total=13  passed=13  failed=0
2026-03-14 20:52:06 | INFO  | run_pipeline | Stage 7/7: ANOMALY DETECTION + REPORTING
2026-03-14 20:52:06 | INFO  | anomaly      | Anomaly detection complete  total=6  triggered=0
2026-03-14 20:52:06 | INFO  | run_pipeline | Pipeline DONE  status=COMPLETED WITH WARNINGS
```

---

## Future Improvements

- GitHub Actions CI (lint + test on push) 
- Docker + docker-compose (app + Postgres) 

---

## License

MIT - see [`LICENSE`](LICENSE).
