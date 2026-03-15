# Pipeline Steps & Data Contracts

## Data Contracts

### Source: `transactions.csv`

| Column         | Type     | Required | Notes |
|:---------------|:---------|:--------:|:------|
| `InvoiceNo`    | string   | ✅       | May be prefixed with `C` for cancellations |
| `StockCode`    | string   | ✅       | Product identifier |
| `Description`  | string   |          | Product description |
| `Quantity`     | integer  | ✅       | Negative = return |
| `InvoiceDate`  | datetime | ✅       | Format: `YYYY-MM-DD HH:MM:SS` |
| `UnitPrice`    | float    | ✅       | Must be ≥ 0 |
| `CustomerID`   | string   |          | Null = guest checkout |
| `Country`      | string   | ✅       | Country of customer |
| `Status`       | string   |          | One of: `completed`, `cancelled`, `refunded` |
| `PaymentMethod`| string   |          | One of: `card`, `paypal`, `bank_transfer`, `voucher` |

### Source: `customers.csv`

| Column       | Type    | Required | Notes |
|:-------------|:--------|:--------:|:------|
| `CustomerID` | string  | ✅       | Must be globally unique |
| `Country`    | string  | ✅       | Normalised to title case |
| `SignupDate` | date    |          | Format: `YYYY-MM-DD` |
| `Segment`    | string  |          | One of: `retail`, `wholesale`, `online` |
| `IsActive`   | boolean |          | |

### Source: Nager.Date API

| Field          | Type    | Notes |
|:---------------|:--------|:------|
| `country_code` | char(2) | ISO 3166-1 alpha-2 |
| `date`         | date    | |
| `local_name`   | string  | Name in local language |
| `name`         | string  | English name |
| `is_global`    | boolean | National vs regional holiday |

### Output: `fct_transactions` (warehouse)

All columns from `stg_transactions` plus:

| Column                  | Source |
|:------------------------|:-------|
| `Customer_Country`      | customers JOIN |
| `SignupDate`            | customers JOIN |
| `CustomerSegment`       | customers JOIN |
| `CustomerIsActive`      | customers JOIN |
| `is_known_customer`     | derived: CustomerID exists in dim_customers |
| `is_holiday`            | holidays enrichment |
| `holiday_name`          | holidays enrichment |
| `customer_tenure_days`  | derived: InvoiceDate − SignupDate |
| `TxWeek`                | derived: ISO week number |
| `CountryISO2`           | derived: Country → ISO-2 map |

---

## Business Rules

### Transactions

| Rule | Check | Severity |
|:-----|:------|:--------:|
| `InvoiceNo` must not be null | `check_required_columns` | error |
| `UnitPrice` must be ≥ 0 | `check_positive_values` | warning |
| `InvoiceDate` must not be in the future | `check_no_future_dates` | error |
| `(InvoiceNo, StockCode)` must be unique | cleaning dedup | warning |
| `Status` must be one of allowed values | `check_valid_status` | warning |
| `LineTotal` sign must match `Quantity` sign | `check_line_total_sign` | warning |

### Customers

| Rule | Check | Severity |
|:-----|:------|:--------:|
| `CustomerID` must not be null | quarantine in cleaning | error |
| `CustomerID` must be unique | `check_primary_key_unique` | error |
| `Country` must not be null | `check_no_nulls` | error |

### Referential integrity

| Rule | Check | Severity |
|:-----|:------|:--------:|
| All non-null `CustomerID` in `fct_transactions` must exist in `stg_customers` | `check_orphan_transactions` | warning |

---

## Stage Descriptions

### Stage 1 — Ingestion
- Copies source CSV files to `data/raw/<run_date>/`
- Calls Nager.Date API for public holidays
- Writes `manifest.json` with row counts and timestamps
- **Does not modify source data**

### Stage 2 — Validation
- Checks schema structure (required columns, no unexpected columns)
- Checks content integrity (nulls, PK uniqueness, value ranges)
- Returns a list of `ValidationResult` objects
- Pipeline continues even on failures (exit code raised to 1)

### Stage 3 — Cleaning
- Customers: strip whitespace, normalise Country, parse dates, lowercase Segment, dedup
- Transactions: parse types, derive `LineTotal`, normalise Status, dedup on `(InvoiceNo, StockCode)`
- Quarantined rows saved to `data/interim/rejected_*.csv`
- Clean data saved to `data/interim/*_clean.csv`

### Stage 4 — Transformation
- LEFT JOIN transactions onto customers
- Enrich with holiday flag per (date, country)
- Derive `customer_tenure_days`, `TxWeek`, `CountryISO2`
- Result saved to `data/processed/fct_transactions.csv`

### Stage 5 — Warehouse Load
- Loads `stg_customers`, `stg_transactions`, `fct_transactions`, `dim_holidays`
- Uses `replace` strategy for staging and fact tables
- Uses `append` strategy for `dq_results`
- Skipped gracefully if Postgres is not reachable

### Stage 6 — Quality Checks
- Rule-based checks against loaded DataFrames
- Results stored in `dq_results` table
- Checks: PK duplicates, orphans, null rates, row counts, status values, sign consistency

### Stage 7 — Anomaly Detection
- Statistical checks (Z-score with rolling window)
- Checks: daily volume spike, unit price outliers, cancellation rate spike, null rate spike

### Stage 8 — Reporting
- Generates `reports/quality_report.md` (machine-friendly)
- Generates `reports/quality_report.html` (stakeholder-friendly)
- Includes all stage summaries in a single document
