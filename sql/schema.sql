-- ============================================================
--  sql/schema.sql
--  Create warehouse tables for the data quality pipeline.
--  Run once; safe to re-run (uses IF NOT EXISTS).
-- ============================================================

-- Staging: customers 
CREATE TABLE IF NOT EXISTS stg_customers (
    "CustomerID"   TEXT         NOT NULL,
    "Country"      TEXT,
    "SignupDate"   TIMESTAMP,
    "Segment"      TEXT,
    "IsActive"     BOOLEAN,
    _loaded_at     TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_stg_customers_id
    ON stg_customers ("CustomerID");

-- Staging: transactions 
CREATE TABLE IF NOT EXISTS stg_transactions (
    "InvoiceNo"     TEXT          NOT NULL,
    "StockCode"     TEXT,
    "Description"   TEXT,
    "Quantity"      INTEGER,
    "InvoiceDate"   TIMESTAMP,
    "UnitPrice"     NUMERIC(12,4),
    "CustomerID"    TEXT,
    "Country"       TEXT,
    "Status"        TEXT,
    "PaymentMethod" TEXT,
    "LineTotal"     NUMERIC(14,4),
    "TxYear"        SMALLINT,
    "TxMonth"       SMALLINT,
    "TxDay"         SMALLINT,
    "TxDOW"         TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_stg_tx_customer
    ON stg_transactions ("CustomerID");

CREATE INDEX IF NOT EXISTS ix_stg_tx_date
    ON stg_transactions ("InvoiceDate");

-- Dimension: holidays 
CREATE TABLE IF NOT EXISTS dim_holidays (
    country_code   CHAR(2)      NOT NULL,
    "date"         DATE         NOT NULL,
    local_name     TEXT,
    name           TEXT,
    is_global      BOOLEAN,
    _loaded_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (country_code, "date")
);

-- Fact: transactions (enriched) 
CREATE TABLE IF NOT EXISTS fct_transactions (
    "InvoiceNo"             TEXT,
    "StockCode"             TEXT,
    "Description"           TEXT,
    "Quantity"              INTEGER,
    "InvoiceDate"           TIMESTAMP,
    "UnitPrice"             NUMERIC(12,4),
    "CustomerID"            TEXT,
    "Country"               TEXT,
    "Status"                TEXT,
    "PaymentMethod"         TEXT,
    "LineTotal"             NUMERIC(14,4),
    "TxYear"                SMALLINT,
    "TxMonth"               SMALLINT,
    "TxDay"                 SMALLINT,
    "TxDOW"                 TEXT,
    "TxWeek"                SMALLINT,
    "Customer_Country"      TEXT,
    "SignupDate"            TIMESTAMP,
    "CustomerSegment"       TEXT,
    "CustomerIsActive"      BOOLEAN,
    "is_known_customer"     BOOLEAN,
    "is_holiday"            BOOLEAN,
    "holiday_name"          TEXT,
    "customer_tenure_days"  INTEGER,
    "CountryISO2"           CHAR(2),
    _loaded_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fct_tx_date
    ON fct_transactions ("InvoiceDate");

CREATE INDEX IF NOT EXISTS ix_fct_tx_customer
    ON fct_transactions ("CustomerID");

-- Data quality results 
CREATE TABLE IF NOT EXISTS dq_results (
    id             SERIAL PRIMARY KEY,
    run_date       DATE,
    "table"        TEXT,
    check          TEXT,
    status         TEXT,       -- PASS | FAIL
    severity       TEXT,       -- error | warning | info
    message        TEXT,
    rows_affected  INTEGER,
    checked_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_dq_run_date
    ON dq_results (run_date);
