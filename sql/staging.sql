-- ============================================================
--  staging.sql
--  Staging-layer transformations run after the raw load.
--  These queries create clean, typed staging views/tables
--  from the raw stg_* tables, ready for the fact layer.
--
--  Run order: schema.sql → staging.sql → quality_checks.sql
-- ============================================================

-- 1. Staging view: customers (normalised) 
CREATE OR REPLACE VIEW stg_customers_v AS
SELECT
    "CustomerID"                                        AS customer_id,
    INITCAP(TRIM("Country"))                            AS country,
    "SignupDate"::DATE                                  AS signup_date,
    LOWER(TRIM(COALESCE("Segment", 'unknown')))         AS segment,
    COALESCE("IsActive", FALSE)                         AS is_active,
    DATE_PART('day', NOW() - "SignupDate"::TIMESTAMP)   AS tenure_days,
    _loaded_at
FROM stg_customers
WHERE "CustomerID" IS NOT NULL;

-- 2. Staging view: transactions (typed + enriched)
CREATE OR REPLACE VIEW stg_transactions_v AS
SELECT
    "InvoiceNo"                                         AS invoice_no,
    "StockCode"                                         AS stock_code,
    INITCAP(TRIM("Description"))                        AS description,
    "Quantity"::INTEGER                                 AS quantity,
    "InvoiceDate"::TIMESTAMP                            AS invoice_date,
    "UnitPrice"::NUMERIC(12,4)                          AS unit_price,
    ("Quantity" * "UnitPrice")::NUMERIC(14,4)           AS line_total,
    COALESCE("CustomerID", 'GUEST')                     AS customer_id,
    INITCAP(TRIM("Country"))                            AS country,
    LOWER(TRIM(COALESCE("Status", 'unknown')))          AS status,
    LOWER(TRIM(COALESCE("PaymentMethod", 'unknown')))   AS payment_method,
    DATE_TRUNC('day',  "InvoiceDate"::TIMESTAMP)        AS invoice_day,
    DATE_TRUNC('week', "InvoiceDate"::TIMESTAMP)        AS invoice_week,
    DATE_TRUNC('month',"InvoiceDate"::TIMESTAMP)        AS invoice_month,
    EXTRACT(DOW  FROM "InvoiceDate"::TIMESTAMP)::INT    AS day_of_week,   -- 0=Sun
    EXTRACT(HOUR FROM "InvoiceDate"::TIMESTAMP)::INT    AS hour_of_day,
    _loaded_at
FROM stg_transactions
WHERE "UnitPrice" >= 0
  AND "InvoiceDate"::TIMESTAMP <= NOW()
  AND "InvoiceNo" IS NOT NULL;

-- 3. Daily aggregation: transaction summary 
CREATE OR REPLACE VIEW stg_daily_summary AS
SELECT
    DATE_TRUNC('day', "InvoiceDate"::TIMESTAMP)         AS tx_date,
    COUNT(*)                                            AS tx_count,
    COUNT(DISTINCT "CustomerID")                        AS unique_customers,
    COUNT(DISTINCT "InvoiceNo")                         AS unique_invoices,
    SUM("Quantity" * "UnitPrice")::NUMERIC(16,2)        AS gross_revenue,
    AVG("UnitPrice")::NUMERIC(10,4)                     AS avg_unit_price,
    SUM(CASE WHEN "Status" = 'cancelled' THEN 1 ELSE 0 END)   AS cancelled_count,
    SUM(CASE WHEN "Status" = 'refunded'  THEN 1 ELSE 0 END)   AS refunded_count,
    ROUND(
        100.0 * SUM(CASE WHEN "Status" = 'cancelled' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
        2
    )                                                   AS cancellation_rate_pct
FROM stg_transactions
GROUP BY DATE_TRUNC('day', "InvoiceDate"::TIMESTAMP)
ORDER BY tx_date;

-- 4. Customer segment summary 
CREATE OR REPLACE VIEW stg_customer_segments AS
SELECT
    LOWER(TRIM(COALESCE("Segment", 'unknown')))         AS segment,
    COUNT(*)                                            AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2)  AS pct_of_total,
    COUNT(CASE WHEN "IsActive" THEN 1 END)              AS active_count,
    MIN("SignupDate"::DATE)                             AS earliest_signup,
    MAX("SignupDate"::DATE)                             AS latest_signup
FROM stg_customers
WHERE "CustomerID" IS NOT NULL
GROUP BY LOWER(TRIM(COALESCE("Segment", 'unknown')))
ORDER BY customer_count DESC;

-- 5. Country revenue summary 
CREATE OR REPLACE VIEW stg_country_revenue AS
SELECT
    INITCAP(TRIM("Country"))                            AS country,
    COUNT(*)                                            AS tx_count,
    COUNT(DISTINCT "CustomerID")                        AS unique_customers,
    SUM("Quantity" * "UnitPrice")::NUMERIC(16,2)        AS total_revenue,
    AVG("Quantity" * "UnitPrice")::NUMERIC(10,4)        AS avg_order_value,
    RANK() OVER (ORDER BY SUM("Quantity" * "UnitPrice") DESC) AS revenue_rank
FROM stg_transactions
WHERE "UnitPrice" >= 0
GROUP BY INITCAP(TRIM("Country"))
ORDER BY total_revenue DESC;

-- 7. Data freshness check 
CREATE OR REPLACE VIEW stg_freshness AS
SELECT
    'stg_customers'    AS table_name,
    MAX(_loaded_at)    AS last_loaded_at,
    COUNT(*)           AS row_count
FROM stg_customers
UNION ALL
SELECT
    'stg_transactions',
    MAX(_loaded_at),
    COUNT(*)
FROM stg_transactions
UNION ALL
SELECT
    'fct_transactions',
    MAX(_loaded_at),
    COUNT(*)
FROM fct_transactions;
