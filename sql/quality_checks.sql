-- sql/quality_checks.sql

-- Ad-hoc quality checks runnable directly in Postgres.

-- 1. Duplicate primary keys in stg_customers
SELECT
    'stg_customers — duplicate CustomerID' AS check_name,
    COUNT(*) - COUNT(DISTINCT "CustomerID") AS duplicates
FROM stg_customers;

-- 2. Duplicate InvoiceNo + StockCode in stg_transactions
SELECT
    'stg_transactions — duplicate (InvoiceNo, StockCode)' AS check_name,
    COUNT(*) - COUNT(DISTINCT ("InvoiceNo" || '|' || "StockCode")) AS duplicates
FROM stg_transactions;

-- 3. Null rate on critical transaction columns
SELECT
    'stg_transactions — null rates' AS check_name,
    ROUND(SUM(CASE WHEN "InvoiceNo" IS NULL THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 4) AS null_rate_InvoiceNo,
    ROUND(SUM(CASE WHEN "InvoiceDate" IS NULL THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 4) AS null_rate_InvoiceDate,
    ROUND(SUM(CASE WHEN "UnitPrice" IS NULL THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 4) AS null_rate_UnitPrice,
    ROUND(SUM(CASE WHEN "CustomerID" IS NULL THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 4) AS null_rate_CustomerID
FROM stg_transactions;

-- 4. Orphan transactions
SELECT
    'fct_transactions — orphan transactions' AS check_name,
    COUNT(*) AS orphan_count
FROM fct_transactions f
WHERE f."CustomerID" IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM stg_customers c
    WHERE c."CustomerID" = f."CustomerID"
  );

-- 5. Negative UnitPrice
SELECT
    'stg_transactions — negative UnitPrice' AS check_name,
    COUNT(*) AS count_negative_price
FROM stg_transactions
WHERE "UnitPrice" < 0;

-- 6. Future-dated invoices
SELECT
    'stg_transactions — future-dated invoices' AS check_name,
    COUNT(*) AS count_future
FROM stg_transactions
WHERE "InvoiceDate" > NOW();

-- 7. Unexpected Status values
SELECT
    'stg_transactions — invalid Status' AS check_name,
    "Status",
    COUNT(*) AS row_count
FROM stg_transactions
WHERE "Status" NOT IN ('completed', 'cancelled', 'refunded')
GROUP BY "Status"
ORDER BY row_count DESC;

-- 8. LineTotal sign mismatch
SELECT
    'fct_transactions — LineTotal sign mismatch' AS check_name,
    COUNT(*) AS mismatches
FROM fct_transactions
WHERE "Quantity" < 0
  AND "LineTotal" > 0;

-- 9. Daily transaction volume
SELECT
    DATE("InvoiceDate") AS tx_date,
    COUNT(*) AS tx_count,
    SUM("LineTotal") AS daily_revenue,
    AVG("UnitPrice") AS avg_unit_price
FROM fct_transactions
GROUP BY DATE("InvoiceDate")
ORDER BY tx_date;

-- 10. Row count comparison stg vs fct
SELECT
    'row count: stg_transactions' AS source,
    COUNT(*) AS rows
FROM stg_transactions

UNION ALL

SELECT
    'row count: fct_transactions',
    COUNT(*)
FROM fct_transactions

UNION ALL

SELECT
    'row count: stg_customers',
    COUNT(*)
FROM stg_customers;

-- 11. Latest dq_results summary
SELECT
    run_date,
    status,
    severity,
    COUNT(*) AS check_count
FROM dq_results
WHERE run_date = (SELECT MAX(run_date) FROM dq_results)
GROUP BY run_date, status, severity
ORDER BY status, severity;