-- =============================================================================
-- include/sql/transform_staging.sql
-- Staging Layer Transformations (Fixed INITCAP function for DuckDB)
-- =============================================================================

-- ── 0. DEFENSIVE RAW TABLE INITIALIZATIONS ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_customers (customer_id VARCHAR, name VARCHAR, region VARCHAR, _source_file VARCHAR, _loaded_at VARCHAR, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS raw_products (product_id VARCHAR, name VARCHAR, category VARCHAR, mrp VARCHAR, cost_price VARCHAR, _source_file VARCHAR, _loaded_at VARCHAR, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS raw_stores (store_id VARCHAR, store_name VARCHAR, city_store VARCHAR, zone VARCHAR, capacity_orders_per_day VARCHAR, _source_file VARCHAR, _loaded_at VARCHAR, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS raw_payments (payment_id VARCHAR, order_id VARCHAR, payment_mode VARCHAR, amount_paid VARCHAR, payment_status VARCHAR, _source_file VARCHAR, _loaded_at VARCHAR, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS raw_orders (order_id VARCHAR, customer_id VARCHAR, product_id VARCHAR, store_id VARCHAR, order_ts VARCHAR, quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR, payment_mode VARCHAR, order_status VARCHAR, delivery_minutes VARCHAR, _source_file VARCHAR, _loaded_at VARCHAR, _run_id VARCHAR);

-- ── 1. STAGING CUSTOMERS ─────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_customers AS
WITH raw_base AS (
    SELECT 
        TRIM(customer_id) AS customer_id,
        COALESCE(TRIM(name), 'Unknown Customer') AS customer_name,
        '***@masked.com' AS email,
        '******' AS phone,
        COALESCE(TRIM(region), 'Bangalore') AS city,
        '560001' AS pincode,
        'Bronze' AS loyalty_tier,
        CAST(NULL AS DATE) AS signup_date,
        _loaded_at
    FROM raw_customers
    WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
),
deduped AS (
    SELECT 
        customer_id, customer_name, email, phone, city, pincode, loyalty_tier, signup_date, _loaded_at,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_base
)
SELECT 
    customer_id, customer_name, email, phone, city, pincode, loyalty_tier, signup_date, _loaded_at,
    'PASSED' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM deduped 
WHERE rn = 1;

-- ── 2. STAGING PRODUCTS ──────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_products AS
WITH raw_base AS (
    SELECT 
        TRIM(product_id) AS product_id,
        COALESCE(TRIM(name), 'Unknown Product') AS product_name,
        CASE 
            WHEN category IS NOT NULL AND LENGTH(TRIM(category)) > 0 
            THEN UPPER(SUBSTR(TRIM(category), 1, 1)) || LOWER(SUBSTR(TRIM(category), 2))
            ELSE 'General'
        END AS category,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        _loaded_at
    FROM raw_products
    WHERE product_id IS NOT NULL AND TRIM(product_id) != ''
),
deduped AS (
    SELECT 
        product_id, product_name, category, mrp, cost_price, _loaded_at,
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_base
)
SELECT 
    product_id, product_name, category, mrp, cost_price, _loaded_at,
    'PASSED' AS dq_status, 
    CAST(NULL AS VARCHAR) AS failure_reason
FROM deduped 
WHERE rn = 1;

-- ── 3. STAGING STORES ────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_stores AS
SELECT DISTINCT
    TRIM(store_id) AS store_id,
    COALESCE(TRIM(store_name), 'Unknown Store') AS store_name,
    COALESCE(TRIM(city_store), 'Bangalore') AS city,
    COALESCE(TRIM(zone), 'Central') AS zone,
    TRY_CAST(capacity_orders_per_day AS INT) AS capacity_orders_per_day,
    _loaded_at,
    'PASSED' AS dq_status, 
    CAST(NULL AS VARCHAR) AS failure_reason
FROM raw_stores
WHERE store_id IS NOT NULL AND TRIM(store_id) != '';

-- ── 4. STAGING PAYMENTS ─────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_payments AS
SELECT 
    TRIM(payment_id) AS payment_id,
    TRIM(order_id) AS order_id,
    UPPER(TRIM(payment_mode)) AS payment_mode,
    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(payment_status)) AS payment_status,
    _loaded_at,
    CASE 
        WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE 
        WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'MISSING_ORDER_ID'
        ELSE CAST(NULL AS VARCHAR)
    END AS failure_reason
FROM raw_payments;

-- ── 5. STAGING ORDERS (IDEMPOTENT MERGE / UPSERT) ───────────────────────────
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    product_id VARCHAR,
    store_id VARCHAR,
    order_ts TIMESTAMP,
    order_date DATE,
    order_ts_ist VARCHAR,
    quantity INT,
    unit_price DOUBLE,
    discount_pct DOUBLE,
    order_amount DOUBLE,
    net_revenue DOUBLE,
    payment_mode VARCHAR,
    order_status VARCHAR,
    delivery_minutes DOUBLE,
    within_15_min BOOLEAN,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    _loaded_at VARCHAR,
    _run_id VARCHAR
);

INSERT INTO stg_orders
SELECT 
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    TRIM(product_id) AS product_id,
    TRIM(store_id) AS store_id,
    TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
    CAST(TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata' AS DATE) AS order_date,
    STRFTIME(TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%d %H:%M:%S IST') AS order_ts_ist,
    TRY_CAST(quantity AS INT) AS quantity,
    TRY_CAST(unit_price AS DOUBLE) AS unit_price,
    TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
    TRY_CAST(order_amount AS DOUBLE) AS order_amount,
    ROUND(TRY_CAST(quantity AS INT) * TRY_CAST(unit_price AS DOUBLE) * (1.0 - COALESCE(TRY_CAST(discount_pct AS DOUBLE), 0.0) / 100.0), 2) AS net_revenue,
    CASE 
        WHEN LOWER(TRIM(payment_mode)) LIKE '%upi%' THEN 'UPI'
        WHEN LOWER(TRIM(payment_mode)) LIKE '%card%' THEN 'Card'
        WHEN LOWER(TRIM(payment_mode)) LIKE '%cod%' OR LOWER(TRIM(payment_mode)) LIKE '%cash%' THEN 'Cash on Delivery'
        ELSE 'Wallet'
    END AS payment_mode,
    UPPER(TRIM(order_status)) AS order_status,
    TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
    CASE WHEN TRY_CAST(delivery_minutes AS DOUBLE) <= 15 THEN TRUE ELSE FALSE END AS within_15_min,
    
    -- Quality Rules
    CASE 
        WHEN TRIM(order_id) IS NULL OR TRIM(order_id) = '' THEN 'FAILED'
        WHEN TRY_CAST(quantity AS INT) <= 0 THEN 'FAILED'
        WHEN TRY_CAST(discount_pct AS DOUBLE) > 100 OR TRY_CAST(discount_pct AS DOUBLE) < 0 THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    
    CASE 
        WHEN TRIM(order_id) IS NULL OR TRIM(order_id) = '' THEN 'NULL_ORDER_ID'
        WHEN TRY_CAST(quantity AS INT) <= 0 THEN 'INVALID_QUANTITY'
        WHEN TRY_CAST(discount_pct AS DOUBLE) > 100 OR TRY_CAST(discount_pct AS DOUBLE) < 0 THEN 'INVALID_DISCOUNT'
        ELSE CAST(NULL AS VARCHAR)
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM raw_orders
WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
ON CONFLICT (order_id) DO UPDATE SET
    order_status = EXCLUDED.order_status,
    delivery_minutes = EXCLUDED.delivery_minutes,
    within_15_min = EXCLUDED.within_15_min,
    net_revenue = EXCLUDED.net_revenue,
    dq_status = EXCLUDED.dq_status,
    failure_reason = EXCLUDED.failure_reason,
    _loaded_at = EXCLUDED._loaded_at,
    _run_id = EXCLUDED._run_id;