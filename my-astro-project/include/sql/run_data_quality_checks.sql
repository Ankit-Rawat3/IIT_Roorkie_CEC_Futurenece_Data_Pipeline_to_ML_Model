-- =============================================================================
-- Data Quality Framework Evaluation Script
-- =============================================================================

CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR,
    dag_id VARCHAR,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    rows_checked INT,
    rows_failed INT,
    status VARCHAR,
    checked_at TIMESTAMP
);

-- 1. COMPLETENESS CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_completeness_order_keys' AS check_name,
    'Completeness' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE order_id IS NULL OR customer_id IS NULL OR TRIM(order_id) = '') AS rows_failed,
    CASE WHEN COUNT(*) FILTER (WHERE order_id IS NULL OR customer_id IS NULL OR TRIM(order_id) = '') > 0 THEN 'FAIL' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM raw_orders;

-- 2. UNIQUENESS CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_uniqueness_orders' AS check_name,
    'Uniqueness' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) - COUNT(DISTINCT order_id) AS rows_failed,
    CASE WHEN COUNT(*) - COUNT(DISTINCT order_id) > 0 THEN 'FAIL' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM raw_orders;

-- 3. VALIDITY CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_validity_order_status' AS check_name,
    'Validity' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'PENDING', 'RETURNED')) AS rows_failed,
    CASE WHEN COUNT(*) FILTER (WHERE UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'PENDING', 'RETURNED')) > 0 THEN 'FAIL' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM raw_orders;

-- 4. ACCURACY CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_accuracy_positive_qty' AS check_name,
    'Accuracy' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE TRY_CAST(quantity AS INT) <= 0 OR TRY_CAST(discount_pct AS DOUBLE) < 0 OR TRY_CAST(discount_pct AS DOUBLE) > 100) AS rows_failed,
    CASE WHEN COUNT(*) FILTER (WHERE TRY_CAST(quantity AS INT) <= 0 OR TRY_CAST(discount_pct AS DOUBLE) < 0 OR TRY_CAST(discount_pct AS DOUBLE) > 100) > 0 THEN 'FAIL' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM raw_orders;

-- 5. CONSISTENCY CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_consistency_foreign_keys' AS check_name,
    'Consistency' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE c.customer_id IS NULL OR p.product_id IS NULL) AS rows_failed,
    CASE WHEN COUNT(*) FILTER (WHERE c.customer_id IS NULL OR p.product_id IS NULL) > 0 THEN 'FAIL' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
LEFT JOIN stg_products p ON o.product_id = p.product_id;

-- 6. TIMELINESS CHECK
INSERT INTO dq_results
SELECT 
    'manual_run' AS run_id,
    'nammamart_elt_Ankit_Rawat' AS dag_id,
    'chk_timeliness_future_orders' AS check_name,
    'Timeliness' AS dimension,
    'Warning' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE TRY_CAST(order_ts AS TIMESTAMP) > CURRENT_TIMESTAMP) AS rows_failed,
    CASE WHEN COUNT(*) FILTER (WHERE TRY_CAST(order_ts AS TIMESTAMP) > CURRENT_TIMESTAMP) > 0 THEN 'WARN' ELSE 'PASS' END AS status,
    CURRENT_TIMESTAMP AS checked_at
FROM raw_orders;