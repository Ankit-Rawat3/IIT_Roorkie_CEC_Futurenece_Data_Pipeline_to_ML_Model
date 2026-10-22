-- ── 1. MART: mart_daily_store_kpis ───────────────────────────────────────
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT 
    o.order_date,
    COALESCE(s.store_id, 'Unknown') AS store_id,
    COALESCE(s.store_name, 'Unknown Store') AS store_name,
    COALESCE(s.zone, 'Unknown') AS zone,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_net_revenue,
    ROUND(AVG(o.delivery_minutes), 1) AS avg_delivery_minutes
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.store_id, s.store_name, s.zone;

-- ── 2. MART: mart_category_margin ────────────────────────────────────────
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT 
    COALESCE(p.category, 'Uncategorized') AS category,
    SUM(o.quantity) AS total_units_sold,
    ROUND(SUM(o.net_revenue), 2) AS total_net_revenue,
    ROUND(SUM(o.net_revenue - (o.quantity * COALESCE(p.cost_price, 0.0))), 2) AS total_gross_margin,
    ROUND((SUM(o.net_revenue - (o.quantity * COALESCE(p.cost_price, 0.0))) / NULLIF(SUM(o.net_revenue), 0)) * 100.0, 2) AS gross_margin_pct
FROM stg_orders o
LEFT JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY p.category;

-- ── 3. MART: mart_delivery_sla ───────────────────────────────────────────
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT 
    o.order_date,
    COALESCE(s.zone, 'Unknown') AS zone,
    COUNT(*) AS total_delivered_orders,
    COUNT(CASE WHEN o.within_15_min THEN 1 END) AS orders_within_15_min,
    ROUND(COUNT(CASE WHEN o.within_15_min THEN 1 END) * 100.0 / COUNT(*), 2) AS sla_compliance_pct
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone;

-- ── 4. MART: mart_payment_reconciliation ─────────────────────────────────
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH order_summary AS (
    SELECT order_id, order_amount FROM stg_orders WHERE dq_status = 'PASSED'
),
payment_summary AS (
    SELECT order_id, amount_paid, payment_status FROM stg_payments WHERE dq_status = 'PASSED'
)
SELECT 
    COALESCE(o.order_id, p.order_id) AS order_id,
    COALESCE(o.order_amount, 0.0) AS order_amount,
    COALESCE(p.amount_paid, 0.0) AS amount_paid,
    COALESCE(p.payment_status, 'MISSING') AS payment_status,
    CASE 
        WHEN p.order_id IS NULL THEN 'MISSING_PAYMENT_RECORD'
        WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT_NO_ORDER'
        WHEN p.payment_status = 'SUCCESS' AND ABS(p.amount_paid - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
        WHEN p.payment_status != 'SUCCESS' THEN 'PAYMENT_FAILED_OR_PENDING'
        ELSE 'RECONCILED'
    END AS reconciliation_reason
FROM order_summary o
FULL OUTER JOIN payment_summary p ON o.order_id = p.order_id
WHERE CASE 
        WHEN p.order_id IS NULL THEN 'MISSING_PAYMENT_RECORD'
        WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT_NO_ORDER'
        WHEN p.payment_status = 'SUCCESS' AND ABS(p.amount_paid - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
        WHEN p.payment_status != 'SUCCESS' THEN 'PAYMENT_FAILED_OR_PENDING'
        ELSE 'RECONCILED'
    END != 'RECONCILED';

-- ── 5. MART: mart_top_customers ──────────────────────────────────────────
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.loyalty_tier,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.customer_name, c.email, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;