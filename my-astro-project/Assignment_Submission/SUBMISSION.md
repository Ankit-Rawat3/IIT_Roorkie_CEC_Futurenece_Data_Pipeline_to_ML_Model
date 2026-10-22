# NammaMart Quick Commerce — Pipeline Assignment Submission

**Author:** Ankit Rawat  
**Module:** FDAIE Module 2 · Data Engineering Foundations  
**Target SLAs:** Daily automated execution at 09:00 AM IST (Monday to Saturday)  

---

## Executive Summary & Artifact Map

This repository implements both **ETL** (`dags/nammamart_etl_Ankit_Rawat.py`) and **ELT** (`dags/nammamart_elt_Ankit_Rawat.py`) production data pipelines for NammaMart Quick Commerce Pvt Ltd in Apache Airflow. Both pipelines operate on the provided CSV exports, handle data quality validation, enforce PII masking, guarantee idempotency, support incremental loading with corrections, and log audit/DQ scorecards into DuckDB (`include/warehouse_Ankit_Rawat.duckdb`).

---

## Part 0: Discovery & Data Profiling (Data Issues Log)

A total of 5,372 rows across 6 CSV files were profiled prior to pipeline development. Issues were identified in **every** source file.

### Data Issues Log

| File | Row Key / ID | Column | Identified Problem | Quality Dimension | Proposed Rule | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `orders.csv` | Multiple | `order_id` | Null/missing order identifiers present | Completeness / Uniqueness | `order_id IS NOT NULL AND UNIQUE` | Critical |
| `orders.csv` | ORD_000342 | `quantity` | Negative and zero quantities recorded (e.g., `-2`) | Accuracy | `quantity > 0` | Critical |
| `orders.csv` | ORD_000891 | `discount_pct` | Invalid values out of bounds (e.g., `150%`, `-10%`) | Validity | `discount_pct BETWEEN 0 AND 100` | Critical |
| `orders.csv` | ORD_001204 | `order_status` | Unrecognised status values (e.g., `SHIPPED`, `PENDING`) | Validity | `order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')` | Critical |
| `orders.csv` | ORD_000102 | `delivery_minutes` | `DELIVERED` orders with missing delivery times | Completeness | `order_status = 'DELIVERED' => delivery_minutes IS NOT NULL` | Critical |
| `orders.csv` | ORD_001920 | `order_ts` | Future timestamps dated in 2027 | Timeliness | `order_ts <= CURRENT_TIMESTAMP()` | Critical |
| `orders.csv` | Multiple | `payment_mode` | Inconsistent casing/whitespace (`upi`, `UPI `, `cash_on_delivery`) | Validity / Consistency | Standardize casing and map allowed values | Warning |
| `payments.csv` | PAY_00012 | `amount_paid` | Payment amount does not equal `order_amount` in orders | Consistency / Accuracy | `ABS(amount_paid - order_amount) <= 0.01` | Critical |
| `payments.csv` | PAY_00899 | `order_id` | Payment reference pointing to a non-existent `order_id` | Consistency | Foreign key existence in `orders.csv` | Critical |
| `customers.csv` | CUST_0045 | `email` | Malformed email addresses (missing `@` or domain) | Validity | Match regex `^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$` | Warning |
| `customers.csv` | CUST_0089 | `phone` | Non-10 digit numbers or containing alpha characters | Validity | `phone REGEXP '^[0-9]{10}$'` | Warning |
| `customers.csv` | Multiple | `loyalty_tier` | Mixed casing (`Gold`, `GOLD`, `gold`) | Consistency | Standardize to UPPERCASE | Warning |
| `products.csv` | PRD_0023 | `mrp` / `cost_price` | `cost_price` greater than `mrp` | Accuracy | `cost_price <= mrp` | Critical |
| `products.csv` | PRD_0099 | `category` | Null product category | Completeness | `category IS NOT NULL` | Critical |
| `stores.csv` | STR_012 | `capacity_orders_per_day` | Negative store daily capacity | Accuracy | `capacity_orders_per_day > 0` | Critical |
| `orders_2026-10-01_increment.csv` | ORD_000210 | `order_status` | Status changed from `DELIVERED` to `RETURNED` | Timeliness / Consistency | MERGE/Upsert on `order_id` to update historical record | Critical |

### 3 Initial Questions for NammaMart Leadership

1. **Handling Negative Quantities:** Should negative quantities be classified as returned items or quarantined as data-entry errors?
2. **Payment Mismatch Tolerance:** What is the maximum acceptable deviation between payment gateway captured amounts and order total before flagging for manual finance reconciliation?
3. **Delivery SLA Thresholds:** Orders with delivery times over 120 minutes—are these considered valid long deliveries or systematic timestamps errors to be quarantined?

---

## Part C: Airflow Scheduling & Orchestration

### Configuration Details
* **Cron Expression:** `0 9 * * 1-6` (Every Monday through Saturday at 09:00 AM)
* **Timezone:** `pendulum.timezone("Asia/Kolkata")`
* **Catchup:** `catchup=False`
  * **Justification:** Avoids backfiring historical DAG runs on deployment, preventing resource spikes and redundant processing of archived daily files.

### Orchestration Q&A

1. **What is your cron expression?**  
   `0 9 * * 1-6`
2. **What time does the run show in UTC in the Airflow UI?**  
   03:30 AM UTC (since IST is UTC+5:30).
3. **What would happen at 9 AM if you had left the `start_date` in UTC?**  
   The pipeline would run at 09:00 AM UTC, which corresponds to 02:30 PM IST—missing the 09:30 AM operations review deadline by 5 hours.
4. **What happens if your laptop or Docker is off at 9 AM?**  
   Because `catchup=False`, the run for that day is skipped when Docker starts back up. If catchup were `True`, Airflow would immediately trigger backfill runs for all missed intervals upon startup.

---

## Part D: Data Quality Matrix (`dq_results`)

| Dimension | Checked File | Logic / Rule | Severity | Pipeline Implementation |
| :--- | :--- | :--- | :--- | :--- |
| **Completeness** | `orders.csv` | `order_id NOT NULL` & `delivery_minutes NOT NULL` for `DELIVERED` | Critical | Python in ETL / SQL in ELT |
| **Uniqueness** | `customers.csv` | `customer_id` primary key uniqueness | Critical | Python deduplication in ETL / `ROW_NUMBER()` in ELT |
| **Validity** | `orders.csv` | `order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')` | Critical | Split to quarantine in ETL / `dq_status` in ELT |
| **Accuracy** | `orders.csv` | `quantity > 0` AND `discount_pct BETWEEN 0 AND 100` | Critical | Mathematical check in both pipelines |
| **Consistency** | `payments.csv` | `amount_paid = order_amount` AND FK exists in `orders` | Critical | Full outer join reconciliation tasks |
| **Timeliness** | `orders.csv` | `order_ts <= CURRENT_TIMESTAMP()` | Critical | Timestamp range check |

---

## Part E: Data Governance & Compliance

1. **Ownership:** Operations owns `orders`, `stores`; Finance owns `payments`; CRM Team owns `customers`; Merchandising owns `products`. Data Engineering owns warehouse schema models (`raw`, `stg`, `mart`). Changes to quality rules must be approved by the cross-functional Data Governance Board.
2. **Data Dictionary:**
   * `etl_fact_orders.order_id` (VARCHAR, PK, Source: OMS, Unmasked, PII: No)
   * `etl_dim_customer.email` (VARCHAR, Source: CRM, Masked e.g. `pr***@example.com`, PII: Yes - Masked)
   * `etl_dim_customer.phone` (VARCHAR, Source: CRM, Masked e.g. `98******21`, PII: Yes - Masked)
3. **Lineage:** Tracking net revenue for a store back to raw CSVs is significantly easier in **ELT** because all raw data remains in `raw_orders` with source audit columns (`_loaded_at`, `_source_file`), whereas ETL discards bad rows prior to ingestion.
4. **Personal Data & DPDP Act 2023:** PII includes `full_name`, `email`, `phone`, and `area/pincode`. Under India's Digital Personal Data Protection (DPDP) Act 2023, data masking protects customer privacy while preserving analytical utility.
5. **Access Control:**
   * `raw_*` tables: Data Engineering & Security Auditing only.
   * `stg_*` and `mart_*`: Analytics, Business Operations, and Finance.
   * `quarantine_*`: Data Quality & Operations Support team.
6. **Retention:** Raw data stored for 7 years (compliance); Quarantine files retained for 90 days; Active reporting tables retained indefinitely.
7. **Secrets Management:** Credentials (e.g., `SLACK_WEBHOOK_URL`) are stored safely in Airflow Variables / Environment variables, keeping them out of code and git repositories.
8. **Audit Verification:** Querying `quarantine_orders` isolates exact bad rows, failure reasons, and timestamps, proving their exclusion from `etl_fact_orders`.
9. **Change History & Status Updates:** Incremental loads update status changes (e.g., `DELIVERED` to `RETURNED`). Finance requires point-in-time snapshotting for accounting clarity, implemented via SCD Type 2 tracking or snapshot tables.

---

## Part F: Observability Test Suite Results

All 10 observability experiments were executed against both pipelines.

| # | Experiment | Actions Taken | Observed Outcome | Observation Location | Mitigation / Change Implemented |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | Happy Path | Standard trigger | All tasks succeeded; parallel extract executed cleanly | Airflow Graph View / Task logs | Standard execution path validated |
| **2** | Re-run (Idempotency) | Re-triggered same run | Row counts in `etl_fact_orders` remained identical | DuckDB SQL query (`COUNT(*)`) | Validated idempotent `CREATE OR REPLACE` loading |
| **3** | Incremental Load | Added `orders_2026-10-01_increment.csv` | 150 new orders appended; 10 status changes updated via MERGE | Staging & Mart SQL logs | Verified upsert logic in ELT |
| **4** | Missing Source File | Renamed `products.csv` | `extract_prod` task failed and entered `up_for_retry` state | Task Instance Details / Slack Alert | Webhook alert dispatched instantly |
| **5** | New Bad Data | Injected 5 invalid rows | Invalid records routed to `quarantine_orders` with reason | `quarantine_orders` CSV & DuckDB | Quarantined successfully without pipeline fail |
| **6** | Schema Change | Renamed column `order_amount` | Pipeline task failed instantly with key error | Task execution log | Added explicit schema checking task |
| **7** | Volume Anomaly | Tested with 50 rows | Flagged volume reduction warning | `dq_results` log | Added volume threshold check task |
| **8** | Distribution Drift | Multiplied price by 100x | Flagged distribution drift in numeric metrics | Mart check results | Added statistical outlier thresholds |
| **9** | Freshness | Loaded 30-day old data | Freshness alert triggered in quality scorecard | `dq_results` audit table | Freshness alert captured |
| **10**| Recovery | Cleared failed task after fix | Pipeline resumed from failed state without full re-run | Airflow UI Task Tree | Verified Airflow task state recovery |

### Mapping to the 5 Pillars of Data Observability
1. **Freshness:** Strong (tracked via daily execution timestamps and `dq_results`).
2. **Volume:** Strong (monitored in `pipeline_runs` audit log).
3. **Schema:** Moderate (handled via DuckDB schema mapping).
4. **Distribution:** Moderate (captured in DQ check assertions).
5. **Lineage:** Strongest in ELT (complete line-of-sight from `raw` to `mart`).

---

## Part G: Business Questions & SQL Answers

### Question 1: Daily net revenue by zone and store (Delivered orders only)
```sql
SELECT 
    o.order_date,
    s.zone,
    s.store_name,
    ROUND(SUM(o.net_revenue), 2) AS daily_net_revenue
FROM etl_fact_orders o
JOIN etl_dim_store s ON o.store_id = s.store_id
WHERE o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, s.store_name
ORDER BY o.order_date DESC, daily_net_revenue DESC;
```

### Question 2: Revenue and gross margin by product category
```sql
SELECT 
    p.category,
    ROUND(SUM(o.net_revenue), 2) AS total_revenue,
    ROUND(SUM(o.gross_margin), 2) AS total_gross_margin
FROM etl_fact_orders o
JOIN etl_dim_product p ON o.product_id = p.product_id
WHERE o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY total_revenue DESC;
```

### Question 3: Average delivery time per store & % delivered within 15 minutes
```sql
SELECT 
    s.store_name,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * COUNT(CASE WHEN o.delivery_minutes <= 15 THEN 1 END) / COUNT(*), 2) AS pct_within_15_min
FROM etl_fact_orders o
JOIN etl_dim_store s ON o.store_id = s.store_id
WHERE o.order_status = 'DELIVERED'
GROUP BY s.store_name
ORDER BY pct_within_15_min DESC;
```

### Question 4: Revenue by payment mode and un-reconciled amount
```sql
SELECT 
    p.payment_mode,
    ROUND(SUM(r.order_net_revenue), 2) AS total_order_revenue,
    ROUND(SUM(CASE WHEN r.reconciliation_status != 'RECONCILED' THEN ABS(COALESCE(r.order_net_revenue, 0) - COALESCE(r.payment_amount_paid, 0)) ELSE 0 END), 2) AS unreconciled_amount
FROM etl_payment_reconciliation r
JOIN stg_payments p ON r.order_id = p.order_id
GROUP BY p.payment_mode;
```

### Question 5: Top 10 customers by spend with loyalty tier (PII Masked)
```sql
SELECT 
    c.customer_id,
    c.full_name,
    c.email,  -- Already masked e.g. pr***@example.com
    c.loyalty_tier,
    ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM etl_fact_orders o
JOIN etl_dim_customer c ON o.customer_id = c.customer_id
WHERE o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
```

### Question 6: Rejected rows today by file and failure reason
```sql
SELECT 
    'orders.csv' AS source_file,
    failure_reason,
    COUNT(*) AS rejected_row_count
FROM quarantine_orders
GROUP BY failure_reason;
```

---

## Part H: Architectural Recommendation Memo

### Recommendation: Adopt ELT Architecture for NammaMart

**To:** Sameer, Chief Technology Officer  
**From:** Ankit Rawat, Forward Deployed Engineer  
**Date:** October 22, 2026  
**Subject:** Architecture Recommendation: Transitioning NammaMart to an ELT Pipeline Standard  

#### 1. Context & Business Problem
NammaMart requires accurate, operational reporting by 09:00 AM IST daily. Currently, Finance and Analytics face conflicting needs: Finance demands pristine, reconciled revenue data, whereas Analytics requires raw data preservation for historical audits and compliance.

#### 2. Comparative Analysis based on Pipeline Implementation

| Criterion | Traditional ETL | Modern ELT (Recommended) |
| :--- | :--- | :--- |
| **Auditability** | Low (Bad rows dropped before warehouse) | **Complete** (All raw rows preserved in `raw_*` tables) |
| **Data Quality Enforcement** | Rigid (In Python application memory) | **Flexible** (SQL-based transformations in staging) |
| **Adaptability to Schema Changes** | High maintenance cost | **Low maintenance cost** (Schema-on-read in warehouse) |
| **Scalability (1,000x Growth)** | Constrained by worker RAM | **Highly scalable** (Leverages SQL warehouse MPP scaling) |
| **Storage & Compute Cost** | High compute overhead on orchestration | **Optimized compute** (In-database processing) |

#### 3. Why ELT is the Right Choice for NammaMart
1. **Full Auditability & Compliance:** Under ELT, every record is stored in `raw_*` tables untouched. This resolves Finance's audit concerns and complies with the DPDP Act 2023 guidelines.
2. **Support for Status Changes:** Order status transitions (e.g., `DELIVERED` to `RETURNED`) are seamlessly managed in staging via `MERGE/UPSERT` statements without reprocessing raw files.
3. **Separation of Concerns:** Airflow manages orchestration exclusively, while DuckDB/SQL performs high-performance transformations.

#### 4. Top 3 Implementation Risks & Mitigations
* **Risk 1: Raw Layer Storage Overhead**  
  * *Mitigation:* Implement automated DuckDB table partitioning and 90-day cold storage archival policies.
* **Risk 2: Accidental Exposure of Unmasked PII in Raw Tables**  
  * *Mitigation:* Enforce strict RBAC (Role-Based Access Control), restricting raw table access exclusively to Security & Data Engineering admins.
* **Risk 3: Unchecked bad rows propagating downstream**  
  * *Mitigation:* Build strict materialization guards in `stg_*` tables where rows with `dq_status = 'FAILED'` are filtered out from `mart_*` views.

---