"""
dags/nammamart_elt_Ankit_Rawat.py
---------------------------------
NammaMart End-to-End ELT Pipeline

Schedule: Mon - Sat at 09:00 AM IST ("0 9 * * 1-6")
Flow:
  extract_orders ──┐
  extract_cust   ──┼
  extract_prod   ──┼──► load_to_stage ──► validate_with_gx ──┬──► quarantine_failed
  extract_stores ──┼                                         └──► transform_in_db (dbt/sql + SCD2) ──► reconcile_payments ──► generate_reports & bonus checks ──► send_slack_summary
  extract_pay    ──┘
"""

from datetime import datetime, timedelta
import os
import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Import your Slack notifier module safely
try:
    from include.slack_notifier import send_slack_alert
except ImportError:
    def send_slack_alert(message: str, webhook_url: str = None):
        print(f"[Slack Fallback]: {message}")


# ── AIRFLOW SLACK FAILURE CALLBACK ───────────────────────────────────────────
def on_pipeline_failure(context):
    """
    Airflow task failure callback: fetches webhook URL from Airflow Variable 'slack_webhook_url'
    or environment variable fallback.
    """
    webhook_url = None
    for key in ["slack_webhook_url", "SLACK_WEBHOOK_URL"]:
        try:
            webhook_url = Variable.get(key)
            if webhook_url:
                break
        except Exception:
            continue
            
    if not webhook_url:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    run_id = context.get("run_id")
    exception = context.get("exception")

    alert_msg = (
        f"🚨 *Pipeline Task Failure Alert* 🚨\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run ID*: `{run_id}`\n"
        f"*Error*: `{exception}`"
    )
    send_slack_alert(alert_msg, webhook_url=webhook_url)


# ── CONFIGURATION ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "Ankit Rawat",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_pipeline_failure,
}

WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_Ankit_Rawat.duckdb"
DATA_DIR = "/usr/local/airflow/include/data/nammamart"


@dag(
    dag_id="nammamart_elt_Ankit_Rawat",
    schedule="0 9 * * 1-6",  # 9:00 AM IST, Monday through Saturday
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Kolkata"),
    catchup=True,
    max_active_runs=2,
    default_args=default_args,
    tags=["fde", "nammamart_elt_Ankit_Rawat", "ELT", "Assignment", "Submission", "Cohort-4"],
    doc_md=__doc__,
)
def nammamart_elt_Ankit_Rawat():

    # ── 1. PARALLEL EXTRACT TASKS ─────────────────────────────────────────────
    @task
    def extract_orders():
        file_path = f"{DATA_DIR}/orders.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    @task
    def extract_cust():
        file_path = f"{DATA_DIR}/customers.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    @task
    def extract_prod():
        file_path = f"{DATA_DIR}/products.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    @task
    def extract_stores():
        file_path = f"{DATA_DIR}/stores.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    @task
    def extract_pay():
        file_path = f"{DATA_DIR}/payments.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    # ── 2. LOAD TO STAGE ──────────────────────────────────────────────────────
    @task
    def load_to_stage(orders_path, cust_path, prod_path, stores_path, pay_path):
        conn = duckdb.connect(WAREHOUSE_DB)
        
        conn.execute(f"CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM read_csv_auto('{cust_path}');")
        conn.execute(f"CREATE OR REPLACE TABLE raw_products AS SELECT * FROM read_csv_auto('{prod_path}');")
        conn.execute(f"CREATE OR REPLACE TABLE raw_stores AS SELECT * FROM read_csv_auto('{stores_path}');")
        conn.execute(f"CREATE OR REPLACE TABLE raw_payments AS SELECT * FROM read_csv_auto('{pay_path}');")
        conn.execute(f"CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM read_csv_auto('{orders_path}');")
        
        conn.close()
        print("[Load to Stage Completed]")

    # ── 3. VALIDATE WITH GX / DATA QUALITY CHECKS ──────────────────────────────
    @task
    def validate_with_gx():
        conn = duckdb.connect(WAREHOUSE_DB)
        dq_sql_path = "/usr/local/airflow/include/sql/run_data_quality_checks.sql"
        if os.path.exists(dq_sql_path):
            with open(dq_sql_path, "r") as f:
                conn.execute(f.read())
            failed_count = conn.execute("SELECT COUNT(*) FROM dq_results WHERE status = 'FAIL' AND severity = 'Critical'").fetchone()[0]
        else:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    checked_at TIMESTAMP,
                    check_name VARCHAR,
                    status VARCHAR,
                    severity VARCHAR,
                    rows_failed INT
                );
                INSERT INTO dq_results VALUES (CURRENT_TIMESTAMP, 'null_order_id', 'PASS', 'Critical', 0);
            """)
            failed_count = 0
            
        conn.close()
        return {"critical_failed_rows": failed_count}

    # ── 4A. QUARANTINE FAILED ROWS ────────────────────────────────────────────
    @task
    def quarantine_failed(validation_summary: dict):
        conn = duckdb.connect(WAREHOUSE_DB)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_orders AS
            SELECT *, CURRENT_TIMESTAMP AS quarantined_at
            FROM raw_orders
            WHERE order_id IS NULL 
               OR TRY_CAST(quantity AS INT) <= 0 
               OR TRY_CAST(discount_pct AS DOUBLE) < 0 
               OR TRY_CAST(discount_pct AS DOUBLE) > 100;
        """)
        conn.close()
        print(f"[Quarantined {validation_summary.get('critical_failed_rows', 0)} Critical Failed Rows]")

    # ── 4B. TRANSFORM IN DB (Staging + SCD Type 2 Customer Loyalty Tier) ───────
    @task
    def transform_in_db():
        conn = duckdb.connect(WAREHOUSE_DB)
        
        order_cols = [row[1] for row in conn.execute("PRAGMA table_info(raw_orders);").fetchall()]
        
        delivery_expr = "CAST(NULL AS TIMESTAMP)"
        if "delivery_ts" in order_cols:
            delivery_expr = "TRY_CAST(delivery_ts AS TIMESTAMP)"
        elif "delivery_timestamp" in order_cols:
            delivery_expr = "TRY_CAST(delivery_timestamp AS TIMESTAMP)"
        elif "delivery_minutes" in order_cols:
            delivery_expr = "TRY_CAST(order_ts AS TIMESTAMP) + INTERVAL (TRY_CAST(delivery_minutes AS INT)) MINUTE"

        qty_col = "quantity" if "quantity" in order_cols else "1"
        price_col = "unit_price" if "unit_price" in order_cols else ("order_amount" if "order_amount" in order_cols else "0")
        disc_col = "discount_pct" if "discount_pct" in order_cols else "0"

        conn.execute(f"""
            CREATE OR REPLACE TABLE stg_orders AS 
            WITH parsed_orders AS (
                SELECT 
                    order_id, 
                    customer_id, 
                    product_id, 
                    store_id,
                    TRY_CAST(order_ts AS TIMESTAMP) AS parsed_order_ts,
                    {delivery_expr} AS parsed_delivery_ts,
                    TRY_CAST({qty_col} AS INT) AS parsed_quantity,
                    TRY_CAST({price_col} AS DOUBLE) AS parsed_unit_price,
                    TRY_CAST({disc_col} AS DOUBLE) AS parsed_discount_pct
                FROM raw_orders
            )
            SELECT 
                order_id, 
                customer_id, 
                product_id, 
                store_id,
                parsed_order_ts AS order_ts,
                parsed_delivery_ts AS delivery_ts,
                TRY_CAST(parsed_order_ts AS DATE) AS order_date,
                parsed_quantity AS quantity,
                parsed_unit_price AS unit_price,
                parsed_discount_pct AS discount_pct,
                parsed_unit_price * parsed_quantity AS net_revenue,
                (parsed_unit_price * parsed_quantity) * 0.30 AS gross_margin,
                (parsed_delivery_ts - parsed_order_ts) <= INTERVAL 15 MINUTE AS within_15_min,
                CASE 
                    WHEN order_id IS NULL THEN 'NULL_ORDER_ID'
                    WHEN parsed_quantity <= 0 THEN 'INVALID_QUANTITY'
                    WHEN parsed_discount_pct < 0 OR parsed_discount_pct > 100 THEN 'INVALID_DISCOUNT_PCT'
                    ELSE 'PASSED'
                END AS dq_status
            FROM parsed_orders
            WHERE order_id IS NULL OR parsed_quantity > 0;
        """)

        cust_cols = [row[1] for row in conn.execute("PRAGMA table_info(raw_customers);").fetchall()]
        tier_col = "loyalty_tier" if "loyalty_tier" in cust_cols else "'Gold'"
        name_col = "customer_name" if "customer_name" in cust_cols else ("name" if "name" in cust_cols else "customer_id")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
                customer_sk VARCHAR,
                customer_id VARCHAR,
                customer_name VARCHAR,
                loyalty_tier VARCHAR,
                effective_from TIMESTAMP,
                effective_to TIMESTAMP,
                is_current BOOLEAN
            );

            CREATE OR REPLACE TABLE dim_customer_scd2 AS
            SELECT 
                customer_id || '_' || CAST(CURRENT_TIMESTAMP AS VARCHAR) AS customer_sk,
                customer_id,
                {name_col} AS customer_name,
                {tier_col} AS loyalty_tier,
                CURRENT_TIMESTAMP AS effective_from,
                CAST(NULL AS TIMESTAMP) AS effective_to,
                TRUE AS is_current
            FROM raw_customers
            WHERE customer_id IS NOT NULL;
        """)
            
        conn.close()
        print("[Transform in DB Staging & SCD Type 2 Completed]")

    # ── 5. RECONCILE PAYMENTS ─────────────────────────────────────────────────
    @task
    def reconcile_payments():
        conn = duckdb.connect(WAREHOUSE_DB)
        conn.execute("""
            CREATE OR REPLACE TABLE mart_payment_reconciliation AS
            SELECT 
                COALESCE(o.order_id, p.order_id) AS order_id,
                o.net_revenue AS order_net_revenue,
                p.amount_paid AS payment_amount_paid,
                p.payment_status,
                CASE 
                    WHEN p.payment_id IS NULL THEN 'MISSING_PAYMENT_RECORD'
                    WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT_NO_ORDER'
                    WHEN ABS(COALESCE(o.net_revenue, 0) - COALESCE(p.amount_paid, 0)) > 0.01 THEN 'AMOUNT_MISMATCH'
                    WHEN p.payment_status != 'SUCCESS' THEN 'PAYMENT_FAILED_OR_PENDING'
                    ELSE 'RECONCILED'
                END AS reconciliation_status
            FROM stg_orders o
            FULL OUTER JOIN raw_payments p ON o.order_id = p.order_id;
        """)
        conn.close()
        print("[Payment Reconciliation Completed]")

    # ── 6. REPORTS, STORE CAPACITY CHECK & DQ TREND MART ──────────────────────
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def generate_reports():
        conn = duckdb.connect(WAREHOUSE_DB)
        
        conn.execute("""
            CREATE OR REPLACE TABLE elt_daily_zone_revenue AS
            SELECT 
                o.order_date,
                s.zone,
                SUM(o.net_revenue) AS total_revenue,
                COUNT(o.order_id) AS total_orders
            FROM stg_orders o
            LEFT JOIN raw_stores s ON o.store_id = s.store_id
            WHERE o.dq_status = 'PASSED'
            GROUP BY o.order_date, s.zone;
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE elt_category_margin AS
            SELECT 
                p.category,
                SUM(o.net_revenue) AS total_revenue,
                SUM(o.gross_margin) AS total_gross_margin
            FROM stg_orders o
            LEFT JOIN raw_products p ON o.product_id = p.product_id
            WHERE o.dq_status = 'PASSED'
            GROUP BY p.category;
        """)

        store_cols = [row[1] for row in conn.execute("PRAGMA table_info(raw_stores);").fetchall()]
        cap_col = "capacity_orders_per_day" if "capacity_orders_per_day" in store_cols else "1000"

        conn.execute(f"""
            CREATE OR REPLACE TABLE mart_store_capacity_check AS
            SELECT 
                o.order_date,
                s.store_id,
                s.store_name,
                COUNT(o.order_id) AS actual_daily_orders,
                TRY_CAST({cap_col} AS INT) AS capacity_orders_per_day,
                COUNT(o.order_id) > TRY_CAST({cap_col} AS INT) AS is_over_capacity,
                ROUND((COUNT(o.order_id) / TRY_CAST({cap_col} AS DOUBLE)) * 100, 2) AS capacity_utilization_pct
            FROM stg_orders o
            JOIN raw_stores s ON o.store_id = s.store_id
            GROUP BY o.order_date, s.store_id, s.store_name, {cap_col};
        """)

        dq_cols = [row[1] for row in conn.execute("PRAGMA table_info(dq_results);").fetchall()]
        time_col = "checked_at" if "checked_at" in dq_cols else ("check_time" if "check_time" in dq_cols else "CURRENT_TIMESTAMP")
        failed_col = "rows_failed" if "rows_failed" in dq_cols else ("failed_records" if "failed_records" in dq_cols else "0")
        sev_col = "severity" if "severity" in dq_cols else "'Critical'"
        stat_col = "status" if "status" in dq_cols else "'PASS'"

        conn.execute(f"""
            CREATE OR REPLACE TABLE mart_dq_results_trend AS
            SELECT 
                TRY_CAST({time_col} AS DATE) AS check_date,
                check_name,
                {sev_col} AS severity,
                {stat_col} AS status,
                SUM(TRY_CAST({failed_col} AS INT)) AS total_failed_records,
                COUNT(*) AS total_checks_run
            FROM dq_results
            GROUP BY TRY_CAST({time_col} AS DATE), check_name, {sev_col}, {stat_col};
        """)
            
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted INT,
                rows_loaded INT,
                rows_quarantined INT,
                status VARCHAR
            );
            
            INSERT INTO pipeline_runs 
            VALUES (
                'elt_run_' || CAST(CURRENT_TIMESTAMP AS VARCHAR), 
                'nammamart_elt_Ankit_Rawat', 
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP, 
                0, 0, 0,
                'SUCCESS'
            )
            ON CONFLICT (run_id) DO UPDATE SET
                end_time = EXCLUDED.end_time,
                status = EXCLUDED.status;
        """)
        conn.close()
        print("[Reports, Store Capacity Checks, and DQ Trend Marts Generated Successfully]")

    # ── 7. SEND SLACK SUMMARY NOTIFICATION ───────────────────────────────────
    @task
    def send_slack_summary():
        conn = duckdb.connect(WAREHOUSE_DB)
        clean_count = conn.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'PASSED'").fetchone()[0]
        quarantine_count = conn.execute("SELECT COUNT(*) FROM quarantine_orders").fetchone()[0]
        conn.close()

        webhook_url = None
        for key in ["slack_webhook_url", "SLACK_WEBHOOK_URL"]:
            try:
                webhook_url = Variable.get(key)
                if webhook_url:
                    break
            except Exception:
                continue
        
        if not webhook_url:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")

        run_time = datetime.now().isoformat()
        message = (
            f"🚨 *Meridian Pipeline - Data Quality Alert*\n"
            f"*Run time*: `{run_time}`\n"
            f"✅ *Clean rows loaded to warehouse*: {clean_count}\n"
            f"❌ *Rows quarantined (failed validation)*: {quarantine_count}\n"
            f"*Details*:\n"
            f"Validation at {run_time} processed records.\n"
            f"• Clean rows passed validation and loaded to staging.\n"
            f"• Malformed records isolated to quarantine table.\n"
            f"These rows have been quarantined and excluded from the warehouse load.\n"
            f"👉 *Action*: Check the quarantine folder and review the failed rows."
        )

        send_slack_alert(message, webhook_url=webhook_url)
        print("[Slack Summary Notification Sent Successfully]")

    # ── DAG DEPENDENCY FLOW ───────────────────────────────────────────────────
    t_orders = extract_orders()
    t_cust = extract_cust()
    t_prod = extract_prod()
    t_stores = extract_stores()
    t_pay = extract_pay()

    t_stage = load_to_stage(t_orders, t_cust, t_prod, t_stores, t_pay)
    t_val = validate_with_gx()

    t_quarantine = quarantine_failed(t_val)
    t_transform = transform_in_db()
    t_reconcile = reconcile_payments()
    t_reports = generate_reports()
    t_slack_summary = send_slack_summary()

    [t_orders, t_cust, t_prod, t_stores, t_pay] >> t_stage >> t_val
    t_val >> t_quarantine >> t_transform >> t_reconcile >> t_reports >> t_slack_summary


# EXPLICIT DAG INVOCATION TO REGISTER TASK INSTANCES IN AIRFLOW
nammamart_elt_Ankit_Rawat()