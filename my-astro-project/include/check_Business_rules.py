import duckdb
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data" / "nammamart"
DATA_PATH = str(DATA_DIR).replace("\\", "/")

con = duckdb.connect()

print("=== RULE 1: Recompute order_amount ===")
r1 = con.execute(f"""
    SELECT 
        COUNT(*) AS total_mismatches,
        MIN(ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2))) AS min_diff,
        MAX(ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2))) AS max_diff
    FROM '{DATA_PATH}/orders.csv'
    WHERE ROUND(order_amount, 2) != ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2);
""").fetchall()
print("Order Amount Mismatches:", r1)

print("\n=== RULE 2: Valid order_status ===")
r2 = con.execute(f"""
    SELECT order_status, COUNT(*) AS cnt
    FROM '{DATA_PATH}/orders.csv'
    GROUP BY order_status;
""").fetchall()
print("Order Status Distribution:", r2)

print("\n=== RULE 3: Valid payment_mode ===")
r3 = con.execute(f"""
    SELECT payment_mode, COUNT(*) AS cnt
    FROM '{DATA_PATH}/orders.csv'
    GROUP BY payment_mode;
""").fetchall()
print("Order Payment Modes:", r3)

print("\n=== RULE 4: Delivery Minutes for DELIVERED Orders & Outliers ===")
r4 = con.execute(f"""
    SELECT 
        COUNT(CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 1 END) AS delivered_null_delivery_min,
        COUNT(CASE WHEN delivery_minutes < 8 OR delivery_minutes > 35 THEN 1 END) AS delivery_outliers_outside_8_35
    FROM '{DATA_PATH}/orders.csv';
""").fetchall()
print("Delivery Minutes Audit:", r4)

print("\n=== RULE 5: Payment Amount = Order Amount (for SUCCESS payments) ===")
r5 = con.execute(f"""
    SELECT 
        COUNT(*) AS amount_mismatches
    FROM '{DATA_PATH}/payments.csv' p
    JOIN '{DATA_PATH}/orders.csv' o ON p.order_id = o.order_id
    WHERE p.payment_status = 'SUCCESS'
      AND ROUND(p.amount_paid, 2) != ROUND(o.order_amount, 2);
""").fetchall()
print("Payment Amount Mismatches:", r5)