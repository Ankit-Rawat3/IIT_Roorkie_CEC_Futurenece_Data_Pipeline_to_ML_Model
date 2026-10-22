import duckdb

# Path to your dataset folder
DATA_DIR = "E:\\git\\CyberarkLAB\\Demo\\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\\my-astro-project\\include\\data\\nammamart"

con = duckdb.connect()

print("--- 1. Checking Orders -> Customers ---")
res1 = con.execute(f"""
    SELECT 
        COUNT(*) AS orphan_rows,
        COUNT(DISTINCT o.customer_id) AS distinct_missing_keys
    FROM '{DATA_DIR}/orders.csv' o
    LEFT JOIN '{DATA_DIR}/customers.csv' c 
        ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL AND o.customer_id IS NOT NULL;
""").fetchall()
print("Orphan Rows, Missing Keys:", res1)

print("\n--- 2. Checking Orders -> Products ---")
res2 = con.execute(f"""
    SELECT 
        COUNT(*) AS orphan_rows,
        COUNT(DISTINCT o.product_id) AS distinct_missing_keys
    FROM '{DATA_DIR}/orders.csv' o
    LEFT JOIN '{DATA_DIR}/products.csv' p 
        ON o.product_id = p.product_id
    WHERE p.product_id IS NULL AND o.product_id IS NOT NULL;
""").fetchall()
print("Orphan Rows, Missing Keys:", res2)

print("\n--- 3. Checking Orders -> Stores ---")
res3 = con.execute(f"""
    SELECT 
        COUNT(*) AS orphan_rows,
        COUNT(DISTINCT o.store_id) AS distinct_missing_keys
    FROM '{DATA_DIR}/orders.csv' o
    LEFT JOIN '{DATA_DIR}/stores.csv' s 
        ON o.store_id = s.store_id
    WHERE s.store_id IS NULL AND o.store_id IS NOT NULL;
""").fetchall()
print("Orphan Rows, Missing Keys:", res3)

print("\n--- 4. Checking Payments -> Orders ---")
res4 = con.execute(f"""
    SELECT 
        COUNT(*) AS orphan_rows,
        COUNT(DISTINCT p.order_id) AS distinct_missing_keys
    FROM '{DATA_DIR}/payments.csv' p
    LEFT JOIN '{DATA_DIR}/orders.csv' o 
        ON p.order_id = o.order_id
    WHERE o.order_id IS NULL AND p.order_id IS NOT NULL;
""").fetchall()
print("Orphan Rows, Missing Keys:", res4)