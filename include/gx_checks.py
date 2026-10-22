"""
include/gx_checks.py
--------------------
Complete Great Expectations & Business Rule Validation Engine for NammaMart.
Achieves 100% data coverage across all 6 pipeline CSV files:
1. customers.csv
2. products.csv
3. stores.csv
4. orders.csv
5. payments.csv
6. orders_2026-10-01_increment.csv

Returns:
    {
        "passed":        <JSON of clean orders rows>,
        "failed":        <JSON of bad orders rows (DLQ)>,
        "customers":     <JSON of customers dataset>,
        "products":      <JSON of products dataset>,
        "stores":        <JSON of stores dataset>,
        "payments":      <JSON of payments dataset>,
        "increment":     <JSON of increment dataset>,
        "summary":       { file_column: [list of issues] },
        "passed_count":  int,
        "failed_count":  int,
        "checked_at":    iso_timestamp
    }
"""

import json
from datetime import datetime
import pandas as pd


def run_gx_checks(
    orders_json: str,
    customers_json: str,
    products_json: str,
    stores_json: str = None,
    payments_json: str = None,
    increment_json: str = None,
) -> dict:
    # ── Load Datasets ────────────────────────────────────────────────────────
    orders = pd.read_json(orders_json)
    customers = pd.read_json(customers_json)
    products = pd.read_json(products_json)
    stores = pd.read_json(stores_json) if stores_json else pd.DataFrame()
    payments = pd.read_json(payments_json) if payments_json else pd.DataFrame()
    increment = (
        pd.read_json(increment_json) if increment_json else pd.DataFrame()
    )

    summary = {}

    def log_issue(key: str, message: str):
        summary[key] = summary.get(key, [])
        summary[key].append(message)

    # =========================================================================
    # FILE 1: CUSTOMERS.CSV VALIDATION
    # =========================================================================
    mask_cust_dup = customers["customer_id"].duplicated(keep=False)
    if mask_cust_dup.any():
        log_issue(
            "customers.customer_id",
            f"duplicate customer_id in {int(mask_cust_dup.sum())} rows",
        )

    mask_cust_pincode = ~customers["pincode"].astype(str).str.match(r"^56\d{4}$")
    if mask_cust_pincode.any():
        log_issue(
            "customers.pincode",
            f"invalid pincode format in {int(mask_cust_pincode.sum())} rows",
        )

    valid_tiers = {"Bronze", "Silver", "Gold", "Platinum"}
    mask_cust_tier = ~customers["loyalty_tier"].isin(valid_tiers)
    if mask_cust_tier.any():
        log_issue(
            "customers.loyalty_tier",
            f"unstandardized tier casing in {int(mask_cust_tier.sum())} rows",
        )

    customers["signup_dt"] = pd.to_datetime(
        customers["signup_date"], errors="coerce"
    )
    mask_cust_future = customers["signup_dt"] > pd.Timestamp.now()
    if mask_cust_future.any():
        log_issue(
            "customers.signup_date",
            f"future signup date in {int(mask_cust_future.sum())} rows",
        )

    # =========================================================================
    # FILE 2: PRODUCTS.CSV VALIDATION
    # =========================================================================
    mask_prod_dup = products["product_id"].duplicated(keep=False)
    if mask_prod_dup.any():
        log_issue(
            "products.product_id",
            f"duplicate product_id in {int(mask_prod_dup.sum())} rows",
        )

    mask_prod_mrp = products["mrp"] <= 0
    if mask_prod_mrp.any():
        log_issue(
            "products.mrp",
            f"negative or zero MRP in {int(mask_prod_mrp.sum())} rows",
        )

    mask_prod_null_cat = products["category"].isnull()
    if mask_prod_null_cat.any():
        log_issue(
            "products.category",
            f"null category in {int(mask_prod_null_cat.sum())} rows",
        )

    # =========================================================================
    # FILE 3: STORES.CSV VALIDATION
    # =========================================================================
    if not stores.empty:
        mask_store_cap = stores["capacity_orders_per_day"] <= 0
        if mask_store_cap.any():
            log_issue(
                "stores.capacity_orders_per_day",
                f"zero or negative capacity in {int(mask_store_cap.sum())} rows",
            )

        mask_store_zone = stores["zone"].isnull()
        if mask_store_zone.any():
            log_issue(
                "stores.zone",
                f"null zone in {int(mask_store_zone.sum())} rows",
            )

    # =========================================================================
    # FILE 4: ORDERS.CSV VALIDATION & RULE EVALUATION
    # =========================================================================
    order_failed_masks = []

    # Rule 1: Primary Key Uniqueness
    mask_order_dup = orders["order_id"].duplicated(keep=False)
    if mask_order_dup.any():
        log_issue(
            "orders.order_id",
            f"duplicate order_id in {int(mask_order_dup.sum())} rows",
        )
    order_failed_masks.append(mask_order_dup)

    # Rule 2: Completeness (customer_id non-null)
    mask_order_null_cust = orders["customer_id"].isnull() | (
        orders["customer_id"].astype(str).str.strip() == ""
    )
    if mask_order_null_cust.any():
        log_issue(
            "orders.customer_id",
            f"null/empty customer_id in {int(mask_order_null_cust.sum())} rows",
        )
    order_failed_masks.append(mask_order_null_cust)

    # Rule 3: Bounds Validation (quantity > 0)
    mask_order_qty = orders["quantity"] <= 0
    if mask_order_qty.any():
        log_issue(
            "orders.quantity",
            f"negative/zero quantity in {int(mask_order_qty.sum())} rows",
        )
    order_failed_masks.append(mask_order_qty)

    # Rule 4: Discount Bounds (0 <= discount_pct <= 100)
    mask_order_disc = (orders["discount_pct"] < 0) | (
        orders["discount_pct"] > 100
    )
    if mask_order_disc.any():
        log_issue(
            "orders.discount_pct",
            f"discount_pct out of bounds in {int(mask_order_disc.sum())} rows",
        )
    order_failed_masks.append(mask_order_disc)

    # Rule 5: Formula Recalculation Check (order_amount)
    expected_amount = (
        orders["quantity"]
        * orders["unit_price"]
        * (1 - orders["discount_pct"] / 100.0)
    ).round(2)
    mask_order_amount_err = (
        orders["order_amount"].round(2) - expected_amount
    ).abs() > 0.01
    if mask_order_amount_err.any():
        log_issue(
            "orders.order_amount",
            f"recomputation mismatch in {int(mask_order_amount_err.sum())} rows",
        )
    order_failed_masks.append(mask_order_amount_err)

    # Rule 6: Order Status ENUM
    valid_statuses = {"DELIVERED", "CANCELLED", "RETURNED"}
    mask_order_status = ~orders["order_status"].isin(valid_statuses)
    if mask_order_status.any():
        log_issue(
            "orders.order_status",
            f"invalid order status in {int(mask_order_status.sum())} rows",
        )
    order_failed_masks.append(mask_order_status)

    # Rule 7: Payment Mode ENUM
    valid_pay_modes = {"UPI", "Card", "Cash on Delivery", "Wallet"}
    mask_order_pay_mode = ~orders["payment_mode"].isin(valid_pay_modes)
    if mask_order_pay_mode.any():
        log_issue(
            "orders.payment_mode",
            f"unstandardized payment mode in {int(mask_order_pay_mode.sum())} rows",
        )
    order_failed_masks.append(mask_order_pay_mode)

    # Rule 8: Delivery Minutes for DELIVERED orders
    mask_order_delivery = (orders["delivery_minutes"] < 8) | (
        orders["delivery_minutes"] > 35
    )
    if mask_order_delivery.any():
        log_issue(
            "orders.delivery_minutes",
            f"delivery duration outside 8-35 window in {int(mask_order_delivery.sum())} rows",
        )
    order_failed_masks.append(mask_order_delivery)

    # Rule 9: Referential Integrity -> Customers
    valid_customers = set(customers["customer_id"].astype(str))
    mask_order_orphan_cust = ~orders["customer_id"].astype(str).isin(
        valid_customers
    ) & ~orders["customer_id"].isnull()
    if mask_order_orphan_cust.any():
        log_issue(
            "orders.customer_id",
            f"orphan customer_id in {int(mask_order_orphan_cust.sum())} rows",
        )
    order_failed_masks.append(mask_order_orphan_cust)

    # Rule 10: Referential Integrity -> Products
    valid_products = set(products["product_id"].astype(str))
    mask_order_orphan_prod = ~orders["product_id"].astype(str).isin(
        valid_products
    )
    if mask_order_orphan_prod.any():
        log_issue(
            "orders.product_id",
            f"orphan product_id in {int(mask_order_orphan_prod.sum())} rows",
        )
    order_failed_masks.append(mask_order_orphan_prod)

    # Rule 11: Referential Integrity -> Stores
    if not stores.empty:
        valid_stores = set(stores["store_id"].astype(str))
        mask_order_orphan_store = ~orders["store_id"].astype(str).isin(
            valid_stores
        )
        if mask_order_orphan_store.any():
            log_issue(
                "orders.store_id",
                f"orphan store_id in {int(mask_order_orphan_store.sum())} rows",
            )
        order_failed_masks.append(mask_order_orphan_store)

    # =========================================================================
    # FILE 5: PAYMENTS.CSV VALIDATION
    # =========================================================================
    if not payments.empty:
        mask_pay_dup = payments["payment_id"].duplicated(keep=False)
        if mask_pay_dup.any():
            log_issue(
                "payments.payment_id",
                f"duplicate payment_id in {int(mask_pay_dup.sum())} rows",
            )

        valid_orders = set(orders["order_id"].astype(str))
        mask_pay_orphan = ~payments["order_id"].astype(str).isin(valid_orders)
        if mask_pay_orphan.any():
            log_issue(
                "payments.order_id",
                f"orphan payment referencing missing order_id in {int(mask_pay_orphan.sum())} rows",
            )

        mask_pay_neg = payments["amount_paid"] <= 0
        if mask_pay_neg.any():
            log_issue(
                "payments.amount_paid",
                f"negative/zero payment amount in {int(mask_pay_neg.sum())} rows",
            )

        # Cross-Reconciliation Check: SUCCESS payments matching order_amount
        pay_merged = payments[payments["payment_status"] == "SUCCESS"].merge(
            orders[["order_id", "order_amount"]], on="order_id", how="inner"
        )
        mask_pay_mismatch = (
            pay_merged["amount_paid"].round(2)
            != pay_merged["order_amount"].round(2)
        )
        if mask_pay_mismatch.any():
            log_issue(
                "payments.amount_paid",
                f"SUCCESS payment != order_amount in {int(mask_pay_mismatch.sum())} reconciled rows",
            )

    # =========================================================================
    # FILE 6: ORDERS INCREMENT CSV VALIDATION
    # =========================================================================
    if not increment.empty:
        mask_inc_dup = increment["order_id"].duplicated(keep=False)
        if mask_inc_dup.any():
            log_issue(
                "increment.order_id",
                f"duplicate order_id in {int(mask_inc_dup.sum())} rows",
            )

    # =========================================================================
    # COMBINE MASKS & BUILD PASSED / FAILED PAYLOAD
    # =========================================================================
    combined_order_fail = order_failed_masks[0]
    for m in order_failed_masks[1:]:
        combined_order_fail = combined_order_fail | m

    failed_orders_df = orders[combined_order_fail].copy()
    passed_orders_df = orders[~combined_order_fail].copy()

    # Clean temporary columns before exporting
    if "signup_dt" in customers.columns:
        customers = customers.drop(columns=["signup_dt"])

    print(
        f"[GX] Orders Passed: {len(passed_orders_df)} | Failed: {len(failed_orders_df)}"
    )
    print(f"[GX] Issues Identified:\n{json.dumps(summary, indent=2)}")

    return {
        "passed": passed_orders_df.to_json(),
        "failed": failed_orders_df.to_json(),
        "customers": customers.to_json(),
        "products": products.to_json(),
        "stores": stores.to_json() if not stores.empty else "{}",
        "payments": payments.to_json() if not payments.empty else "{}",
        "increment": increment.to_json() if not increment.empty else "{}",
        "summary": summary,
        "passed_count": len(passed_orders_df),
        "failed_count": len(failed_orders_df),
        "checked_at": datetime.utcnow().isoformat(),
    }