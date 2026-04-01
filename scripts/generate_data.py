"""
generate_data.py
────────────────
Phase 2: Synthetic Data Generation
Generates realistic retail/e-commerce datasets for all 7 entities.
Outputs CSV files to data/raw/ — these simulate source system extracts.

Run:
    python scripts/generate_data.py
    python scripts/generate_data.py --customers 10000 --orders 100000
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
from loguru import logger

# ── Project root on sys.path ───────────────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.config_loader import load_config, get_path

fake = Faker("en_US")

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _seed(cfg: dict) -> None:
    seed = cfg["data_generation"]["seed"]
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def _rand_date(start: str, end: str) -> datetime:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    delta = (e - s).days
    return s + timedelta(days=random.randint(0, delta))


def _save(df: pd.DataFrame, name: str, raw_path: Path) -> None:
    out = raw_path / f"{name}.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved {name}.csv | rows={len(df):,} | path={out}")


# ══════════════════════════════════════════════════════════════════════════════
# Generator functions — one per entity
# ══════════════════════════════════════════════════════════════════════════════

def generate_customers(n: int, cfg: dict) -> pd.DataFrame:
    dr = cfg["data_generation"]["date_range"]
    segments = ["bronze", "silver", "gold", "platinum"]
    seg_weights = [0.50, 0.28, 0.15, 0.07]

    records = []
    for i in range(1, n + 1):
        signup = _rand_date(dr["start"], dr["end"])
        churned = random.random() < 0.18   # 18% churn rate
        records.append({
            "customer_id":      f"CUST_{i:06d}",
            "first_name":       fake.first_name(),
            "last_name":        fake.last_name(),
            "email":            fake.email(),
            "phone":            fake.phone_number(),
            "city":             fake.city(),
            "state":            fake.state_abbr(),
            "zip_code":         fake.zipcode(),
            "country":          "US",
            "signup_date":      signup.date().isoformat(),
            "segment":          random.choices(segments, seg_weights)[0],
            "age":              random.randint(18, 75),
            "gender":           random.choice(["M", "F", "Other", None]),
            "preferred_channel":random.choice(["email", "sms", "app", "none"]),
            "is_churned":       int(churned),
            "churn_date":       (signup + timedelta(days=random.randint(30, 500))).date().isoformat()
                                if churned else None,
            "created_at":       signup.isoformat(),
            "updated_at":       (signup + timedelta(days=random.randint(0, 30))).isoformat(),
        })

    df = pd.DataFrame(records)
    # Inject ~2% nulls on non-critical columns to simulate real data
    for col in ["phone", "gender", "zip_code"]:
        mask = np.random.random(len(df)) < 0.02
        df.loc[mask, col] = None
    return df


def generate_products(n: int, cfg: dict) -> pd.DataFrame:
    categories = {
        "Electronics":   ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera"],
        "Clothing":      ["T-Shirt", "Jeans", "Jacket", "Shoes", "Dress"],
        "Home & Garden": ["Sofa", "Lamp", "Rug", "Blender", "Drill"],
        "Sports":        ["Running Shoes", "Yoga Mat", "Bicycle", "Dumbbells", "Tent"],
        "Books":         ["Fiction Novel", "Textbook", "Comic", "Biography", "Cookbook"],
    }
    records = []
    for i in range(1, n + 1):
        cat   = random.choice(list(categories.keys()))
        ptype = random.choice(categories[cat])
        cost  = round(random.uniform(2.0, 800.0), 2)
        price = round(cost * random.uniform(1.2, 3.5), 2)
        records.append({
            "product_id":       f"PROD_{i:05d}",
            "product_name":     f"{fake.color_name()} {ptype}",
            "category":         cat,
            "sub_category":     ptype,
            "brand":            fake.company().split()[0],
            "sku":              fake.bothify("??-####-??").upper(),
            "unit_cost":        cost,
            "unit_price":       price,
            "margin_pct":       round((price - cost) / price * 100, 2),
            "weight_kg":        round(random.uniform(0.1, 20.0), 2),
            "is_active":        int(random.random() > 0.05),
            "launch_date":      fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "supplier_id":      f"SUP_{random.randint(1,50):03d}",
            "created_at":       fake.date_time_between(start_date="-3y", end_date="now").isoformat(),
            "updated_at":       fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
        })
    return pd.DataFrame(records)


def generate_orders(n: int, customer_ids: list, product_ids: list, cfg: dict) -> pd.DataFrame:
    dr = cfg["data_generation"]["date_range"]
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
    status_w  = [0.05, 0.10, 0.15, 0.60, 0.07, 0.03]
    channels  = ["web", "mobile_app", "in_store", "phone"]

    records = []
    for i in range(1, n + 1):
        ordered_at = _rand_date(dr["start"], dr["end"])
        num_items  = random.randint(1, 6)
        items      = random.choices(product_ids, k=num_items)
        status     = random.choices(statuses, status_w)[0]
        # Inject some anomalous orders (large amounts) for anomaly detection use case
        is_anomaly = random.random() < 0.02
        subtotal   = round(random.uniform(500, 15000), 2) if is_anomaly \
                     else round(random.uniform(10, 800), 2)
        discount   = round(subtotal * random.uniform(0, 0.25), 2)
        tax        = round((subtotal - discount) * 0.08, 2)
        total      = round(subtotal - discount + tax, 2)

        records.append({
            "order_id":         f"ORD_{i:07d}",
            "customer_id":      random.choice(customer_ids),
            "order_date":       ordered_at.date().isoformat(),
            "order_timestamp":  ordered_at.isoformat(),
            "status":           status,
            "channel":          random.choice(channels),
            "num_items":        num_items,
            "product_ids":      "|".join(items),          # pipe-delimited for raw realism
            "subtotal":         subtotal,
            "discount_amount":  discount,
            "tax_amount":       tax,
            "total_amount":     total,
            "currency":         "USD",
            "promo_code":       fake.bothify("SAVE##") if random.random() < 0.3 else None,
            "is_anomaly":       int(is_anomaly),
            "created_at":       ordered_at.isoformat(),
            "updated_at":       (ordered_at + timedelta(hours=random.randint(0, 48))).isoformat(),
        })
    return pd.DataFrame(records)


def generate_payments(orders_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    methods  = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
    method_w = [0.40, 0.25, 0.15, 0.12, 0.08]
    statuses = ["success", "failed", "refunded", "pending"]
    status_w = [0.88, 0.05, 0.05, 0.02]

    records = []
    for _, row in orders_df.iterrows():
        paid_at = datetime.fromisoformat(row["order_timestamp"]) + timedelta(minutes=random.randint(1, 30))
        status  = random.choices(statuses, status_w)[0]
        records.append({
            "payment_id":       f"PAY_{row['order_id']}",
            "order_id":         row["order_id"],
            "customer_id":      row["customer_id"],
            "payment_method":   random.choices(methods, method_w)[0],
            "payment_status":   status,
            "amount":           row["total_amount"],
            "currency":         "USD",
            "gateway":          random.choice(["stripe", "braintree", "adyen"]),
            "transaction_id":   fake.uuid4(),
            "failure_reason":   random.choice(["insufficient_funds", "card_declined", "timeout"])
                                if status == "failed" else None,
            "paid_at":          paid_at.isoformat() if status == "success" else None,
            "created_at":       paid_at.isoformat(),
            "updated_at":       (paid_at + timedelta(minutes=random.randint(0, 60))).isoformat(),
        })
    return pd.DataFrame(records)


def generate_inventory(product_ids: list, cfg: dict) -> pd.DataFrame:
    warehouses = ["WH_EAST", "WH_WEST", "WH_CENTRAL", "WH_SOUTH"]
    records = []
    idx = 1
    for pid in product_ids:
        for wh in random.sample(warehouses, k=random.randint(1, 4)):
            qty_on_hand = random.randint(0, 500)
            records.append({
                "inventory_id":     f"INV_{idx:07d}",
                "product_id":       pid,
                "warehouse_id":     wh,
                "quantity_on_hand": qty_on_hand,
                "quantity_reserved":random.randint(0, max(0, qty_on_hand - 10)),
                "reorder_point":    random.randint(10, 50),
                "reorder_quantity": random.randint(50, 200),
                "unit_cost":        round(random.uniform(2, 800), 2),
                "last_restocked":   fake.date_between(start_date="-180d", end_date="today").isoformat(),
                "created_at":       fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
                "updated_at":       fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
            })
            idx += 1
    return pd.DataFrame(records)


def generate_clickstream(n: int, customer_ids: list, product_ids: list, cfg: dict) -> pd.DataFrame:
    dr = cfg["data_generation"]["date_range"]
    events    = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
                 "checkout_start", "purchase", "search", "wishlist_add"]
    event_w   = [0.35, 0.25, 0.12, 0.05, 0.08, 0.06, 0.06, 0.03]
    devices   = ["desktop", "mobile", "tablet"]
    device_w  = [0.45, 0.45, 0.10]
    browsers  = ["Chrome", "Safari", "Firefox", "Edge"]

    records = []
    for i in range(1, n + 1):
        ts = _rand_date(dr["start"], dr["end"]) + timedelta(
            hours=random.randint(0, 23), minutes=random.randint(0, 59)
        )
        event = random.choices(events, event_w)[0]
        records.append({
            "event_id":         f"EVT_{i:09d}",
            "session_id":       fake.uuid4()[:16],
            "customer_id":      random.choice(customer_ids) if random.random() > 0.3 else None,
            "product_id":       random.choice(product_ids)
                                if event in ["product_view", "add_to_cart", "wishlist_add"] else None,
            "event_type":       event,
            "page_url":         f"/{random.choice(['home','category','product','cart','checkout'])}/"
                                + fake.slug(),
            "referrer":         random.choice(["google.com", "facebook.com", "email", "direct", None]),
            "device_type":      random.choices(devices, device_w)[0],
            "browser":          random.choice(browsers),
            "session_duration_s": random.randint(5, 1800),
            "event_timestamp":  ts.isoformat(),
            "created_at":       ts.isoformat(),
        })
    return pd.DataFrame(records)


def generate_shipments(orders_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    carriers  = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"]
    statuses  = ["processing", "shipped", "in_transit", "delivered", "delayed", "lost"]
    status_w  = [0.05, 0.10, 0.15, 0.62, 0.07, 0.01]

    delivered_orders = orders_df[
        orders_df["status"].isin(["shipped", "delivered", "returned"])
    ].copy()

    records = []
    for i, (_, row) in enumerate(delivered_orders.iterrows(), start=1):
        order_ts    = datetime.fromisoformat(row["order_timestamp"])
        shipped_at  = order_ts + timedelta(hours=random.randint(12, 72))
        est_days    = random.randint(2, 10)
        est_deliver = shipped_at + timedelta(days=est_days)
        status      = random.choices(statuses, status_w)[0]
        actual_days = est_days + random.randint(-1, 5)
        actual_deliver = shipped_at + timedelta(days=max(1, actual_days))
        is_delayed  = int(actual_deliver > est_deliver and status not in ["processing", "shipped"])

        records.append({
            "shipment_id":          f"SHIP_{i:07d}",
            "order_id":             row["order_id"],
            "customer_id":          row["customer_id"],
            "carrier":              random.choice(carriers),
            "tracking_number":      fake.bothify("1Z###############"),
            "status":               status,
            "origin_warehouse":     random.choice(["WH_EAST", "WH_WEST", "WH_CENTRAL", "WH_SOUTH"]),
            "destination_zip":      fake.zipcode(),
            "shipped_at":           shipped_at.isoformat(),
            "estimated_delivery":   est_deliver.date().isoformat(),
            "actual_delivery":      actual_deliver.date().isoformat()
                                    if status == "delivered" else None,
            "is_delayed":           is_delayed,
            "delay_days":           max(0, actual_days - est_days) if is_delayed else 0,
            "weight_kg":            round(random.uniform(0.1, 30.0), 2),
            "shipping_cost":        round(random.uniform(3.99, 49.99), 2),
            "created_at":           shipped_at.isoformat(),
            "updated_at":           actual_deliver.isoformat() if status == "delivered"
                                    else shipped_at.isoformat(),
        })
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════════════════════
# Main entrypoint
# ══════════════════════════════════════════════════════════════════════════════

def main(overrides: dict | None = None) -> None:
    cfg = load_config()
    dg  = cfg["data_generation"]

    # Apply CLI overrides
    if overrides:
        dg.update({k: v for k, v in overrides.items() if v is not None})

    _seed(cfg)

    raw_path = get_path("raw")
    raw_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("NexCart Lakehouse — Phase 2: Synthetic Data Generation")
    logger.info("=" * 60)

    # ── Customers ─────────────────────────────────────────────────────────────
    customers_df = generate_customers(dg["num_customers"], cfg)
    _save(customers_df, "customers", raw_path)
    customer_ids = customers_df["customer_id"].tolist()

    # ── Products ──────────────────────────────────────────────────────────────
    products_df = generate_products(dg["num_products"], cfg)
    _save(products_df, "products", raw_path)
    product_ids = products_df["product_id"].tolist()

    # ── Orders ────────────────────────────────────────────────────────────────
    orders_df = generate_orders(dg["num_orders"], customer_ids, product_ids, cfg)
    _save(orders_df, "orders", raw_path)

    # ── Payments (derived from orders) ────────────────────────────────────────
    payments_df = generate_payments(orders_df, cfg)
    _save(payments_df, "payments", raw_path)

    # ── Inventory ─────────────────────────────────────────────────────────────
    inventory_df = generate_inventory(product_ids, cfg)
    _save(inventory_df, "inventory", raw_path)

    # ── Clickstream ───────────────────────────────────────────────────────────
    clickstream_df = generate_clickstream(dg["num_clickstream"], customer_ids, product_ids, cfg)
    _save(clickstream_df, "clickstream", raw_path)

    # ── Shipments (derived from orders) ───────────────────────────────────────
    shipments_df = generate_shipments(orders_df, cfg)
    _save(shipments_df, "shipments", raw_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Data generation complete. Summary:")
    for name, df in [
        ("customers", customers_df), ("products", products_df),
        ("orders", orders_df), ("payments", payments_df),
        ("inventory", inventory_df), ("clickstream", clickstream_df),
        ("shipments", shipments_df),
    ]:
        logger.info(f"  {name:<15} {len(df):>8,} rows  |  {df.shape[1]} cols")
    logger.info(f"Output → {raw_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NexCart synthetic data generator")
    parser.add_argument("--customers",   type=int, help="Override num_customers")
    parser.add_argument("--products",    type=int, help="Override num_products")
    parser.add_argument("--orders",      type=int, help="Override num_orders")
    parser.add_argument("--clickstream", type=int, help="Override num_clickstream")
    args = parser.parse_args()

    main({
        "num_customers":   args.customers,
        "num_products":    args.products,
        "num_orders":      args.orders,
        "num_clickstream": args.clickstream,
    })
