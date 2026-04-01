"""
generate_data_lite.py
─────────────────────
Offline-compatible version of the data generator.
Uses ONLY: pandas, numpy, random, datetime (no Faker, no loguru needed).
Produces identical schema to generate_data.py.

Run: python3 scripts/generate_data_lite.py
"""
from __future__ import annotations

import csv
import json
import random
import string
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
import numpy as np
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
SEED            = 42
NUM_CUSTOMERS   = 5_000
NUM_PRODUCTS    = 500
NUM_ORDERS      = 50_000
NUM_CLICKSTREAM = 200_000
DATE_START      = date(2023, 1, 1)
DATE_END        = date(2024, 12, 31)
RAW_PATH        = Path(__file__).resolve().parents[1] / "data" / "raw"

random.seed(SEED)
np.random.seed(SEED)
rng = np.random.default_rng(SEED)

# ── Helpers ───────────────────────────────────────────────────────────────────
FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
               "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
               "Thomas","Karen","Charles","Lisa","Emma","Noah","Olivia","Liam","Ava",
               "Sophia","Lucas","Isabella","Mason","Mia","Ethan","Harper","Aiden","Ella"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
               "Wilson","Anderson","Taylor","Thomas","Hernandez","Moore","Martin","Lee",
               "Thompson","White","Lopez","Harris","Clark","Lewis","Robinson","Walker"]
CITIES      = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
               "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville",
               "Denver","Seattle","Nashville","Boston","Portland","Las Vegas","Memphis"]
STATES      = ["NY","CA","IL","TX","AZ","PA","FL","CO","WA","TN","MA","OR","NV","GA","OH"]
DOMAINS     = ["gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com","aol.com"]
COMPANIES   = ["Apex","Nexus","Vertex","Orbit","Zenith","Crest","Peak","Summit","Ridge","Core"]
COLORS      = ["Red","Blue","Green","Black","White","Silver","Gold","Purple","Orange","Navy"]

def rand_date(start: date = DATE_START, end: date = DATE_END) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_email(first: str, last: str) -> str:
    return f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{random.choice(DOMAINS)}"

def rand_phone() -> str:
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

def rand_zip() -> str:
    return f"{random.randint(10000,99999)}"

def rand_str(n: int) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def save(df: pd.DataFrame, name: str) -> None:
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    path = RAW_PATH / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ {name:<18} {len(df):>8,} rows  →  {path}")

# ══════════════════════════════════════════════════════════════════════════════
def gen_customers(n: int) -> pd.DataFrame:
    segments   = ["bronze","silver","gold","platinum"]
    seg_w      = [0.50, 0.28, 0.15, 0.07]
    channels   = ["email","sms","app","none"]
    genders    = ["M","F","Other",None]

    rows = []
    for i in range(1, n+1):
        fn       = random.choice(FIRST_NAMES)
        ln       = random.choice(LAST_NAMES)
        signup   = rand_date()
        churned  = random.random() < 0.18
        seg      = random.choices(segments, seg_w)[0]
        rows.append({
            "customer_id":       f"CUST_{i:06d}",
            "first_name":        fn,
            "last_name":         ln,
            "email":             rand_email(fn, ln),
            "phone":             rand_phone() if random.random() > 0.02 else None,
            "city":              random.choice(CITIES),
            "state":             random.choice(STATES),
            "zip_code":          rand_zip() if random.random() > 0.02 else None,
            "country":           "US",
            "signup_date":       signup.isoformat(),
            "segment":           seg,
            "age":               random.randint(18, 75),
            "gender":            random.choices(genders, [0.48,0.48,0.02,0.02])[0],
            "preferred_channel": random.choice(channels),
            "is_churned":        int(churned),
            "churn_date":        (signup + timedelta(days=random.randint(30,500))).isoformat()
                                 if churned else None,
            "created_at":        datetime.combine(signup, datetime.min.time()).isoformat(),
            "updated_at":        datetime.combine(
                                   signup + timedelta(days=random.randint(0,30)),
                                   datetime.min.time()).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_products(n: int) -> pd.DataFrame:
    cat_map = {
        "Electronics":   ["Laptop","Smartphone","Tablet","Headphones","Camera","Monitor","Keyboard"],
        "Clothing":      ["T-Shirt","Jeans","Jacket","Shoes","Dress","Hoodie","Sneakers"],
        "Home & Garden": ["Sofa","Lamp","Rug","Blender","Drill","Chair","Shelf"],
        "Sports":        ["Running Shoes","Yoga Mat","Bicycle","Dumbbells","Tent","Gloves"],
        "Books":         ["Fiction Novel","Textbook","Comic","Biography","Cookbook","Journal"],
    }
    cats     = list(cat_map.keys())
    rows = []
    for i in range(1, n+1):
        cat   = random.choice(cats)
        ptype = random.choice(cat_map[cat])
        cost  = round(random.uniform(2.0, 800.0), 2)
        price = round(cost * random.uniform(1.2, 3.5), 2)
        ld    = rand_date(date(2021,1,1), date(2024,12,31))
        rows.append({
            "product_id":    f"PROD_{i:05d}",
            "product_name":  f"{random.choice(COLORS)} {ptype}",
            "category":      cat,
            "sub_category":  ptype,
            "brand":         random.choice(COMPANIES),
            "sku":           f"{rand_str(2)}-{random.randint(1000,9999)}-{rand_str(2)}",
            "unit_cost":     cost,
            "unit_price":    price,
            "margin_pct":    round((price - cost) / price * 100, 2),
            "weight_kg":     round(random.uniform(0.1, 20.0), 2),
            "is_active":     int(random.random() > 0.05),
            "launch_date":   ld.isoformat(),
            "supplier_id":   f"SUP_{random.randint(1,50):03d}",
            "created_at":    datetime.combine(ld, datetime.min.time()).isoformat(),
            "updated_at":    datetime.combine(
                               ld + timedelta(days=random.randint(0,365)),
                               datetime.min.time()).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_orders(n: int, cust_ids: list, prod_ids: list) -> pd.DataFrame:
    statuses = ["pending","confirmed","shipped","delivered","cancelled","returned"]
    status_w = [0.05, 0.10, 0.15, 0.60, 0.07, 0.03]
    channels = ["web","mobile_app","in_store","phone"]
    promos   = ["SAVE10","SAVE20","FLASH15","VIP25","WELCOME5",None]

    rows = []
    for i in range(1, n+1):
        od         = rand_date()
        ot         = datetime.combine(od, datetime.min.time()) + timedelta(
                       hours=random.randint(0,23), minutes=random.randint(0,59))
        num_items  = random.randint(1, 6)
        items      = random.choices(prod_ids, k=num_items)
        status     = random.choices(statuses, status_w)[0]
        is_anomaly = random.random() < 0.02
        subtotal   = round(random.uniform(500,15000), 2) if is_anomaly \
                     else round(random.uniform(10, 800), 2)
        discount   = round(subtotal * random.uniform(0, 0.25), 2)
        tax        = round((subtotal - discount) * 0.08, 2)
        total      = round(subtotal - discount + tax, 2)
        rows.append({
            "order_id":        f"ORD_{i:07d}",
            "customer_id":     random.choice(cust_ids),
            "order_date":      od.isoformat(),
            "order_timestamp": ot.isoformat(),
            "status":          status,
            "channel":         random.choice(channels),
            "num_items":       num_items,
            "product_ids":     "|".join(items),
            "subtotal":        subtotal,
            "discount_amount": discount,
            "tax_amount":      tax,
            "total_amount":    total,
            "currency":        "USD",
            "promo_code":      random.choices(promos, [0.1,0.1,0.1,0.05,0.05,0.6])[0],
            "is_anomaly":      int(is_anomaly),
            "created_at":      ot.isoformat(),
            "updated_at":      (ot + timedelta(hours=random.randint(0,48))).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_payments(orders_df: pd.DataFrame) -> pd.DataFrame:
    methods   = ["credit_card","debit_card","paypal","apple_pay","bank_transfer"]
    method_w  = [0.40, 0.25, 0.15, 0.12, 0.08]
    statuses  = ["success","failed","refunded","pending"]
    status_w  = [0.88, 0.05, 0.05, 0.02]
    gateways  = ["stripe","braintree","adyen"]
    fail_rsns = ["insufficient_funds","card_declined","timeout"]

    rows = []
    for _, r in orders_df.iterrows():
        ot      = datetime.fromisoformat(r["order_timestamp"])
        paid_at = ot + timedelta(minutes=random.randint(1, 30))
        status  = random.choices(statuses, status_w)[0]
        rows.append({
            "payment_id":      f"PAY_{r['order_id']}",
            "order_id":        r["order_id"],
            "customer_id":     r["customer_id"],
            "payment_method":  random.choices(methods, method_w)[0],
            "payment_status":  status,
            "amount":          r["total_amount"],
            "currency":        "USD",
            "gateway":         random.choice(gateways),
            "transaction_id":  str(uuid.uuid4()),
            "failure_reason":  random.choice(fail_rsns) if status == "failed" else None,
            "paid_at":         paid_at.isoformat() if status == "success" else None,
            "created_at":      paid_at.isoformat(),
            "updated_at":      (paid_at + timedelta(minutes=random.randint(0,60))).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_inventory(prod_ids: list) -> pd.DataFrame:
    warehouses = ["WH_EAST","WH_WEST","WH_CENTRAL","WH_SOUTH"]
    rows = []
    idx  = 1
    for pid in prod_ids:
        for wh in random.sample(warehouses, k=random.randint(1, 4)):
            qty = random.randint(0, 500)
            lr  = rand_date(date(2024,1,1), date(2024,12,31))
            rows.append({
                "inventory_id":      f"INV_{idx:07d}",
                "product_id":        pid,
                "warehouse_id":      wh,
                "quantity_on_hand":  qty,
                "quantity_reserved": random.randint(0, max(0, qty-10)),
                "reorder_point":     random.randint(10, 50),
                "reorder_quantity":  random.randint(50, 200),
                "unit_cost":         round(random.uniform(2, 800), 2),
                "last_restocked":    lr.isoformat(),
                "created_at":        datetime.combine(lr, datetime.min.time()).isoformat(),
                "updated_at":        datetime.combine(
                                       lr + timedelta(days=random.randint(0,90)),
                                       datetime.min.time()).isoformat(),
            })
            idx += 1
    return pd.DataFrame(rows)


def gen_clickstream(n: int, cust_ids: list, prod_ids: list) -> pd.DataFrame:
    events   = ["page_view","product_view","add_to_cart","remove_from_cart",
                "checkout_start","purchase","search","wishlist_add"]
    event_w  = [0.35, 0.25, 0.12, 0.05, 0.08, 0.06, 0.06, 0.03]
    devices  = ["desktop","mobile","tablet"]
    device_w = [0.45, 0.45, 0.10]
    browsers = ["Chrome","Safari","Firefox","Edge"]
    pages    = ["home","category","product","cart","checkout","search","account"]
    refs     = ["google.com","facebook.com","email","direct",None]

    rows = []
    for i in range(1, n+1):
        ev  = random.choices(events, event_w)[0]
        od  = rand_date()
        ot  = datetime.combine(od, datetime.min.time()) + timedelta(
                hours=random.randint(0,23), minutes=random.randint(0,59))
        rows.append({
            "event_id":           f"EVT_{i:09d}",
            "session_id":         rand_str(16),
            "customer_id":        random.choice(cust_ids) if random.random() > 0.3 else None,
            "product_id":         random.choice(prod_ids)
                                  if ev in ["product_view","add_to_cart","wishlist_add"] else None,
            "event_type":         ev,
            "page_url":           f"/{random.choice(pages)}/{rand_str(8).lower()}",
            "referrer":           random.choice(refs),
            "device_type":        random.choices(devices, device_w)[0],
            "browser":            random.choice(browsers),
            "session_duration_s": random.randint(5, 1800),
            "event_timestamp":    ot.isoformat(),
            "created_at":         ot.isoformat(),
        })
    return pd.DataFrame(rows)


def gen_shipments(orders_df: pd.DataFrame) -> pd.DataFrame:
    carriers  = ["FedEx","UPS","USPS","DHL","Amazon Logistics"]
    statuses  = ["processing","shipped","in_transit","delivered","delayed","lost"]
    status_w  = [0.05, 0.10, 0.15, 0.62, 0.07, 0.01]
    warehouses= ["WH_EAST","WH_WEST","WH_CENTRAL","WH_SOUTH"]

    shipped_orders = orders_df[
        orders_df["status"].isin(["shipped","delivered","returned"])
    ].copy()

    rows = []
    for i, (_, r) in enumerate(shipped_orders.iterrows(), start=1):
        ot          = datetime.fromisoformat(r["order_timestamp"])
        shipped_at  = ot + timedelta(hours=random.randint(12, 72))
        est_days    = random.randint(2, 10)
        est_deliver = (shipped_at + timedelta(days=est_days)).date()
        status      = random.choices(statuses, status_w)[0]
        actual_days = est_days + random.randint(-1, 5)
        actual_del  = (shipped_at + timedelta(days=max(1, actual_days))).date()
        is_delayed  = int(actual_del > est_deliver and status not in ["processing","shipped"])
        rows.append({
            "shipment_id":        f"SHIP_{i:07d}",
            "order_id":           r["order_id"],
            "customer_id":        r["customer_id"],
            "carrier":            random.choice(carriers),
            "tracking_number":    f"1Z{rand_str(15)}",
            "status":             status,
            "origin_warehouse":   random.choice(warehouses),
            "destination_zip":    rand_zip(),
            "shipped_at":         shipped_at.isoformat(),
            "estimated_delivery": est_deliver.isoformat(),
            "actual_delivery":    actual_del.isoformat() if status == "delivered" else None,
            "is_delayed":         is_delayed,
            "delay_days":         max(0, actual_days - est_days) if is_delayed else 0,
            "weight_kg":          round(random.uniform(0.1, 30.0), 2),
            "shipping_cost":      round(random.uniform(3.99, 49.99), 2),
            "created_at":         shipped_at.isoformat(),
            "updated_at":         actual_del.isoformat() if status == "delivered"
                                  else shipped_at.isoformat(),
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  NexCart Lakehouse — Data Generation (Lite / Offline)")
    print("=" * 60)

    customers_df  = gen_customers(NUM_CUSTOMERS);      save(customers_df,  "customers")
    products_df   = gen_products(NUM_PRODUCTS);        save(products_df,   "products")

    cust_ids = customers_df["customer_id"].tolist()
    prod_ids = products_df["product_id"].tolist()

    orders_df     = gen_orders(NUM_ORDERS, cust_ids, prod_ids); save(orders_df,     "orders")
    payments_df   = gen_payments(orders_df);                    save(payments_df,   "payments")
    inventory_df  = gen_inventory(prod_ids);                    save(inventory_df,  "inventory")
    clickstream_df= gen_clickstream(NUM_CLICKSTREAM, cust_ids, prod_ids)
    save(clickstream_df, "clickstream")
    shipments_df  = gen_shipments(orders_df);                   save(shipments_df,  "shipments")

    print("=" * 60)
    total = sum([len(customers_df), len(products_df), len(orders_df),
                 len(payments_df), len(inventory_df), len(clickstream_df), len(shipments_df)])
    print(f"  Total records generated: {total:,}")
    print(f"  Output: {RAW_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    main()
