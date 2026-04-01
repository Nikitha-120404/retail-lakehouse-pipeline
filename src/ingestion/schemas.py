"""
schemas.py
──────────
Centralised PySpark schema registry for all Bronze tables.

Why explicit schemas instead of inferSchema=True?
  - Schema inference is O(n) — scans the whole file to guess types
  - In production, schema drift breaks pipelines silently
  - Explicit schemas = schema-as-code = version controlled + reviewable

Add new tables here as the platform grows.
"""
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, FloatType,
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

# ── Customers ─────────────────────────────────────────────────────────────────
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",    StringType(),    nullable=False),
    StructField("first_name",     StringType(),    nullable=True),
    StructField("last_name",      StringType(),    nullable=True),
    StructField("email",          StringType(),    nullable=True),
    StructField("phone",          StringType(),    nullable=True),
    StructField("city",           StringType(),    nullable=True),
    StructField("state",          StringType(),    nullable=True),
    StructField("zip_code",       StringType(),    nullable=True),
    StructField("country",        StringType(),    nullable=True),
    StructField("age",            IntegerType(),   nullable=True),
    StructField("gender",         StringType(),    nullable=True),
    StructField("tier",           StringType(),    nullable=True),
    StructField("registered_at",  StringType(),    nullable=True),   # cast in Silver
    StructField("is_active",      IntegerType(),   nullable=True),
    StructField("email_verified", StringType(),    nullable=True),   # nullable intentional
])

# ── Products ──────────────────────────────────────────────────────────────────
PRODUCTS_SCHEMA = StructType([
    StructField("product_id",    StringType(),  nullable=False),
    StructField("product_name",  StringType(),  nullable=True),
    StructField("sku",           StringType(),  nullable=True),
    StructField("department",    StringType(),  nullable=True),
    StructField("brand",         StringType(),  nullable=True),
    StructField("cost_price",    DoubleType(),  nullable=True),
    StructField("selling_price", DoubleType(),  nullable=True),
    StructField("weight_kg",     DoubleType(),  nullable=True),
    StructField("is_perishable", StringType(),  nullable=True),
    StructField("status",        StringType(),  nullable=True),
    StructField("created_at",    StringType(),  nullable=True),
    StructField("supplier_id",   StringType(),  nullable=True),
])

# ── Orders ────────────────────────────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id",        StringType(),  nullable=False),
    StructField("customer_id",     StringType(),  nullable=False),
    StructField("order_date",      StringType(),  nullable=False),
    StructField("status",          StringType(),  nullable=True),
    StructField("channel",         StringType(),  nullable=True),
    StructField("num_items",       IntegerType(), nullable=True),
    StructField("sub_total",       DoubleType(),  nullable=True),
    StructField("discount_amount", DoubleType(),  nullable=True),
    StructField("tax_amount",      DoubleType(),  nullable=True),
    StructField("total_amount",    DoubleType(),  nullable=True),
    StructField("promo_code",      StringType(),  nullable=True),
    StructField("notes",           StringType(),  nullable=True),
])

# ── Payments ──────────────────────────────────────────────────────────────────
PAYMENTS_SCHEMA = StructType([
    StructField("payment_id",        StringType(),  nullable=False),
    StructField("order_id",          StringType(),  nullable=False),
    StructField("customer_id",       StringType(),  nullable=False),
    StructField("payment_method",    StringType(),  nullable=True),
    StructField("amount",            DoubleType(),  nullable=True),
    StructField("currency",          StringType(),  nullable=True),
    StructField("status",            StringType(),  nullable=True),
    StructField("payment_timestamp", StringType(),  nullable=True),
    StructField("gateway_ref",       StringType(),  nullable=True),
    StructField("is_fraud_flag",     IntegerType(), nullable=True),
])

# ── Inventory ─────────────────────────────────────────────────────────────────
INVENTORY_SCHEMA = StructType([
    StructField("inventory_id",  StringType(),  nullable=False),
    StructField("product_id",    StringType(),  nullable=False),
    StructField("warehouse_id",  StringType(),  nullable=True),
    StructField("snapshot_date", StringType(),  nullable=True),
    StructField("qty_on_hand",   IntegerType(), nullable=True),
    StructField("qty_reserved",  IntegerType(), nullable=True),
    StructField("qty_incoming",  IntegerType(), nullable=True),
    StructField("reorder_point", IntegerType(), nullable=True),
    StructField("reorder_qty",   IntegerType(), nullable=True),
    StructField("unit_cost",     DoubleType(),  nullable=True),
])

# ── Clickstream ───────────────────────────────────────────────────────────────
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",     StringType(),  nullable=False),
    StructField("session_id",   StringType(),  nullable=False),
    StructField("customer_id",  StringType(),  nullable=True),   # null = anonymous
    StructField("event_ts",     StringType(),  nullable=False),
    StructField("page",         StringType(),  nullable=True),
    StructField("product_id",   StringType(),  nullable=True),
    StructField("device",       StringType(),  nullable=True),
    StructField("browser",      StringType(),  nullable=True),
    StructField("duration_sec", IntegerType(), nullable=True),
    StructField("referrer",     StringType(),  nullable=True),
])

# ── Shipments ─────────────────────────────────────────────────────────────────
SHIPMENTS_SCHEMA = StructType([
    StructField("shipment_id",            StringType(),  nullable=False),
    StructField("order_id",              StringType(),  nullable=False),
    StructField("carrier",               StringType(),  nullable=True),
    StructField("tracking_number",       StringType(),  nullable=True),
    StructField("status",                StringType(),  nullable=True),
    StructField("ship_date",             StringType(),  nullable=True),
    StructField("promised_date",         StringType(),  nullable=True),
    StructField("actual_delivery_date",  StringType(),  nullable=True),
    StructField("is_delayed",            IntegerType(), nullable=True),
    StructField("delay_hours",           IntegerType(), nullable=True),
    StructField("origin_warehouse",      StringType(),  nullable=True),
    StructField("dest_state",            StringType(),  nullable=True),
])


# ── Registry ──────────────────────────────────────────────────────────────────
SCHEMAS: dict[str, StructType] = {
    "customers":  CUSTOMERS_SCHEMA,
    "products":   PRODUCTS_SCHEMA,
    "orders":     ORDERS_SCHEMA,
    "payments":   PAYMENTS_SCHEMA,
    "inventory":  INVENTORY_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "shipments":  SHIPMENTS_SCHEMA,
}
