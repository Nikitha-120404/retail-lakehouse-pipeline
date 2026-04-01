"""
schema_registry.py
──────────────────
Single source of truth for all dataset schemas.

Bronze ingestion enforces these schemas on read.
Silver transformations reference them for type casting.
Tests use them for fixture generation.

Usage
-----
    from src.utils.schema_registry import SCHEMAS
    schema = SCHEMAS["customers"]
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMAS: dict[str, StructType] = {

    "customers": StructType([
        StructField("customer_id",    StringType(),    nullable=False),
        StructField("first_name",     StringType(),    nullable=True),
        StructField("last_name",      StringType(),    nullable=True),
        StructField("email",          StringType(),    nullable=True),
        StructField("phone",          StringType(),    nullable=True),
        StructField("city",           StringType(),    nullable=True),
        StructField("country",        StringType(),    nullable=True),
        StructField("signup_date",    TimestampType(), nullable=True),
        StructField("segment",        StringType(),    nullable=True),  # premium|standard|new
        StructField("is_active",      StringType(),    nullable=True),  # raw = string
    ]),

    "products": StructType([
        StructField("product_id",     StringType(),    nullable=False),
        StructField("name",           StringType(),    nullable=True),
        StructField("category",       StringType(),    nullable=True),
        StructField("sub_category",   StringType(),    nullable=True),
        StructField("brand",          StringType(),    nullable=True),
        StructField("unit_price",     StringType(),    nullable=True),  # raw = string
        StructField("cost_price",     StringType(),    nullable=True),
        StructField("sku",            StringType(),    nullable=True),
        StructField("is_active",      StringType(),    nullable=True),
    ]),

    "orders": StructType([
        StructField("order_id",       StringType(),    nullable=False),
        StructField("customer_id",    StringType(),    nullable=True),
        StructField("product_id",     StringType(),    nullable=True),
        StructField("quantity",       StringType(),    nullable=True),
        StructField("unit_price",     StringType(),    nullable=True),
        StructField("discount_pct",   StringType(),    nullable=True),
        StructField("order_status",   StringType(),    nullable=True),
        StructField("order_date",     TimestampType(), nullable=True),
        StructField("channel",        StringType(),    nullable=True),  # web|mobile|store
    ]),

    "payments": StructType([
        StructField("payment_id",     StringType(),    nullable=False),
        StructField("order_id",       StringType(),    nullable=True),
        StructField("customer_id",    StringType(),    nullable=True),
        StructField("amount",         StringType(),    nullable=True),
        StructField("payment_method", StringType(),    nullable=True),
        StructField("payment_status", StringType(),    nullable=True),
        StructField("payment_date",   TimestampType(), nullable=True),
        StructField("gateway",        StringType(),    nullable=True),
    ]),

    "inventory": StructType([
        StructField("inventory_id",   StringType(),    nullable=False),
        StructField("product_id",     StringType(),    nullable=True),
        StructField("warehouse_id",   StringType(),    nullable=True),
        StructField("quantity_on_hand",StringType(),   nullable=True),
        StructField("reorder_level",  StringType(),    nullable=True),
        StructField("last_updated",   TimestampType(), nullable=True),
    ]),

    "clickstream": StructType([
        StructField("event_id",       StringType(),    nullable=False),
        StructField("customer_id",    StringType(),    nullable=True),
        StructField("session_id",     StringType(),    nullable=True),
        StructField("product_id",     StringType(),    nullable=True),
        StructField("event_type",     StringType(),    nullable=True),  # view|add_cart|purchase|search
        StructField("page_url",       StringType(),    nullable=True),
        StructField("device_type",    StringType(),    nullable=True),
        StructField("event_ts",       TimestampType(), nullable=True),
    ]),

    "shipments": StructType([
        StructField("shipment_id",    StringType(),    nullable=False),
        StructField("order_id",       StringType(),    nullable=True),
        StructField("carrier",        StringType(),    nullable=True),
        StructField("tracking_number",StringType(),    nullable=True),
        StructField("ship_date",      TimestampType(), nullable=True),
        StructField("estimated_delivery", TimestampType(), nullable=True),
        StructField("actual_delivery",TimestampType(), nullable=True),
        StructField("status",         StringType(),    nullable=True),
        StructField("warehouse_id",   StringType(),    nullable=True),
    ]),
}

# Column subsets used during Silver type-casting
NUMERIC_COLUMNS: dict[str, list[str]] = {
    "products":  ["unit_price", "cost_price"],
    "orders":    ["quantity", "unit_price", "discount_pct"],
    "payments":  ["amount"],
    "inventory": ["quantity_on_hand", "reorder_level"],
}

BOOLEAN_COLUMNS: dict[str, list[str]] = {
    "customers": ["is_active"],
    "products":  ["is_active"],
}
