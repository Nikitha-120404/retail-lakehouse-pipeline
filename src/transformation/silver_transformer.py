"""
silver_transformer.py
─────────────────────
Phase 4: Silver Layer — reads Bronze Parquet → writes Silver Parquet.
All business logic preserved. Delta imports removed entirely.
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark
from src.utils.config_loader import get_path
from src.utils.parquet_utils import read_parquet, write_parquet


# ══════════════════════════════════════════════════════════════════════════════
# Transformation functions — one per table
# ══════════════════════════════════════════════════════════════════════════════

def transform_customers(df: DataFrame) -> DataFrame:
    VALID_SEGMENTS = ["bronze", "silver", "gold", "platinum"]
    VALID_GENDERS  = ["M", "F", "Other"]
    VALID_CHANNELS = ["email", "sms", "app", "none"]

    w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("signup_date",  F.to_date("signup_date"))
        .withColumn("churn_date",   F.to_date("churn_date"))
        .withColumn("created_at",   F.to_timestamp("created_at"))
        .withColumn("updated_at",   F.to_timestamp("updated_at"))
        .withColumn("age",          F.col("age").cast(T.IntegerType()))
        .withColumn("is_churned",   F.col("is_churned").cast(T.BooleanType()))
        .withColumn("segment",
            F.when(F.lower("segment").isin(VALID_SEGMENTS), F.lower("segment"))
             .otherwise(F.lit("unknown")))
        .withColumn("gender",
            F.when(F.col("gender").isin(VALID_GENDERS), F.col("gender"))
             .otherwise(None))
        .withColumn("preferred_channel",
            F.when(F.lower("preferred_channel").isin(VALID_CHANNELS), F.lower("preferred_channel"))
             .otherwise(F.lit("none")))
        .withColumn("full_name",
            F.concat_ws(" ", F.initcap("first_name"), F.initcap("last_name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("customer_tenure_days",
            F.datediff(F.current_date(), F.col("signup_date")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_products(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["product_id"])
        .withColumn("unit_cost",   F.col("unit_cost").cast(T.DoubleType()))
        .withColumn("unit_price",  F.col("unit_price").cast(T.DoubleType()))
        .withColumn("margin_pct",  F.col("margin_pct").cast(T.DoubleType()))
        .withColumn("weight_kg",   F.col("weight_kg").cast(T.DoubleType()))
        .withColumn("is_active",   F.col("is_active").cast(T.BooleanType()))
        .withColumn("launch_date", F.to_date("launch_date"))
        .withColumn("created_at",  F.to_timestamp("created_at"))
        .withColumn("updated_at",  F.to_timestamp("updated_at"))
        .withColumn("category",    F.initcap(F.trim("category")))
        .withColumn("brand",       F.initcap(F.trim("brand")))
        .withColumn("margin_bucket",
            F.when(F.col("margin_pct") >= 60, "high")
             .when(F.col("margin_pct") >= 35, "medium")
             .otherwise("low"))
        .withColumn("price_tier",
            F.when(F.col("unit_price") >= 500, "premium")
             .when(F.col("unit_price") >= 100, "mid")
             .otherwise("value"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_orders(df: DataFrame, customers_df: DataFrame) -> DataFrame:
    VALID_STATUSES = ["pending","confirmed","shipped","delivered","cancelled","returned"]
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    orders = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("order_date",       F.to_date("order_date"))
        .withColumn("order_timestamp",  F.to_timestamp("order_timestamp"))
        .withColumn("created_at",       F.to_timestamp("created_at"))
        .withColumn("updated_at",       F.to_timestamp("updated_at"))
        .withColumn("subtotal",         F.col("subtotal").cast(T.DoubleType()))
        .withColumn("discount_amount",  F.col("discount_amount").cast(T.DoubleType()))
        .withColumn("tax_amount",       F.col("tax_amount").cast(T.DoubleType()))
        .withColumn("total_amount",     F.col("total_amount").cast(T.DoubleType()))
        .withColumn("num_items",        F.col("num_items").cast(T.IntegerType()))
        .withColumn("is_anomaly",       F.col("is_anomaly").cast(T.BooleanType()))
        .withColumn("status",
            F.when(F.lower("status").isin(VALID_STATUSES), F.lower("status"))
             .otherwise(F.lit("unknown")))
        .withColumn("total_amount",
            F.when(F.col("total_amount") > 0, F.col("total_amount")).otherwise(None))
        .withColumn("order_hour",    F.hour("order_timestamp"))
        .withColumn("order_dow",     F.dayofweek("order_date"))
        .withColumn("order_month",   F.month("order_date"))
        .withColumn("is_weekend",    F.col("order_dow").isin([1, 7]).cast(T.BooleanType()))
        .withColumn("days_since_order", F.datediff(F.current_date(), F.col("order_date")))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    cust_slim = customers_df.select("customer_id", "segment", "state").alias("c")
    return (
        orders.alias("o")
        .join(cust_slim, on="customer_id", how="left")
        .withColumnRenamed("segment", "customer_segment")
        .withColumnRenamed("state",   "customer_state")
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_payments(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["payment_id"])
        .withColumn("amount",     F.col("amount").cast(T.DoubleType()))
        .withColumn("paid_at",    F.to_timestamp("paid_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("is_failed",
            (F.col("payment_status") == "failed").cast(T.BooleanType()))
        .withColumn("is_refunded",
            (F.col("payment_status") == "refunded").cast(T.BooleanType()))
        .withColumn("payment_lag_minutes",
            F.when(F.col("paid_at").isNotNull(),
                (F.unix_timestamp("paid_at") - F.unix_timestamp("created_at")) / 60.0
            ).otherwise(None))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_inventory(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["inventory_id"])
        .withColumn("quantity_on_hand",  F.col("quantity_on_hand").cast(T.IntegerType()))
        .withColumn("quantity_reserved", F.col("quantity_reserved").cast(T.IntegerType()))
        .withColumn("reorder_point",     F.col("reorder_point").cast(T.IntegerType()))
        .withColumn("reorder_quantity",  F.col("reorder_quantity").cast(T.IntegerType()))
        .withColumn("unit_cost",         F.col("unit_cost").cast(T.DoubleType()))
        .withColumn("last_restocked",    F.to_date("last_restocked"))
        .withColumn("created_at",        F.to_timestamp("created_at"))
        .withColumn("updated_at",        F.to_timestamp("updated_at"))
        .withColumn("quantity_available",
            F.greatest(F.col("quantity_on_hand") - F.col("quantity_reserved"), F.lit(0)))
        .withColumn("stock_status",
            F.when(F.col("quantity_available") == 0, "out_of_stock")
             .when(F.col("quantity_available") <= F.col("reorder_point"), "low_stock")
             .otherwise("in_stock"))
        .withColumn("needs_reorder",
            (F.col("quantity_available") <= F.col("reorder_point")).cast(T.BooleanType()))
        .withColumn("days_since_restock",
            F.datediff(F.current_date(), F.col("last_restocked")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_clickstream(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["event_id"])
        .withColumn("event_timestamp",    F.to_timestamp("event_timestamp"))
        .withColumn("created_at",         F.to_timestamp("created_at"))
        .withColumn("session_duration_s", F.col("session_duration_s").cast(T.IntegerType()))
        .withColumn("event_date",         F.to_date("event_timestamp"))
        .withColumn("event_hour",         F.hour("event_timestamp"))
        .withColumn("is_purchase",        (F.col("event_type") == "purchase").cast(T.BooleanType()))
        .withColumn("is_add_to_cart",     (F.col("event_type") == "add_to_cart").cast(T.BooleanType()))
        .withColumn("device_category",
            F.when(F.col("device_type").isin(["mobile","tablet"]), "mobile")
             .otherwise("desktop"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


def transform_shipments(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["shipment_id"])
        .withColumn("shipped_at",           F.to_timestamp("shipped_at"))
        .withColumn("estimated_delivery",   F.to_date("estimated_delivery"))
        .withColumn("actual_delivery",      F.to_date("actual_delivery"))
        .withColumn("created_at",           F.to_timestamp("created_at"))
        .withColumn("updated_at",           F.to_timestamp("updated_at"))
        .withColumn("is_delayed",           F.col("is_delayed").cast(T.BooleanType()))
        .withColumn("delay_days",           F.col("delay_days").cast(T.IntegerType()))
        .withColumn("weight_kg",            F.col("weight_kg").cast(T.DoubleType()))
        .withColumn("shipping_cost",        F.col("shipping_cost").cast(T.DoubleType()))
        .withColumn("transit_days",
            F.when(F.col("actual_delivery").isNotNull(),
                F.datediff("actual_delivery", F.to_date("shipped_at"))
            ).otherwise(None))
        .withColumn("on_time",
            F.when(F.col("actual_delivery").isNotNull(),
                (F.col("actual_delivery") <= F.col("estimated_delivery")).cast(T.BooleanType())
            ).otherwise(None))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("_source_table", "_source_file", "_ingestion_date")
    )


# ══════════════════════════════════════════════════════════════════════════════
# Orchestration class
# ══════════════════════════════════════════════════════════════════════════════
class SilverTransformer:

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark       = spark or get_spark()
        self.bronze_path = get_path("bronze")
        self.silver_path = get_path("silver")

    def _read(self, table: str) -> DataFrame:
        return read_parquet(self.spark, self.bronze_path / table)

    def _write(self, df: DataFrame, table: str) -> None:
        write_parquet(df, self.silver_path / table, mode="overwrite")
        print(f"[Silver] Written: {table}")

    def run_customers(self):
        self._write(transform_customers(self._read("customers")), "customers_clean")

    def run_products(self):
        self._write(transform_products(self._read("products")), "products_clean")

    def run_orders(self):
        df = transform_orders(self._read("orders"), self._read("customers"))
        self._write(df, "orders_enriched")

    def run_payments(self):
        self._write(transform_payments(self._read("payments")), "payments_validated")

    def run_inventory(self):
        self._write(transform_inventory(self._read("inventory")), "inventory_clean")

    def run_clickstream(self):
        self._write(transform_clickstream(self._read("clickstream")), "clickstream_sessions")

    def run_shipments(self):
        self._write(transform_shipments(self._read("shipments")), "shipments_enriched")

    def run_all(self):
        print("\n[Silver] Starting all transformations...")
        self.run_customers()
        self.run_products()
        self.run_orders()
        self.run_payments()
        self.run_inventory()
        self.run_clickstream()
        self.run_shipments()
        print("\n[Silver] All transformations complete ✓")


if __name__ == "__main__":
    SilverTransformer().run_all()
