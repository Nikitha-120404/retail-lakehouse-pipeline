"""
feature_engineer.py
───────────────────
Phase 6: Feature Engineering — reads Silver Parquet → writes Features Parquet.
All ML feature logic preserved. Delta imports removed.
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

# ── Encoding maps ──────────────────────────────────────────────────────────────
SEGMENT_MAP   = {"bronze":0,"silver":1,"gold":2,"platinum":3}
CHANNEL_MAP   = {"none":0,"sms":1,"email":2,"app":3}
GENDER_MAP    = {"M":0,"F":1,"Other":2}
CARRIER_MAP   = {"USPS":0,"FedEx":1,"UPS":2,"DHL":3,"Amazon Logistics":4}
WAREHOUSE_MAP = {"WH_EAST":0,"WH_WEST":1,"WH_CENTRAL":2,"WH_SOUTH":3}


def _map_col(col_name: str, mapping: dict) -> F.Column:
    expr = F.lit(-1)
    for k, v in mapping.items():
        expr = F.when(F.col(col_name) == k, v).otherwise(expr)
    return expr


def build_customer_features(customers_df, orders_df, payments_df, clickstream_df) -> DataFrame:
    today = F.current_date()
    paid  = payments_df.filter(F.col("payment_status") == "success").select("order_id")
    orders = orders_df.join(paid, on="order_id", how="inner")

    def window_agg(days: int, suffix: str) -> DataFrame:
        cutoff = F.date_sub(today, days)
        return (
            orders.filter(F.col("order_date") >= cutoff)
            .groupBy("customer_id")
            .agg(
                F.count("order_id")   .alias(f"orders_{suffix}"),
                F.sum("total_amount") .alias(f"spend_{suffix}"),
                F.avg("total_amount") .alias(f"avg_order_{suffix}"),
            )
        )

    w7, w30, w90, w180 = window_agg(7,"7d"), window_agg(30,"30d"), window_agg(90,"90d"), window_agg(180,"180d")

    lifetime = (
        orders.groupBy("customer_id")
        .agg(
            F.count("order_id")              .alias("total_orders"),
            F.sum("total_amount")            .alias("total_spend"),
            F.avg("total_amount")            .alias("avg_order_value"),
            F.max("order_date")              .alias("last_order_date"),
            F.min("order_date")              .alias("first_order_date"),
            F.stddev("total_amount")         .alias("stddev_order_value"),
            F.sum(F.col("is_anomaly").cast(T.IntegerType())).alias("n_anomalous_orders"),
        )
    )
    pay_stats = (
        payments_df.groupBy("customer_id")
        .agg(
            F.count("payment_id")                            .alias("n_payments"),
            F.sum(F.col("is_failed").cast(T.IntegerType()))  .alias("n_failed_payments"),
            F.avg(F.col("is_failed").cast(T.IntegerType()))  .alias("payment_failure_rate"),
        )
    )
    cs_30d = (
        clickstream_df
        .filter(F.col("event_date") >= F.date_sub(today, 30))
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.count("event_id")                              .alias("clicks_30d"),
            F.countDistinct("session_id")                   .alias("sessions_30d"),
            F.sum(F.col("is_purchase").cast(T.IntegerType())).alias("purchases_30d"),
            F.sum(F.col("is_add_to_cart").cast(T.IntegerType())).alias("add_to_cart_30d"),
            F.avg("session_duration_s")                     .alias("avg_session_duration_30d"),
        )
    )
    return (
        customers_df
        .select("customer_id","age","segment","gender","preferred_channel",
                "customer_tenure_days","is_churned","signup_date")
        .join(lifetime,   on="customer_id", how="left")
        .join(w7,         on="customer_id", how="left")
        .join(w30,        on="customer_id", how="left")
        .join(w90,        on="customer_id", how="left")
        .join(w180,       on="customer_id", how="left")
        .join(pay_stats,  on="customer_id", how="left")
        .join(cs_30d,     on="customer_id", how="left")
        .withColumn("days_since_last_order",
            F.datediff(today, F.col("last_order_date")))
        .withColumn("order_frequency_per_month",
            F.when(F.col("customer_tenure_days") > 0,
                F.col("total_orders") / (F.col("customer_tenure_days") / 30.0)
            ).otherwise(0.0))
        .withColumn("segment_encoded",  _map_col("segment", SEGMENT_MAP))
        .withColumn("gender_encoded",   _map_col("gender", GENDER_MAP))
        .withColumn("channel_encoded",  _map_col("preferred_channel", CHANNEL_MAP))
        .fillna(0)
        .withColumn("_feature_snapshot_date", today)
        .withColumn("_feature_created_at", F.current_timestamp())
    )


def build_product_features(products_df, orders_df, inventory_df) -> DataFrame:
    today = F.current_date()
    order_items = (
        orders_df
        .filter(F.col("status").isin(["confirmed","shipped","delivered"]))
        .select(F.explode(F.split("product_ids", r"\|")).alias("product_id"), "order_date")
    )
    def velocity(days, suffix):
        return (
            order_items.filter(F.col("order_date") >= F.date_sub(today, days))
            .groupBy("product_id")
            .agg(F.count("product_id").alias(f"orders_{suffix}"))
        )
    v7, v30, v90 = velocity(7,"7d"), velocity(30,"30d"), velocity(90,"90d")
    inv_agg = (
        inventory_df.groupBy("product_id")
        .agg(
            F.sum("quantity_available").alias("total_available"),
            F.sum("quantity_on_hand")  .alias("total_on_hand"),
            F.countDistinct("warehouse_id").alias("num_warehouses"),
            F.avg("days_since_restock").alias("avg_days_since_restock"),
            F.sum(F.col("needs_reorder").cast(T.IntegerType())).alias("n_reorder_flags"),
        )
    )
    return (
        products_df
        .select("product_id","product_name","category","sub_category","brand",
                "unit_price","unit_cost","margin_pct","weight_kg","is_active",
                "margin_bucket","price_tier")
        .join(v7,      on="product_id", how="left")
        .join(v30,     on="product_id", how="left")
        .join(v90,     on="product_id", how="left")
        .join(inv_agg, on="product_id", how="left")
        .withColumn("velocity_trend_30_90",
            F.when(F.col("orders_90d") > 0,
                F.col("orders_30d") / (F.col("orders_90d") / 3.0)
            ).otherwise(0.0))
        .withColumn("daily_sales_rate_30d", F.col("orders_30d") / F.lit(30.0))
        .withColumn("days_of_supply",
            F.when(F.col("daily_sales_rate_30d") > 0,
                F.col("total_available") / F.col("daily_sales_rate_30d")
            ).otherwise(999.0))
        .withColumn("is_active_int", F.col("is_active").cast(T.IntegerType()))
        .withColumn("margin_bucket_encoded",
            F.when(F.col("margin_bucket") == "high", 2)
             .when(F.col("margin_bucket") == "medium", 1)
             .otherwise(0))
        .fillna(0)
        .withColumn("_feature_snapshot_date", today)
        .withColumn("_feature_created_at", F.current_timestamp())
    )


def build_order_risk_features(orders_df, payments_df, customers_df) -> DataFrame:
    customer_stats = (
        orders_df.filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.avg("total_amount")   .alias("cust_avg_order"),
            F.stddev("total_amount").alias("cust_stddev_order"),
            F.count("order_id")     .alias("cust_total_orders"),
        )
    )
    return (
        orders_df
        .join(payments_df.select("order_id","payment_method","gateway",
                                  "is_failed","payment_lag_minutes"),
              on="order_id", how="left")
        .join(customer_stats, on="customer_id", how="left")
        .join(customers_df.select("customer_id","segment_encoded","customer_tenure_days"),
              on="customer_id", how="left")
        .withColumn("amount_z_score",
            F.when(F.col("cust_stddev_order") > 0,
                (F.col("total_amount") - F.col("cust_avg_order")) / F.col("cust_stddev_order")
            ).otherwise(0.0))
        .withColumn("is_first_order",  (F.col("cust_total_orders") == 1).cast(T.IntegerType()))
        .withColumn("is_night_order",  F.col("order_hour").between(0, 5).cast(T.IntegerType()))
        .withColumn("has_promo",       F.col("promo_code").isNotNull().cast(T.IntegerType()))
        .withColumn("high_discount",
            (F.col("discount_amount") / F.col("subtotal") > 0.30).cast(T.IntegerType()))
        .withColumn("channel_encoded",
            F.when(F.col("channel")=="web",0)
             .when(F.col("channel")=="mobile_app",1)
             .when(F.col("channel")=="in_store",2)
             .otherwise(3))
        .withColumn("payment_method_encoded",
            F.when(F.col("payment_method")=="credit_card",0)
             .when(F.col("payment_method")=="debit_card",1)
             .when(F.col("payment_method")=="paypal",2)
             .when(F.col("payment_method")=="apple_pay",3)
             .otherwise(4))
        .select(
            "order_id","customer_id","order_date",
            "total_amount","num_items","discount_amount","tax_amount",
            "subtotal","payment_lag_minutes","amount_z_score",
            "cust_avg_order","cust_stddev_order","cust_total_orders","customer_tenure_days",
            "order_hour","order_dow","is_weekend",
            "is_first_order","is_night_order","has_promo","high_discount","is_failed",
            "channel_encoded","payment_method_encoded","segment_encoded",
            "is_anomaly",
            F.current_date().alias("_feature_snapshot_date"),
            F.current_timestamp().alias("_feature_created_at"),
        )
        .fillna(0)
    )


def build_delivery_features(shipments_df, orders_df) -> DataFrame:
    carrier_stats = (
        shipments_df.filter(F.col("status") == "delivered")
        .groupBy("carrier")
        .agg(
            F.avg(F.col("on_time").cast(T.IntegerType())).alias("carrier_ontime_rate"),
            F.avg("transit_days").alias("carrier_avg_transit"),
            F.avg("delay_days")  .alias("carrier_avg_delay"),
        )
    )
    wh_stats = (
        shipments_df.groupBy("origin_warehouse")
        .agg(F.avg(F.col("is_delayed").cast(T.IntegerType())).alias("wh_delay_rate"))
    )
    order_slim = orders_df.select("order_id","channel","num_items","total_amount")
    return (
        shipments_df
        .join(carrier_stats, on="carrier",          how="left")
        .join(wh_stats,      on="origin_warehouse", how="left")
        .join(order_slim,    on="order_id",         how="left")
        .withColumn("ship_hour",    F.hour("shipped_at"))
        .withColumn("ship_dow",     F.dayofweek(F.to_date("shipped_at")))
        .withColumn("ship_month",   F.month(F.to_date("shipped_at")))
        .withColumn("is_weekend_ship",
            F.col("ship_dow").isin([1,7]).cast(T.IntegerType()))
        .withColumn("carrier_encoded",   _map_col("carrier", CARRIER_MAP))
        .withColumn("warehouse_encoded", _map_col("origin_warehouse", WAREHOUSE_MAP))
        .withColumn("channel_encoded",
            F.when(F.col("channel")=="web",0)
             .when(F.col("channel")=="mobile_app",1)
             .otherwise(2))
        .select(
            "shipment_id","order_id","customer_id",
            "num_items","total_amount","weight_kg","shipping_cost",
            "ship_hour","ship_dow","ship_month","is_weekend_ship",
            "carrier_ontime_rate","carrier_avg_transit","carrier_avg_delay","wh_delay_rate",
            "carrier_encoded","warehouse_encoded","channel_encoded",
            F.col("is_delayed").cast(T.IntegerType()).alias("is_delayed"),
            "delay_days",
            F.current_date().alias("_feature_snapshot_date"),
            F.current_timestamp().alias("_feature_created_at"),
        )
        .fillna(0)
    )


class FeatureEngineer:

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark         = spark or get_spark()
        self.silver_path   = get_path("silver")
        self.features_path = get_path("features")

    def _read(self, table: str) -> DataFrame:
        return read_parquet(self.spark, self.silver_path / table)

    def _write(self, df: DataFrame, table: str) -> None:
        write_parquet(df, self.features_path / table, mode="overwrite")
        print(f"[Features] Written: {table}")

    def run_customer_features(self):
        df = build_customer_features(
            self._read("customers_clean"), self._read("orders_enriched"),
            self._read("payments_validated"), self._read("clickstream_sessions"),
        )
        self._write(df, "customer_features")

    def run_product_features(self):
        df = build_product_features(
            self._read("products_clean"), self._read("orders_enriched"),
            self._read("inventory_clean"),
        )
        self._write(df, "product_features")

    def run_order_risk_features(self):
        custs = read_parquet(self.spark, get_path("features") / "customer_features")
        df = build_order_risk_features(
            self._read("orders_enriched"),
            self._read("payments_validated"),
            custs,
        )
        self._write(df, "order_risk_features")

    def run_delivery_features(self):
        df = build_delivery_features(
            self._read("shipments_enriched"), self._read("orders_enriched"),
        )
        self._write(df, "delivery_features")

    def run_all(self):
        print("\n[Features] Starting feature engineering...")
        self.run_customer_features()
        self.run_product_features()
        self.run_order_risk_features()
        self.run_delivery_features()
        print("\n[Features] All feature tables complete ✓")


if __name__ == "__main__":
    FeatureEngineer().run_all()
