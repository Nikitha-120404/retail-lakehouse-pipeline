"""
gold_builder.py
───────────────
Phase 5: Gold Layer — reads Silver Parquet → writes Gold Parquet.
All business aggregations preserved. Delta imports removed.

PostgreSQL sink
───────────────
After each gold table is written to Parquet, it is also loaded into
PostgreSQL using the same NEXCART_PG_* environment variables that the
audit logger uses.  The write is additive and non-breaking: if the DB
is unreachable, a warning is printed and the pipeline continues normally.

Connection is resolved from environment variables:
    NEXCART_PG_HOST      default: localhost
    NEXCART_PG_PORT      default: 5432
    NEXCART_PG_DB        default: nexcart_audit
    NEXCART_PG_USER      default: postgres
    NEXCART_PG_PASSWORD  default: (empty)
    NEXCART_PG_DSN       full DSN — overrides all of the above if set

Inside Docker Compose these are already set to point at the airflow-db
service, so no code changes are needed between local and Docker runs.

Dependencies (already present):
    psycopg2-binary  — in requirements-airflow.txt
    sqlalchemy       — installed as an Airflow core dependency
"""
from __future__ import annotations
import os
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
# PostgreSQL sink helper
# ══════════════════════════════════════════════════════════════════════════════

def _build_pg_url() -> str:
    """
    Build a SQLAlchemy connection URL from environment variables.

    Priority:
      1. NEXCART_PG_DSN  — used as-is if set
      2. Individual NEXCART_PG_* variables
    """
    dsn = os.getenv("NEXCART_PG_DSN")
    if dsn:
        # Accept both libpq DSN (host=... dbname=...) and URL forms.
        # If it's already a URL, return it; otherwise wrap it.
        if dsn.startswith("postgresql"):
            return dsn
        # libpq key=value form — fall through to individual vars
    host     = os.getenv("NEXCART_PG_HOST",     "localhost")
    port     = os.getenv("NEXCART_PG_PORT",     "5432")
    dbname   = os.getenv("NEXCART_PG_DB",       "nexcart_audit")
    user     = os.getenv("NEXCART_PG_USER",     "postgres")
    password = os.getenv("NEXCART_PG_PASSWORD", "")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def write_to_postgres(df: DataFrame, table_name: str) -> None:
    """
    Write a Spark DataFrame to a PostgreSQL table.

    Strategy
    ────────
    - Converts the Spark DataFrame to pandas (gold tables are small aggregations)
    - Uses SQLAlchemy + psycopg2 to write via pandas.DataFrame.to_sql
    - mode=replace  →  DROP + CREATE + INSERT on every run (safe for demo)
    - If the connection fails, prints a warning and returns — pipeline is unaffected

    Parameters
    ----------
    df         : Spark DataFrame to persist
    table_name : Destination PostgreSQL table name (e.g. "daily_revenue")
    """
    try:
        from sqlalchemy import create_engine  # Airflow core dependency — always available
    except ImportError:
        print(
            f"[Gold→PG] WARNING: SQLAlchemy not installed — skipping PostgreSQL write for {table_name}.",
            file=sys.stderr,
        )
        return

    url = _build_pg_url()
    try:
        engine = create_engine(url)

        # Convert to pandas — cast timestamp columns to string to avoid
        # timezone-aware datetime issues between Spark and psycopg2.
        pandas_df = df.toPandas()
        for col in pandas_df.select_dtypes(include=["datetimetz"]).columns:
            pandas_df[col] = pandas_df[col].astype(str)

        with engine.begin() as conn:
            pandas_df.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",   # overwrite mode — safe for demo
                index=False,
                method="multi",        # batch inserts for speed
            )

        row_count = len(pandas_df)
        print(f"[Gold→PG] Written: {table_name} ({row_count:,} rows) → {url.split('@')[-1]}")

    except Exception as exc:
        # Non-fatal: log the error and let the pipeline continue.
        print(
            f"[Gold→PG] WARNING: Could not write {table_name} to PostgreSQL: {exc}",
            file=sys.stderr,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Gold build functions (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def build_daily_revenue(orders_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    paid = payments_df.filter(F.col("payment_status") == "success").select("order_id", "payment_method")
    return (
        orders_df
        .join(paid, on="order_id", how="inner")
        .filter(F.col("status").isin(["confirmed", "shipped", "delivered"]))
        .groupBy("order_date", "channel", "customer_segment", "payment_method", "is_weekend")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("discount_amount").alias("total_discounts"),
            F.sum("tax_amount").alias("total_tax"),
            F.sum(F.col("total_amount") - F.col("discount_amount")).alias("net_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("num_items").alias("total_items_sold"),
            F.sum(F.col("is_anomaly").cast(T.IntegerType())).alias("anomalous_orders"),
        )
        .withColumn(
            "revenue_per_customer",
            F.when(F.col("unique_customers") > 0, F.col("gross_revenue") / F.col("unique_customers"))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "discount_rate_pct",
            F.when(F.col("gross_revenue") > 0,
                   F.round(F.col("total_discounts") / F.col("gross_revenue") * 100, 2))
             .otherwise(F.lit(0.0))
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
        .orderBy("order_date")
    )


def build_customer_ltv(
    orders_df: DataFrame,
    payments_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:
    paid = payments_df.filter(F.col("payment_status") == "success").select("order_id")

    order_metrics = (
        orders_df
        .join(paid, on="order_id", how="inner")
        .filter(F.col("status").isin(["confirmed", "shipped", "delivered", "returned"]))
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_date").alias("active_days"),
            F.sum(F.col("is_anomaly").cast(T.IntegerType())).alias("anomalous_orders"),
        )
    )

    return (
        customers_df
        .select(
            "customer_id", "full_name", "email", "segment", "state",
            "signup_date", "customer_tenure_days", "is_churned", "churn_date",
            "preferred_channel", "age", "gender"
        )
        .join(order_metrics, on="customer_id", how="left")
        .fillna(0, subset=["total_orders", "total_spend", "anomalous_orders"])
        .withColumn(
            "ltv_tier",
            F.when(F.col("total_spend") >= 5000, "top")
             .when(F.col("total_spend") >= 1000, "high")
             .when(F.col("total_spend") >= 200, "medium")
             .when(F.col("total_spend") > 0, "low")
             .otherwise("no_purchase")
        )
        .withColumn("days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn(
            "orders_per_month",
            F.when(
                F.col("customer_tenure_days") > 0,
                F.col("total_orders") / (F.col("customer_tenure_days") / 30.0)
            ).otherwise(F.lit(None))
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def build_product_performance(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    order_products = (
        orders_df
        .filter(F.col("status").isin(["confirmed", "shipped", "delivered"]))
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "total_amount",
            "customer_segment",
            F.explode(F.split("product_ids", r"\|")).alias("product_id")
        )
    )

    monthly = (
        order_products
        .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
        .groupBy("product_id", "year_month")
        .agg(
            F.count("order_id").alias("times_ordered"),
            F.countDistinct("customer_id").alias("unique_buyers"),
            F.sum("total_amount").alias("attributed_revenue"),
            F.avg("total_amount").alias("avg_basket_value"),
        )
    )

    return (
        monthly
        .join(
            products_df.select(
                "product_id", "product_name", "category", "sub_category", "brand",
                "unit_price", "unit_cost", "margin_pct", "margin_bucket", "price_tier", "is_active"
            ),
            on="product_id",
            how="left"
        )
        .withColumn(
            "estimated_units_sold",
            F.when(F.col("unit_price") > 0, (F.col("attributed_revenue") / F.col("unit_price")).cast(T.IntegerType()))
             .otherwise(F.lit(0))
        )
        .withColumn("estimated_cogs", F.col("estimated_units_sold") * F.col("unit_cost"))
        .withColumn("estimated_gross_profit", F.col("attributed_revenue") - F.col("estimated_cogs"))
        .withColumn(
            "category_revenue_rank",
            F.rank().over(
                Window.partitionBy("category", "year_month")
                .orderBy(F.col("attributed_revenue").desc())
            )
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
        .orderBy("year_month", "category", "category_revenue_rank")
    )


def build_inventory_health(inventory_df: DataFrame, products_df: DataFrame) -> DataFrame:
    # Keep only the columns we really need from inventory
    inv = inventory_df.select(
        "product_id",
        "warehouse_id",
        "quantity_on_hand",
        "quantity_available",
        "quantity_reserved",
        "needs_reorder",
        "days_since_restock"
    )

    # Rename product-side columns to avoid ambiguity after join
    prod = products_df.select(
        "product_id",
        F.col("product_name").alias("prod_product_name"),
        F.col("category").alias("prod_category"),
        F.col("unit_price").alias("prod_unit_price"),
        F.col("unit_cost").alias("prod_unit_cost"),
        F.col("is_active").alias("prod_is_active")
    )

    joined = inv.join(prod, on="product_id", how="left")

    return (
        joined
        .groupBy(
            "product_id",
            "prod_product_name",
            "prod_category",
            "prod_unit_price",
            "prod_unit_cost",
            "prod_is_active"
        )
        .agg(
            F.sum("quantity_on_hand").alias("total_qty_on_hand"),
            F.sum("quantity_available").alias("total_qty_available"),
            F.sum("quantity_reserved").alias("total_qty_reserved"),
            F.countDistinct("warehouse_id").alias("num_warehouses"),
            F.sum(F.col("needs_reorder").cast(T.IntegerType())).alias("warehouses_needing_reorder"),
            F.avg("days_since_restock").alias("avg_days_since_restock"),
            F.sum(F.col("quantity_on_hand") * F.col("prod_unit_cost")).alias("total_inventory_value"),
        )
        .withColumnRenamed("prod_product_name", "product_name")
        .withColumnRenamed("prod_category", "category")
        .withColumnRenamed("prod_unit_price", "unit_price")
        .withColumnRenamed("prod_unit_cost", "unit_cost")
        .withColumnRenamed("prod_is_active", "is_active")
        .withColumn(
            "overall_stock_status",
            F.when(F.col("total_qty_available") == 0, "out_of_stock")
             .when(F.col("warehouses_needing_reorder") > 0, "low_stock")
             .otherwise("healthy")
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def build_fulfillment_kpis(shipments_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    order_slim = orders_df.select("order_id", "channel", "customer_segment")

    return (
        shipments_df
        .join(order_slim, on="order_id", how="left")
        .withColumn("week", F.date_trunc("week", F.to_timestamp("shipped_at")).cast(T.DateType()))
        .groupBy("week", "carrier", "origin_warehouse", "channel")
        .agg(
            F.count("shipment_id").alias("total_shipments"),
            F.sum(F.col("is_delayed").cast(T.IntegerType())).alias("delayed_shipments"),
            F.sum(F.col("on_time").cast(T.IntegerType())).alias("on_time_shipments"),
            F.avg("transit_days").alias("avg_transit_days"),
            F.avg("delay_days").alias("avg_delay_days"),
            F.sum("shipping_cost").alias("total_shipping_cost"),
            F.avg("shipping_cost").alias("avg_shipping_cost"),
        )
        .withColumn(
            "on_time_rate_pct",
            F.when(F.col("total_shipments") > 0,
                   F.round(F.col("on_time_shipments") / F.col("total_shipments") * 100, 2))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "delay_rate_pct",
            F.when(F.col("total_shipments") > 0,
                   F.round(F.col("delayed_shipments") / F.col("total_shipments") * 100, 2))
             .otherwise(F.lit(0.0))
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
        .orderBy("week", "carrier")
    )


# ══════════════════════════════════════════════════════════════════════════════
# GoldBuilder
# ══════════════════════════════════════════════════════════════════════════════

class GoldBuilder:

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark()
        self.silver_path = get_path("silver")
        self.gold_path = get_path("gold")

    def _read(self, table: str) -> DataFrame:
        return read_parquet(self.spark, self.silver_path / table)

    def _write(self, df: DataFrame, table: str, partition_by=None) -> None:
        """Write to Parquet (existing behaviour) and also sink to PostgreSQL."""
        write_parquet(df, self.gold_path / table, mode="overwrite", partition_by=partition_by)
        print(f"[Gold] Written: {table}")
        # PostgreSQL sink — non-breaking; failures print a warning and continue
        write_to_postgres(df, table)

    def run_daily_revenue(self):
        df = build_daily_revenue(self._read("orders_enriched"), self._read("payments_validated"))
        self._write(df, "daily_revenue")

    def run_customer_ltv(self):
        df = build_customer_ltv(
            self._read("orders_enriched"),
            self._read("payments_validated"),
            self._read("customers_clean")
        )
        self._write(df, "customer_ltv")

    def run_product_performance(self):
        df = build_product_performance(self._read("orders_enriched"), self._read("products_clean"))
        self._write(df, "product_performance")

    def run_inventory_health(self):
        df = build_inventory_health(self._read("inventory_clean"), self._read("products_clean"))
        self._write(df, "inventory_health")

    def run_fulfillment_kpis(self):
        df = build_fulfillment_kpis(self._read("shipments_enriched"), self._read("orders_enriched"))
        self._write(df, "fulfillment_kpis")

    def run_all(self):
        print("\n[Gold] Starting all builds...")
        self.run_daily_revenue()
        self.run_customer_ltv()
        self.run_product_performance()
        self.run_inventory_health()
        self.run_fulfillment_kpis()
        print("\n[Gold] All builds complete ✓")


if __name__ == "__main__":
    GoldBuilder().run_all()
