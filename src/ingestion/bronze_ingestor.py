"""
bronze_ingestor.py
──────────────────
Phase 3: Bronze Layer Ingestion
Reads raw CSVs → enforces schema → adds audit columns → writes Parquet.

Bronze rules:
  - Schema enforcement (reject bad rows to _corrupt_record)
  - Audit columns: _source_table, _source_file, _batch_id, _ingested_at, _ingestion_date
  - Partition by _ingestion_date
  - No business logic — preserve raw values exactly
"""
from __future__ import annotations
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.spark_session import get_spark
from src.utils.config_loader import load_config, get_path
from src.utils.parquet_utils import read_csv, write_parquet

# ══════════════════════════════════════════════════════════════════════════════
# Schema Registry
# ══════════════════════════════════════════════════════════════════════════════
SCHEMAS: dict[str, T.StructType] = {

    "customers": T.StructType([
        T.StructField("customer_id",       T.StringType(), False),
        T.StructField("first_name",        T.StringType(), True),
        T.StructField("last_name",         T.StringType(), True),
        T.StructField("email",             T.StringType(), True),
        T.StructField("phone",             T.StringType(), True),
        T.StructField("city",              T.StringType(), True),
        T.StructField("state",             T.StringType(), True),
        T.StructField("zip_code",          T.StringType(), True),
        T.StructField("country",           T.StringType(), True),
        T.StructField("signup_date",       T.StringType(), True),
        T.StructField("segment",           T.StringType(), True),
        T.StructField("age",               T.StringType(), True),
        T.StructField("gender",            T.StringType(), True),
        T.StructField("preferred_channel", T.StringType(), True),
        T.StructField("is_churned",        T.StringType(), True),
        T.StructField("churn_date",        T.StringType(), True),
        T.StructField("created_at",        T.StringType(), True),
        T.StructField("updated_at",        T.StringType(), True),
    ]),

    "products": T.StructType([
        T.StructField("product_id",    T.StringType(), False),
        T.StructField("product_name",  T.StringType(), True),
        T.StructField("category",      T.StringType(), True),
        T.StructField("sub_category",  T.StringType(), True),
        T.StructField("brand",         T.StringType(), True),
        T.StructField("sku",           T.StringType(), True),
        T.StructField("unit_cost",     T.StringType(), True),
        T.StructField("unit_price",    T.StringType(), True),
        T.StructField("margin_pct",    T.StringType(), True),
        T.StructField("weight_kg",     T.StringType(), True),
        T.StructField("is_active",     T.StringType(), True),
        T.StructField("launch_date",   T.StringType(), True),
        T.StructField("supplier_id",   T.StringType(), True),
        T.StructField("created_at",    T.StringType(), True),
        T.StructField("updated_at",    T.StringType(), True),
    ]),

    "orders": T.StructType([
        T.StructField("order_id",         T.StringType(), False),
        T.StructField("customer_id",      T.StringType(), False),
        T.StructField("order_date",       T.StringType(), True),
        T.StructField("order_timestamp",  T.StringType(), True),
        T.StructField("status",           T.StringType(), True),
        T.StructField("channel",          T.StringType(), True),
        T.StructField("num_items",        T.StringType(), True),
        T.StructField("product_ids",      T.StringType(), True),
        T.StructField("subtotal",         T.StringType(), True),
        T.StructField("discount_amount",  T.StringType(), True),
        T.StructField("tax_amount",       T.StringType(), True),
        T.StructField("total_amount",     T.StringType(), True),
        T.StructField("currency",         T.StringType(), True),
        T.StructField("promo_code",       T.StringType(), True),
        T.StructField("is_anomaly",       T.StringType(), True),
        T.StructField("created_at",       T.StringType(), True),
        T.StructField("updated_at",       T.StringType(), True),
    ]),

    "payments": T.StructType([
        T.StructField("payment_id",      T.StringType(), False),
        T.StructField("order_id",        T.StringType(), False),
        T.StructField("customer_id",     T.StringType(), False),
        T.StructField("payment_method",  T.StringType(), True),
        T.StructField("payment_status",  T.StringType(), True),
        T.StructField("amount",          T.StringType(), True),
        T.StructField("currency",        T.StringType(), True),
        T.StructField("gateway",         T.StringType(), True),
        T.StructField("transaction_id",  T.StringType(), True),
        T.StructField("failure_reason",  T.StringType(), True),
        T.StructField("paid_at",         T.StringType(), True),
        T.StructField("created_at",      T.StringType(), True),
        T.StructField("updated_at",      T.StringType(), True),
    ]),

    "inventory": T.StructType([
        T.StructField("inventory_id",       T.StringType(), False),
        T.StructField("product_id",         T.StringType(), False),
        T.StructField("warehouse_id",       T.StringType(), True),
        T.StructField("quantity_on_hand",   T.StringType(), True),
        T.StructField("quantity_reserved",  T.StringType(), True),
        T.StructField("reorder_point",      T.StringType(), True),
        T.StructField("reorder_quantity",   T.StringType(), True),
        T.StructField("unit_cost",          T.StringType(), True),
        T.StructField("last_restocked",     T.StringType(), True),
        T.StructField("created_at",         T.StringType(), True),
        T.StructField("updated_at",         T.StringType(), True),
    ]),

    "clickstream": T.StructType([
        T.StructField("event_id",            T.StringType(), False),
        T.StructField("session_id",          T.StringType(), True),
        T.StructField("customer_id",         T.StringType(), True),
        T.StructField("product_id",          T.StringType(), True),
        T.StructField("event_type",          T.StringType(), True),
        T.StructField("page_url",            T.StringType(), True),
        T.StructField("referrer",            T.StringType(), True),
        T.StructField("device_type",         T.StringType(), True),
        T.StructField("browser",             T.StringType(), True),
        T.StructField("session_duration_s",  T.StringType(), True),
        T.StructField("event_timestamp",     T.StringType(), True),
        T.StructField("created_at",          T.StringType(), True),
    ]),

    "shipments": T.StructType([
        T.StructField("shipment_id",        T.StringType(), False),
        T.StructField("order_id",           T.StringType(), False),
        T.StructField("customer_id",        T.StringType(), False),
        T.StructField("carrier",            T.StringType(), True),
        T.StructField("tracking_number",    T.StringType(), True),
        T.StructField("status",             T.StringType(), True),
        T.StructField("origin_warehouse",   T.StringType(), True),
        T.StructField("destination_zip",    T.StringType(), True),
        T.StructField("shipped_at",         T.StringType(), True),
        T.StructField("estimated_delivery", T.StringType(), True),
        T.StructField("actual_delivery",    T.StringType(), True),
        T.StructField("is_delayed",         T.StringType(), True),
        T.StructField("delay_days",         T.StringType(), True),
        T.StructField("weight_kg",          T.StringType(), True),
        T.StructField("shipping_cost",      T.StringType(), True),
        T.StructField("created_at",         T.StringType(), True),
        T.StructField("updated_at",         T.StringType(), True),
    ]),
}

# Partition strategy per table
PARTITION_COLS: dict[str, list[str]] = {
    "customers":   ["country"],
    "products":    ["category"],
    "orders":      [],
    "payments":    [],
    "inventory":   ["warehouse_id"],
    "clickstream": [],
    "shipments":   [],
}


# ══════════════════════════════════════════════════════════════════════════════
# BronzeIngestor
# ══════════════════════════════════════════════════════════════════════════════
class BronzeIngestor:

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark       = spark or get_spark()
        self.cfg         = load_config()
        self.raw_path    = get_path("raw")
        self.bronze_path = get_path("bronze")
        self.batch_id    = str(uuid.uuid4())[:8]
        self.ingested_at = datetime.utcnow().isoformat()
        print(f"[Bronze] Ingestor ready | batch_id={self.batch_id}")

    def _read_csv(self, table: str) -> DataFrame:
        schema   = SCHEMAS[table]
        csv_path = self.raw_path / f"{table}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Raw file not found: {csv_path}")
        return read_csv(self.spark, csv_path, schema=schema)

    def _add_audit_columns(self, df: DataFrame, table: str) -> DataFrame:
        return (
            df
            .withColumn("_source_table",   F.lit(table))
            .withColumn("_source_file",    F.lit(f"{table}.csv"))
            .withColumn("_batch_id",       F.lit(self.batch_id))
            .withColumn("_ingested_at",    F.lit(self.ingested_at))
            .withColumn("_ingestion_date", F.lit(datetime.utcnow().date().isoformat()))
        )

    def ingest_table(self, table: str) -> None:
        print(f"\n[Bronze] Ingesting: {table}")
        df          = self._read_csv(table)
        df          = self._add_audit_columns(df, table)
        output_path = self.bronze_path / table
        partition_by = PARTITION_COLS.get(table, [])
        write_parquet(df, output_path, mode="overwrite",
                      partition_by=partition_by if partition_by else None)
        print(f"[Bronze] Done: {table} → {output_path}")

    def ingest_all(self) -> None:
        tables = self.cfg["tables"]["bronze"]
        print(f"\n[Bronze] Starting ingestion | tables={tables}")
        for t in tables:
            self.ingest_table(t)
        print(f"\n[Bronze] All tables ingested ✓")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str, default=None)
    args = parser.parse_args()
    ingestor = BronzeIngestor()
    if args.table:
        ingestor.ingest_table(args.table)
    else:
        ingestor.ingest_all()
