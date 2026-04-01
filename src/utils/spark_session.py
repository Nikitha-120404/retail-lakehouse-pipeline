"""
spark_session.py
────────────────
SparkSession factory — Parquet only, no Delta Lake dependency.
Works on Windows, Mac, and Linux without winutils or Hadoop native IO.
"""
from __future__ import annotations
from pyspark.sql import SparkSession

_SPARK_INSTANCE: SparkSession | None = None


def get_spark(app_name: str = "NexCartLakehouse") -> SparkSession:
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE is not None:
        try:
            if not _SPARK_INSTANCE.sparkContext._jsc.sc().isStopped():
                return _SPARK_INSTANCE
        except Exception:
            pass

    _SPARK_INSTANCE = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        # Windows-safe: disable native Hadoop IO
        .config("spark.hadoop.io.native.lib.available", "false")
        # Parquet settings
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        # Avoid temp dir issues on Windows
        .config("spark.local.dir", "tmp/spark")
        .getOrCreate()
    )
    _SPARK_INSTANCE.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Session started | version={_SPARK_INSTANCE.version} | master=local[*]")
    return _SPARK_INSTANCE


def stop_spark() -> None:
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE:
        _SPARK_INSTANCE.stop()
        _SPARK_INSTANCE = None
        print("[Spark] Session stopped.")
