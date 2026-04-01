from __future__ import annotations
from pathlib import Path
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession


def _normalize_path(path: str | Path) -> str:
    p = Path(path).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p.as_posix()


def write_parquet(
    df: DataFrame,
    path: str | Path,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
) -> None:
    path_str = _normalize_path(path)
    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path_str)
    print(f"[WRITE] Parquet -> {path_str} mode={mode}")


def read_parquet(spark: SparkSession, path: str | Path) -> DataFrame:
    path_str = Path(path).resolve().as_posix()
    df = spark.read.format("parquet").load(path_str)
    print(f"[READ] Parquet <- {path_str}")
    return df


def read_csv(spark: SparkSession, path: str | Path, schema=None) -> DataFrame:
    path_str = Path(path).resolve().as_posix()
    reader = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
    )
    if schema:
        reader = reader.schema(schema)
    df = reader.csv(path_str)
    print(f"[READ] CSV <- {path_str}")
    return df