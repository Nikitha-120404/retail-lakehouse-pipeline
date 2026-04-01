"""
data_quality.py
───────────────
Phase 8: Data Quality Checks — reads Silver Parquet, writes report as Parquet.
Delta imports removed.
"""
from __future__ import annotations
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.spark_session import get_spark
from src.utils.config_loader import get_path, load_config
from src.utils.parquet_utils import read_parquet, write_parquet


@dataclass
class DQResult:
    table:      str
    rule_name:  str
    column:     str
    status:     str
    metric:     float
    threshold:  float
    message:    str
    checked_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class DataQualityChecker:

    def __init__(self, table_name: str, df: DataFrame, cfg: dict | None = None):
        self.table   = table_name
        self.df      = df
        self.cfg     = cfg or load_config()
        self.results: list[DQResult] = []
        self._n      = df.count()

    def check_row_count(self, min_rows: int | None = None):
        min_rows = min_rows or self.cfg["quality"]["min_row_count"]
        passed   = self._n >= min_rows
        self.results.append(DQResult(
            table=self.table, rule_name="row_count_check", column="*",
            status="PASS" if passed else "FAIL",
            metric=float(self._n), threshold=float(min_rows),
            message=f"Row count {self._n:,} {'≥' if passed else '<'} {min_rows:,}",
        ))
        return self

    def check_nulls(self, columns: list[str], max_null_pct: float | None = None):
        max_null_pct = max_null_pct or self.cfg["quality"]["max_null_pct"]
        for col in columns:
            if col not in self.df.columns:
                continue
            null_pct = self.df.filter(F.col(col).isNull()).count() / self._n if self._n > 0 else 0
            passed   = null_pct <= max_null_pct
            self.results.append(DQResult(
                table=self.table, rule_name="null_check", column=col,
                status="PASS" if passed else "FAIL",
                metric=round(null_pct*100,2), threshold=round(max_null_pct*100,2),
                message=f"{col}: {null_pct*100:.2f}% nulls",
            ))
        return self

    def check_duplicates(self, primary_key: list[str]):
        max_dup = self.cfg["quality"]["max_duplicate_pct"]
        n_dist  = self.df.dropDuplicates(primary_key).count()
        dup_pct = (self._n - n_dist) / self._n if self._n > 0 else 0
        passed  = dup_pct <= max_dup
        self.results.append(DQResult(
            table=self.table, rule_name="duplicate_check", column=str(primary_key),
            status="PASS" if passed else "FAIL",
            metric=round(dup_pct*100,4), threshold=round(max_dup*100,2),
            message=f"{self._n - n_dist:,} duplicate rows ({dup_pct*100:.3f}%)",
        ))
        return self

    def check_range(self, column: str, min_val: float, max_val: float):
        if column not in self.df.columns:
            return self
        out = self.df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
        pct = out / self._n if self._n > 0 else 0
        self.results.append(DQResult(
            table=self.table, rule_name="range_check", column=column,
            status="PASS" if pct < 0.01 else "WARN",
            metric=round(pct*100,4), threshold=1.0,
            message=f"{column}: {out:,} values outside [{min_val}, {max_val}]",
        ))
        return self

    def check_enum(self, column: str, valid_values: list[str]):
        if column not in self.df.columns:
            return self
        invalid = self.df.filter(~F.col(column).isin(valid_values) & F.col(column).isNotNull()).count()
        pct     = invalid / self._n if self._n > 0 else 0
        self.results.append(DQResult(
            table=self.table, rule_name="enum_check", column=column,
            status="PASS" if pct < 0.01 else "FAIL",
            metric=round(pct*100,4), threshold=1.0,
            message=f"{column}: {invalid:,} invalid values",
        ))
        return self

    def summarise(self):
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        warned = sum(1 for r in self.results if r.status == "WARN")
        icon   = "✅" if failed == 0 else "❌"
        print(f"\n{icon} DQ [{self.table}] PASS={passed} WARN={warned} FAIL={failed}")
        for r in self.results:
            print(f"  [{r.status}] {r.rule_name} | {r.message}")

    def to_dataframe(self, spark: SparkSession) -> DataFrame:
        schema = T.StructType([
            T.StructField("table",      T.StringType(), True),
            T.StructField("rule_name",  T.StringType(), True),
            T.StructField("column",     T.StringType(), True),
            T.StructField("status",     T.StringType(), True),
            T.StructField("metric",     T.DoubleType(), True),
            T.StructField("threshold",  T.DoubleType(), True),
            T.StructField("message",    T.StringType(), True),
            T.StructField("checked_at", T.StringType(), True),
        ])
        rows = [(r.table, r.rule_name, r.column, r.status,
                 r.metric, r.threshold, r.message, r.checked_at) for r in self.results]
        return spark.createDataFrame(rows, schema)


def run_silver_quality_checks(spark: SparkSession) -> list[DQResult]:
    silver = get_path("silver")
    all_results = []

    tables_cfg = [
        ("customers_clean",    ["customer_id","email","signup_date"], ["customer_id"],
         "segment", ["bronze","silver","gold","platinum","unknown"]),
        ("orders_enriched",    ["order_id","customer_id","order_date","total_amount"], ["order_id"],
         "status", ["pending","confirmed","shipped","delivered","cancelled","returned","unknown"]),
        ("payments_validated", ["payment_id","order_id","amount"], ["payment_id"],
         None, None),
        ("shipments_enriched", ["shipment_id","order_id","carrier"], ["shipment_id"],
         None, None),
    ]

    for table, null_cols, pk, enum_col, enum_vals in tables_cfg:
        df      = read_parquet(spark, silver / table)
        checker = DataQualityChecker(table, df)
        checker.check_row_count().check_nulls(null_cols).check_duplicates(pk)
        if enum_col and enum_vals:
            checker.check_enum(enum_col, enum_vals)
        checker.summarise()
        all_results.extend(checker.results)

    return all_results


def save_quality_report(spark: SparkSession, results: list[DQResult]) -> None:
    schema = T.StructType([
        T.StructField("table",      T.StringType(), True),
        T.StructField("rule_name",  T.StringType(), True),
        T.StructField("column",     T.StringType(), True),
        T.StructField("status",     T.StringType(), True),
        T.StructField("metric",     T.DoubleType(), True),
        T.StructField("threshold",  T.DoubleType(), True),
        T.StructField("message",    T.StringType(), True),
        T.StructField("checked_at", T.StringType(), True),
    ])
    rows = [(r.table, r.rule_name, r.column, r.status,
             r.metric, r.threshold, r.message, r.checked_at) for r in results]
    df   = spark.createDataFrame(rows, schema)
    report_path = get_path("gold") / "quality_report"
    write_parquet(df, report_path, mode="overwrite")
    print(f"\n[DQ] Quality report saved → {report_path} ({len(rows)} rules)")
    failures = [r for r in results if r.status == "FAIL"]
    if failures:
        raise ValueError(f"[DQ] {len(failures)} rule(s) FAILED: {[f.message for f in failures]}")


if __name__ == "__main__":
    spark   = get_spark()
    results = run_silver_quality_checks(spark)
    save_quality_report(spark, results)
