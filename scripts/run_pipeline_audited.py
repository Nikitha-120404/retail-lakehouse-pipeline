"""
run_pipeline_audited.py
───────────────────────
Audited pipeline runner for NexCart Lakehouse.

Wraps the same steps as run_pipeline.py with AuditLogger, writing a
structured JSONL audit trail to logs/audit_<date>_<run_id>.jsonl.

The existing run_pipeline.py is untouched — this is an additive script.

Usage
─────
    python scripts/run_pipeline_audited.py                   # full pipeline
    python scripts/run_pipeline_audited.py --skip-generate   # skip data gen
    python scripts/run_pipeline_audited.py --layer bronze    # one layer only
    python scripts/run_pipeline_audited.py --layer silver
    python scripts/run_pipeline_audited.py --layer gold
    python scripts/run_pipeline_audited.py --layer features
    python scripts/run_pipeline_audited.py --layer quality
    python scripts/run_pipeline_audited.py --stats           # record row counts
    python scripts/run_pipeline_audited.py --history         # print run history

The --stats flag performs one extra Spark read per output table to capture
row counts.  Skip it for faster runs when counts are not needed.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Ensure Spark tmp dir exists (Windows requirement)
(ROOT / "tmp" / "spark").mkdir(parents=True, exist_ok=True)
(ROOT / "logs").mkdir(parents=True, exist_ok=True)

from src.utils.audit_logger   import AuditLogger
from src.utils.pipeline_logger import print_run_history
from src.utils.config_loader  import get_path

# ── Output table names per layer ───────────────────────────────────────────────
# Maps the input table key used in run_* method names to the Parquet folder
# that is actually written.  Used for optional row-count stats collection.

_SILVER_OUTPUT: dict[str, str] = {
    "customers":   "customers_clean",
    "products":    "products_clean",
    "orders":      "orders_enriched",
    "payments":    "payments_validated",
    "inventory":   "inventory_clean",
    "clickstream": "clickstream_sessions",
    "shipments":   "shipments_enriched",
}

_GOLD_TABLES: list[str] = [
    "daily_revenue",
    "customer_ltv",
    "product_performance",
    "inventory_health",
    "fulfillment_kpis",
]

_FEATURE_TABLES: list[str] = [
    "customer_features",
    "product_features",
    "order_risk_features",
    "delivery_features",
]


# ══════════════════════════════════════════════════════════════════════════════
# Layer runners
# ══════════════════════════════════════════════════════════════════════════════

def run_generate(audit: AuditLogger) -> None:
    with audit.track_step("raw", "all_tables"):
        from scripts.generate_data_lite import main as gm
        gm()


def run_bronze(audit: AuditLogger, collect_stats: bool, spark) -> None:
    from src.ingestion.bronze_ingestor import BronzeIngestor
    ingestor = BronzeIngestor(spark=spark)
    tables   = ingestor.cfg["tables"]["bronze"]
    for table in tables:
        with audit.track_step("bronze", table):
            ingestor.ingest_table(table)
        if collect_stats:
            audit.log_table_stats(
                spark, "bronze", table, get_path("bronze") / table
            )


def run_silver(audit: AuditLogger, collect_stats: bool, spark) -> None:
    from src.transformation.silver_transformer import SilverTransformer
    t = SilverTransformer(spark=spark)
    for src_table, out_table in _SILVER_OUTPUT.items():
        with audit.track_step("silver", src_table):
            getattr(t, f"run_{src_table}")()
        if collect_stats:
            audit.log_table_stats(
                spark, "silver", out_table, get_path("silver") / out_table
            )


def run_gold(audit: AuditLogger, collect_stats: bool, spark) -> None:
    from src.gold.gold_builder import GoldBuilder
    g = GoldBuilder(spark=spark)
    for table in _GOLD_TABLES:
        with audit.track_step("gold", table):
            getattr(g, f"run_{table}")()
        if collect_stats:
            audit.log_table_stats(
                spark, "gold", table, get_path("gold") / table
            )


def run_features(audit: AuditLogger, collect_stats: bool, spark) -> None:
    from src.features.feature_engineer import FeatureEngineer
    fe = FeatureEngineer(spark=spark)
    for table in _FEATURE_TABLES:
        with audit.track_step("features", table):
            getattr(fe, f"run_{table}")()
        if collect_stats:
            audit.log_table_stats(
                spark, "features", table, get_path("features") / table
            )


def run_quality(audit: AuditLogger) -> None:
    from src.quality.data_quality import run_silver_quality_checks, save_quality_report
    from src.utils.spark_session  import get_spark
    spark = get_spark()
    with audit.track_step("quality", "silver_checks"):
        results = run_silver_quality_checks(spark)
        save_quality_report(spark, results)
    # Forward every DQ rule into the audit log
    audit.log_quality_results_bulk(results)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NexCart Lakehouse — Audited Pipeline Runner"
    )
    parser.add_argument("--skip-generate", action="store_true",
                        help="Skip synthetic data generation step")
    parser.add_argument(
        "--layer", type=str, default=None,
        choices=["generate", "bronze", "silver", "gold", "features", "quality"],
        help="Run a single layer instead of the full pipeline",
    )
    parser.add_argument("--stats",   action="store_true",
                        help="Collect row counts after each write (extra Spark reads)")
    parser.add_argument("--history", action="store_true",
                        help="Print pipeline run history and exit")
    args = parser.parse_args()

    if args.history:
        print_run_history()
        return

    audit = AuditLogger()
    pipeline_name = args.layer or "full"
    audit.log_pipeline_start(pipeline_name)

    from src.utils.spark_session import get_spark
    spark = get_spark()

    print("\n" + "=" * 60)
    print("  NexCart Lakehouse — Audited Pipeline Runner")
    print("=" * 60)

    final_status = "success"
    try:
        # Step 0 — Generate
        if not args.skip_generate and args.layer in (None, "generate"):
            run_generate(audit)

        # Step 1 — Bronze
        if args.layer in (None, "bronze"):
            run_bronze(audit, collect_stats=args.stats, spark=spark)

        # Step 2 — Silver
        if args.layer in (None, "silver"):
            run_silver(audit, collect_stats=args.stats, spark=spark)

        # Step 3 — Gold
        if args.layer in (None, "gold"):
            run_gold(audit, collect_stats=args.stats, spark=spark)

        # Step 4 — Features
        if args.layer in (None, "features"):
            run_features(audit, collect_stats=args.stats, spark=spark)

        # Step 5 — Quality
        if args.layer in (None, "quality"):
            run_quality(audit)

    except Exception as exc:
        final_status = "failed"
        print(f"\n[Pipeline] FATAL: {type(exc).__name__}: {exc}")
        raise
    finally:
        audit.log_pipeline_end(pipeline_name, status=final_status)
        audit.print_summary()
        audit.close()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE ✓")
    print("=" * 60)
    print_run_history(last_n=10)


if __name__ == "__main__":
    main()
