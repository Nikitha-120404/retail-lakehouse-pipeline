"""
run_pipeline.py
───────────────
Single-command pipeline runner with observability logging.

Run:
    python scripts/run_pipeline.py                      # full pipeline
    python scripts/run_pipeline.py --skip-generate      # skip data gen
    python scripts/run_pipeline.py --layer silver       # one layer only
    python scripts/run_pipeline.py --history            # show run history
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Ensure tmp dir exists (Windows Spark requirement)
(ROOT / "tmp" / "spark").mkdir(parents=True, exist_ok=True)
(ROOT / "logs").mkdir(parents=True, exist_ok=True)

from src.utils.pipeline_logger import track_step, print_run_history


def main():
    parser = argparse.ArgumentParser(description="NexCart Lakehouse Pipeline Runner")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--layer", type=str, default=None,
                        choices=["generate","bronze","silver","gold","features","quality"])
    parser.add_argument("--history", action="store_true", help="Show pipeline run history")
    args = parser.parse_args()

    if args.history:
        print_run_history()
        return

    print("\n" + "="*60)
    print("  NexCart Lakehouse — Pipeline Runner")
    print("="*60)

    # Step 0: Generate
    if not args.skip_generate and args.layer in (None, "generate"):
        with track_step("raw", "all_tables"):
            from scripts.generate_data_lite import main as gm
            gm()

    # Step 1: Bronze
    if args.layer in (None, "bronze"):
        from src.ingestion.bronze_ingestor import BronzeIngestor
        ingestor = BronzeIngestor()
        for table in ingestor.cfg["tables"]["bronze"]:
            with track_step("bronze", table):
                ingestor.ingest_table(table)

    # Step 2: Silver
    if args.layer in (None, "silver"):
        from src.transformation.silver_transformer import SilverTransformer
        t = SilverTransformer()
        for table in ["customers","products","orders","payments",
                      "inventory","clickstream","shipments"]:
            with track_step("silver", table):
                getattr(t, f"run_{table}")()

    # Step 3: Gold
    if args.layer in (None, "gold"):
        from src.gold.gold_builder import GoldBuilder
        g = GoldBuilder()
        for table in ["daily_revenue","customer_ltv","product_performance",
                      "inventory_health","fulfillment_kpis"]:
            with track_step("gold", table):
                getattr(g, f"run_{table}")()

    # Step 4: Features
    if args.layer in (None, "features"):
        from src.features.feature_engineer import FeatureEngineer
        fe = FeatureEngineer()
        for table in ["customer_features","product_features",
                      "order_risk_features","delivery_features"]:
            with track_step("features", table):
                getattr(fe, f"run_{table}")()

    # Step 5: Quality
    if args.layer in (None, "quality"):
        from src.quality.data_quality import run_silver_quality_checks, save_quality_report
        from src.utils.spark_session import get_spark
        with track_step("quality", "silver_checks"):
            spark   = get_spark()
            results = run_silver_quality_checks(spark)
            save_quality_report(spark, results)

    print("\n" + "="*60)
    print("  PIPELINE COMPLETE ✓")
    print("="*60)
    print_run_history(last_n=10)


if __name__ == "__main__":
    main()
