"""
Unit tests for config_loader — no Spark required.
"""
import pytest
from pathlib import Path
from unittest.mock import patch
import yaml


def test_load_config_returns_dict(tmp_path, monkeypatch):
    """Config loads and returns a dict with expected top-level keys."""
    # Write a minimal config to tmp dir
    cfg = {
        "project": {"name": "test", "environment": "local"},
        "paths": {"base_dir": "data", "raw": "data/raw", "bronze": "data/bronze",
                   "silver": "data/silver", "gold": "data/gold", "features": "data/features",
                   "checkpoints": "data/checkpoints"},
        "spark": {
            "app_name": "Test", "master": "local[1]",
            "driver_memory": "1g", "executor_memory": "1g",
            "shuffle_partitions": 2,
            "extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "catalog": "spark_catalog",
            "catalog_impl": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "delta_log_retention_hours": 24,
        },
        "data_generation": {"seed": 42, "num_customers": 10, "num_products": 5,
                             "num_orders": 20, "num_payments": 20, "num_inventory": 5,
                             "num_clickstream": 50, "num_shipments": 15,
                             "date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "tables": {"bronze": [], "silver": [], "gold": [], "features": []},
        "quality": {"max_null_pct": 0.05, "min_row_count": 1, "max_duplicate_pct": 0.01},
        "airflow": {"schedule_bronze": "0 1 * * *", "schedule_silver": "0 2 * * *",
                     "schedule_gold": "0 3 * * *", "schedule_features": "0 4 * * *",
                     "retries": 1, "retry_delay_minutes": 1},
    }
    cfg_file = tmp_path / "settings.yaml"
    cfg_file.write_text(yaml.dump(cfg))

    import src.utils.config_loader as cl
    cl.load_config.cache_clear()
    monkeypatch.setattr(cl, "_CONFIG_PATH", cfg_file)
    cl.load_config.cache_clear()

    loaded = cl.load_config()
    assert isinstance(loaded, dict)
    assert "project" in loaded
    assert "paths"   in loaded
    assert "spark"   in loaded
    cl.load_config.cache_clear()


def test_get_spark_conf_returns_strings(tmp_path, monkeypatch):
    """All spark config values should be strings (SparkConf requirement)."""
    import src.utils.config_loader as cl
    cl.load_config.cache_clear()

    cfg_file = tmp_path / "settings.yaml"
    cfg_file.write_text(yaml.dump({
        "project": {"name": "t", "environment": "local"},
        "paths": {"base_dir": "data", "raw": "data/raw", "bronze": "data/bronze",
                   "silver": "data/silver", "gold": "data/gold", "features": "data/features",
                   "checkpoints": "data/checkpoints"},
        "spark": {
            "app_name": "Test", "master": "local[1]",
            "driver_memory": "1g", "executor_memory": "1g",
            "shuffle_partitions": 2,
            "extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "catalog": "spark_catalog",
            "catalog_impl": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "delta_log_retention_hours": 24,
        },
        "data_generation": {"seed": 0, "num_customers": 1, "num_products": 1,
                             "num_orders": 1, "num_payments": 1, "num_inventory": 1,
                             "num_clickstream": 1, "num_shipments": 1,
                             "date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "tables": {}, "quality": {}, "airflow": {},
    }))
    monkeypatch.setattr(cl, "_CONFIG_PATH", cfg_file)
    cl.load_config.cache_clear()

    spark_conf = cl.get_spark_conf()
    for k, v in spark_conf.items():
        assert isinstance(v, str), f"Key '{k}' has non-string value: {v!r}"
    cl.load_config.cache_clear()
