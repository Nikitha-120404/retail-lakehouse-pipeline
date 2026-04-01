"""
config_loader.py
────────────────
Loads settings.yaml and exposes typed helpers.
Delta references removed — all paths return plain directories for Parquet.
"""
from __future__ import annotations
import os
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH  = _PROJECT_ROOT / "config" / "settings.yaml"


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {_CONFIG_PATH}")
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    env = os.getenv("NEXCART_ENV")
    if env:
        cfg["project"]["environment"] = env
    print(f"[Config] Loaded | project={cfg['project']['name']} | env={cfg['project']['environment']}")
    return cfg


def get_path(layer: str) -> Path:
    """Return absolute path for a data layer: raw | bronze | silver | gold | features."""
    cfg = load_config()
    return _PROJECT_ROOT / cfg["paths"][layer]


def get_spark_conf() -> dict[str, str]:
    """Return Spark config as flat string dict (Parquet-only, no Delta keys)."""
    cfg = load_config()
    sc  = cfg["spark"]
    return {
        "spark.app.name":                   sc["app_name"],
        "spark.master":                     sc["master"],
        "spark.driver.memory":              sc["driver_memory"],
        "spark.executor.memory":            sc["executor_memory"],
        "spark.sql.shuffle.partitions":     str(sc["shuffle_partitions"]),
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.mergeSchema":    "false",
        "spark.hadoop.io.native.lib.available": "false",
        "spark.local.dir":                  "tmp/spark",
    }
