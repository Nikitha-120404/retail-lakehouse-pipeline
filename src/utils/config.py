"""
config.py
─────────
Singleton config loader.  Every module imports `get_config()` rather than
reading YAML directly, ensuring a single parsed object for the session.

Usage
-----
    from src.utils.config import get_config
    cfg = get_config()
    print(cfg.paths.bronze)
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


# ── Simple namespace so callers use cfg.paths.bronze ──────────────────────────
class _Namespace:
    """Recursively converts a dict into attribute-accessible namespace."""

    def __init__(self, data: dict[str, Any]) -> None:
        for key, value in data.items():
            setattr(
                self,
                key,
                _Namespace(value) if isinstance(value, dict) else value,
            )

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in self.__dict__.items():
            out[key] = value.as_dict() if isinstance(value, _Namespace) else value
        return out


@lru_cache(maxsize=1)
def get_config(config_path: str | None = None) -> _Namespace:
    """
    Load and cache the platform configuration.

    Parameters
    ----------
    config_path : str, optional
        Override the default ``config/settings.yaml``.  Useful in tests.

    Returns
    -------
    _Namespace
        Attribute-accessible config tree.
    """
    if config_path is None:
        # Support running from any working directory
        root = Path(__file__).resolve().parents[2]
        config_path = str(root / "config" / "settings.yaml")

    env_override = os.getenv("NEXCART_CONFIG")
    if env_override:
        config_path = env_override

    with open(config_path, "r") as fh:
        raw = yaml.safe_load(fh)

    return _Namespace(raw)


def reload_config(config_path: str | None = None) -> _Namespace:
    """Force reload — useful when config changes during a long-running process."""
    get_config.cache_clear()
    return get_config(config_path)
