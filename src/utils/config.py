# src/utils/config.py

"""Load project configuration from config.yaml."""
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    """Load and cache the project configuration."""
    cfg_path = Path(__file__).resolve().parents[2] / "configs" / "config.yaml"
    with open(cfg_path) as fh:
        return yaml.safe_load(fh)


def get(key: str, default: Any = None) -> Any:
    """Access config values using dot notation."""
    cfg = load_config()
    parts = key.split(".")
    val = cfg

    for p in parts:
        if isinstance(val, dict) and p in val:
            val = val[p]
        else:
            return default
    return val