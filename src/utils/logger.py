# src/utils/logger.py

"""Central logging setup and logger factory."""
import logging
import logging.config
import logging.handlers
from pathlib import Path

import yaml

_CONFIGURED = False


def _setup_logging() -> None:
    """Configure logging from YAML or fallback settings."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    log_cfg_path = Path(__file__).resolve().parents[2] / "configs" / "logging.yaml"

    # Ensure log directory exists before handlers try to open files
    log_dir = Path(__file__).resolve().parents[2] / "artifacts" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if log_cfg_path.exists():
        with open(log_cfg_path) as fh:
            cfg = yaml.safe_load(fh)
        # Patch file handler paths to absolute
        for handler in cfg.get("handlers", {}).values():
            if "filename" in handler:
                handler["filename"] = str(
                    Path(__file__).resolve().parents[2] / handler["filename"]
                )
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        )

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger."""
    _setup_logging()
    return logging.getLogger(name)
