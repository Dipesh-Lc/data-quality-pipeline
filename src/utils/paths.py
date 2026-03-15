# src/utils/paths.py

"""Central path resolver -- all pipeline code imports from here."""
from pathlib import Path

# Project root is three levels up: src/utils/paths.py → src/utils → src → root
ROOT    = Path(__file__).resolve().parents[2]

RAW       = ROOT / "data" / "raw"
INTERIM   = ROOT / "data" / "interim"
PROCESSED = ROOT / "data" / "processed"
SAMPLES   = ROOT / "data" / "samples"
LOGS      = ROOT / "artifacts" / "logs"
REPORTS   = ROOT / "reports"
CONFIGS   = ROOT / "configs"
SQL       = ROOT / "sql"


def ensure_dirs() -> None:
    """Create all data-layer directories if they do not exist."""
    for d in (RAW, INTERIM, PROCESSED, SAMPLES, LOGS, REPORTS / "figures"):
        d.mkdir(parents=True, exist_ok=True)


def dated_raw_dir(run_date: str) -> Path:
    """Return data/raw/<run_date>/ and create it if needed."""
    p = RAW / run_date
    p.mkdir(parents=True, exist_ok=True)
    return p
