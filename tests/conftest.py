"""
Test configuration — ensures the project root is on sys.path so that
``import src.*`` works regardless of how pytest or unittest is invoked.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
