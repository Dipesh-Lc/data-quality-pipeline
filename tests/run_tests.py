"""
Test runner — runs all tests using Python's built-in unittest.
Works without pytest installed.

Usage:
    python tests/run_tests.py
    python tests/run_tests.py -v
"""
import sys
import unittest
from pathlib import Path

# Make src importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Import all test modules 
from tests.test_ingestion   import *  # noqa: F401,F403
from tests.test_validation  import *  # noqa: F401,F403
from tests.test_processing  import *  # noqa: F401,F403
from tests.test_monitoring  import *  # noqa: F401,F403
from tests.test_warehouse   import *  # noqa: F401,F403


if __name__ == "__main__":
    verbosity = 2 if "-v" in sys.argv else 1
    runner    = unittest.TextTestRunner(verbosity=verbosity)
    loader    = unittest.TestLoader()
    suite     = loader.discover(str(ROOT / "tests"), pattern="test_*.py")
    result    = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
