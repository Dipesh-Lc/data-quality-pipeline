"""Tests for ingestion layer — unittest compatible."""
from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingestion.ingest import (
    fetch_holidays,
    ingest_csv,
    write_ingestion_manifest,
)

_MOCK_HOLIDAYS = [
    {"date": "2023-01-01", "localName": "New Year's Day",
     "name": "New Year's Day", "global": True},
    {"date": "2023-12-25", "localName": "Christmas Day",
     "name": "Christmas Day", "global": True},
]


def _make_tx_csv(path: Path) -> Path:
    pd.DataFrame({
        "InvoiceNo":     ["I001", "I002"],
        "StockCode":     ["SC1",  "SC2"],
        "Quantity":      [3, -1],
        "InvoiceDate":   ["2023-01-01", "2023-01-02"],
        "UnitPrice":     [2.5, 3.0],
        "Country":       ["United Kingdom", "Germany"],
        "CustomerID":    ["C12001", "C12002"],
        "Status":        ["completed", "cancelled"],
        "PaymentMethod": ["card", "paypal"],
    }).to_csv(path, index=False)
    return path


class TestIngestCsv(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.src = _make_tx_csv(self.tmp / "transactions.csv")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_copies_file_to_dest(self):
        dest = self.tmp / "dest"
        dest.mkdir()
        ingest_csv(self.src, dest, "test")
        self.assertTrue((dest / "transactions.csv").exists())

    def test_returns_dataframe(self):
        dest = self.tmp / "out"
        dest.mkdir()
        df = ingest_csv(self.src, dest, "tx")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    def test_raises_on_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            ingest_csv(self.tmp / "ghost.csv", self.tmp, "ghost")

    def test_row_count_matches(self):
        dest = self.tmp / "rc"
        dest.mkdir()
        df = ingest_csv(self.src, dest, "tx")
        self.assertEqual(len(df), 2)


class TestManifest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_manifest_fields(self):
        with patch("src.ingestion.ingest.dated_raw_dir", return_value=self.tmp):
            write_ingestion_manifest("2023-01-01", 100, 50, 20)
        manifest = json.loads((self.tmp / "manifest.json").read_text())
        self.assertEqual(manifest["transactions"], 100)
        self.assertEqual(manifest["customers"], 50)
        self.assertEqual(manifest["holidays"], 20)
        self.assertIn("ingested_at", manifest)


class TestFetchHolidays(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_returns_dataframe_on_success(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _MOCK_HOLIDAYS
        with (
            patch("requests.get", return_value=mock_resp),
            patch("src.ingestion.ingest.dated_raw_dir", return_value=self.tmp),
        ):
            df = fetch_holidays(countries=["GB"], year=2023, run_date="2023-01-01")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_returns_empty_df_on_api_error(self):
        import requests as req
        with (
            patch("requests.get", side_effect=req.RequestException("timeout")),
            patch("src.ingestion.ingest.dated_raw_dir", return_value=self.tmp),
        ):
            df = fetch_holidays(countries=["XX"], year=2023, run_date="2023-01-01")
        self.assertEqual(len(df), 0)

    def test_saves_csv_file(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _MOCK_HOLIDAYS
        with (
            patch("requests.get", return_value=mock_resp),
            patch("src.ingestion.ingest.dated_raw_dir", return_value=self.tmp),
        ):
            fetch_holidays(countries=["GB"], year=2023, run_date="2023-01-01")
        self.assertTrue((self.tmp / "holidays.csv").exists())


if __name__ == "__main__":
    unittest.main()
