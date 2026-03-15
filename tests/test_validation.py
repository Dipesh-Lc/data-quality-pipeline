"""Tests for validation layer -- unittest compatible."""
from __future__ import annotations
import sys, unittest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.validation.schema_checks import (
    ValidationResult,
    check_no_future_dates, check_no_nulls, check_not_empty,
    check_positive_values, check_primary_key_unique,
    check_required_columns, results_to_df,
    validate_customers, validate_transactions,
)


def _good_tx():
    return pd.DataFrame({
        "InvoiceNo":     ["I001","I002","I003"],
        "StockCode":     ["SC1","SC2","SC3"],
        "Quantity":      [5,-1,10],
        "InvoiceDate":   ["2023-06-01","2023-06-02","2023-06-03"],
        "UnitPrice":     [2.5,3.0,0.99],
        "Country":       ["United Kingdom","Germany","France"],
        "CustomerID":    ["C12001","C12002",None],
        "Description":   ["Widget A","Widget B","Widget C"],
        "Status":        ["completed","cancelled","refunded"],
        "PaymentMethod": ["card","paypal","card"],
    })


def _good_cu():
    return pd.DataFrame({
        "CustomerID": ["C12001","C12002","C12003"],
        "Country":    ["United Kingdom","Germany","France"],
        "SignupDate":  ["2020-01-01","2021-06-15","2022-03-10"],
        "Segment":    ["retail","wholesale","online"],
        "IsActive":   [True,False,True],
    })


class TestNotEmpty(unittest.TestCase):
    def test_passes_with_data(self):
        self.assertTrue(check_not_empty(_good_tx(), "t").passed)
    def test_fails_empty(self):
        r = check_not_empty(pd.DataFrame(), "t")
        self.assertFalse(r.passed)
        self.assertEqual(r.severity, "error")


class TestRequiredColumns(unittest.TestCase):
    def test_passes_all_present(self):
        r = check_required_columns(_good_tx(), "transactions", ["InvoiceNo","StockCode"])
        self.assertTrue(r.passed)
    def test_fails_missing(self):
        df = pd.DataFrame({"InvoiceNo": ["I1"]})
        r  = check_required_columns(df, "transactions", ["InvoiceNo","UnitPrice"])
        self.assertFalse(r.passed)
        self.assertIn("UnitPrice", r.details["missing"])


class TestPKUnique(unittest.TestCase):
    def test_passes_unique(self):
        self.assertTrue(check_primary_key_unique(_good_tx(), "t", "InvoiceNo").passed)
    def test_fails_duplicates(self):
        df = pd.DataFrame({"InvoiceNo": ["I001","I001","I002"]})
        r  = check_primary_key_unique(df, "t", "InvoiceNo")
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)


class TestNoNulls(unittest.TestCase):
    def test_passes_no_nulls(self):
        results = check_no_nulls(_good_tx(), "t", ["InvoiceNo"])
        self.assertTrue(all(r.passed for r in results))
    def test_flags_nulls(self):
        df = pd.DataFrame({"Amount": [1.0, None, 3.0]})
        results = check_no_nulls(df, "t", ["Amount"])
        self.assertFalse(results[0].passed)
        self.assertEqual(results[0].rows_affected, 1)
    def test_skips_missing_column(self):
        df = pd.DataFrame({"X": [1, 2]})
        self.assertEqual(check_no_nulls(df, "t", ["DoesNotExist"]), [])


class TestPositiveValues(unittest.TestCase):
    def test_passes_positive(self):
        df = pd.DataFrame({"UnitPrice": [1.0, 2.5, 0.01]})
        self.assertTrue(check_positive_values(df, "t", "UnitPrice").passed)
    def test_fails_negative(self):
        df = pd.DataFrame({"UnitPrice": [1.0, -0.5, 3.0]})
        r  = check_positive_values(df, "t", "UnitPrice", allow_zero=True)
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)


class TestNoFutureDates(unittest.TestCase):
    def test_passes_past_dates(self):
        self.assertTrue(check_no_future_dates(_good_tx(), "t", "InvoiceDate").passed)
    def test_flags_future(self):
        df = pd.DataFrame({"InvoiceDate": ["2099-01-01","2023-01-01"]})
        r  = check_no_future_dates(df, "t", "InvoiceDate")
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)


class TestConvenienceRunners(unittest.TestCase):
    def test_validate_transactions_returns_list(self):
        results = validate_transactions(_good_tx())
        self.assertIsInstance(results, list)
        self.assertTrue(all(isinstance(r, ValidationResult) for r in results))

    def test_validate_customers_has_results(self):
        results = validate_customers(_good_cu())
        self.assertGreater(len(results), 0)

    def test_results_to_df_shape(self):
        results = validate_transactions(_good_tx())
        df = results_to_df(results, "2023-01-01")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(results))
        self.assertIn("check", df.columns)


if __name__ == "__main__":
    unittest.main()
