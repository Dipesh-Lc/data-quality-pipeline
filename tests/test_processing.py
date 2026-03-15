"""Tests for processing/cleaning layer -- unittest compatible."""
from __future__ import annotations
import sys, unittest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.processing.clean_customers import clean_customers
from src.processing.clean_transactions import clean_transactions
from src.processing.transform import derive_columns, enrich_holidays, join_transactions_customers


def _raw_customers():
    return pd.DataFrame({
        "CustomerID": ["C001","C002"," C003 ",None,"C001"],
        "Country":    ["UNITED KINGDOM","germany","France","Italy","UNITED KINGDOM"],
        "SignupDate": ["2020-01-01","bad-date","2021-05-10","2022-03-01","2020-01-01"],
        "Segment":    ["RETAIL","Wholesale","online","retail","RETAIL"],
        "IsActive":   [True,False,True,True,True],
    })


def _raw_transactions():
    return pd.DataFrame({
        "InvoiceNo":     ["I001","I002","I003","I004","I001"],
        "StockCode":     ["SC1","SC2","SC3","SC4","SC1"],
        "Description":   ["widget a","Widget B","WIDGET C","Widget D","widget a"],
        "Quantity":      [5,-2,10,3,5],
        "InvoiceDate":   ["2023-01-15 10:00:00","2023-02-20 14:30:00",
                          "not-a-date","2023-03-01 09:00:00","2023-01-15 10:00:00"],
        "UnitPrice":     [2.5,1.0,3.0,-1.5,2.5],
        "CustomerID":    ["C001","C002","C003","C004","C001"],
        "Country":       ["United Kingdom"]*5,
        "Status":        ["completed","cancelled","refunded","completed","completed"],
        "PaymentMethod": ["card"]*5,
    })


def _holidays():
    return pd.DataFrame({
        "country_code": ["GB","DE"],
        "date":         ["2023-01-02","2023-01-06"],
        "local_name":   ["New Year (obs.)","Heilige Drei Könige"],
        "name":         ["New Year's Day","Epiphany"],
        "is_global":    [True,True],
    })


# clean_customers 
#-----------------------------------------------------------------------------------

class TestCleanCustomers(unittest.TestCase):
    def setUp(self):
        self.clean, self.rejected = clean_customers(_raw_customers())

    def test_rejects_null_customer_id(self):
        self.assertTrue(self.rejected["CustomerID"].isna().all())
        self.assertEqual(self.clean["CustomerID"].isna().sum(), 0)

    def test_deduplicates_on_customer_id(self):
        self.assertEqual(self.clean["CustomerID"].duplicated().sum(), 0)

    def test_normalises_country_to_title_case(self):
        self.assertNotIn("UNITED KINGDOM", self.clean["Country"].values)
        self.assertIn("United Kingdom", self.clean["Country"].values)

    def test_lowercases_segment(self):
        self.assertTrue(self.clean["Segment"].str.islower().all())

    def test_strips_whitespace_from_id(self):
        self.assertIn("C003", self.clean["CustomerID"].values)


#  clean_transactions 
#-----------------------------------------------------------------------------------

class TestCleanTransactions(unittest.TestCase):
    def setUp(self):
        self.clean, self.rejected = clean_transactions(_raw_transactions())

    def test_rejects_bad_date(self):
        reasons = self.rejected.get("_reject_reason", pd.Series(dtype=str))
        self.assertTrue(any("bad_date" in str(r) for r in reasons))

    def test_rejects_negative_price(self):
        self.assertEqual((self.clean["UnitPrice"] < 0).sum(), 0)
        self.assertGreater(len(self.rejected), 0)

    def test_deduplicates_natural_key(self):
        self.assertEqual(
            self.clean.duplicated(subset=["InvoiceNo","StockCode"]).sum(), 0
        )

    def test_derives_line_total(self):
        self.assertIn("LineTotal", self.clean.columns)
        expected = (self.clean["Quantity"] * self.clean["UnitPrice"]).round(4)
        pd.testing.assert_series_equal(
            self.clean["LineTotal"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_derives_date_parts(self):
        for col in ("TxYear","TxMonth","TxDay","TxDOW"):
            self.assertIn(col, self.clean.columns)

    def test_normalises_description_to_title_case(self):
        titles = self.clean["Description"].dropna()
        self.assertTrue(all(t[0].isupper() for t in titles if t))


# join & enrich 
#-----------------------------------------------------------------------------------

class TestJoin(unittest.TestCase):
    def _make(self):
        tx = pd.DataFrame({"InvoiceNo":["I1","I2","I3"],"CustomerID":["C1","C2","C_GHOST"]})
        cu = pd.DataFrame({"CustomerID":["C1","C2"],"Country":["UK","DE"],
                           "SignupDate":[None,None],"CustomerSegment":["retail","online"],
                           "CustomerIsActive":[True,True]})
        return tx, cu

    def test_preserves_all_tx_rows(self):
        tx, cu = self._make()
        result = join_transactions_customers(tx, cu)
        self.assertEqual(len(result), len(tx))

    def test_flags_unknown_customers(self):
        tx, cu = self._make()
        result = join_transactions_customers(tx, cu)
        ghost = result[result["CustomerID"] == "C_GHOST"]
        self.assertFalse(ghost["is_known_customer"].iloc[0])


class TestEnrichHolidays(unittest.TestCase):
    def test_flags_holiday_row(self):
        tx = pd.DataFrame({
            "InvoiceNo":   ["I1","I2"],
            "InvoiceDate": ["2023-01-02","2023-06-15"],
            "Country":     ["United Kingdom","United Kingdom"],
        })
        result = enrich_holidays(tx, _holidays())
        self.assertTrue(result.loc[0, "is_holiday"])
        self.assertFalse(result.loc[1, "is_holiday"])

    def test_handles_none_holidays(self):
        tx = pd.DataFrame({"InvoiceNo":["I1"],"InvoiceDate":["2023-01-01"],"Country":["UK"]})
        result = enrich_holidays(tx, None)
        self.assertEqual(result["is_holiday"].sum(), 0)


class TestDeriveColumns(unittest.TestCase):
    def test_adds_tenure_days(self):
        df = pd.DataFrame({"InvoiceDate":["2023-06-01"],"SignupDate":["2022-06-01"],"Country":["UK"]})
        result = derive_columns(df)
        self.assertIn("customer_tenure_days", result.columns)
        self.assertEqual(result["customer_tenure_days"].iloc[0], 365)

    def test_adds_week_number(self):
        df = pd.DataFrame({"InvoiceDate":["2023-01-09"]})  # ISO week 2
        result = derive_columns(df)
        self.assertEqual(result["TxWeek"].iloc[0], 2)

    def test_adds_country_iso2(self):
        df = pd.DataFrame({"Country":["United Kingdom","Germany","Unknown"]})
        result = derive_columns(df)
        self.assertEqual(result.loc[0,"CountryISO2"], "GB")
        self.assertEqual(result.loc[1,"CountryISO2"], "DE")
        self.assertTrue(pd.isna(result.loc[2,"CountryISO2"]))


if __name__ == "__main__":
    unittest.main()
