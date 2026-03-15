"""Tests for quality checks and anomaly detection — unittest compatible."""
from __future__ import annotations
import sys, unittest
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.monitoring.anomaly_detection import (
    AnomalyResult, detect_amount_anomaly, detect_cancellation_spike,
    detect_daily_volume_anomaly, detect_null_rate_spike,
    results_to_df as anomaly_to_df, run_anomaly_detection,
)
from src.monitoring.quality_checks import (
    QualityResult, check_line_total_sign, check_null_rates,
    check_orphan_transactions, check_pk_duplicates, check_valid_status,
    results_to_df as dq_to_df, run_quality_checks,
)


def _clean_fact(n=60):
    rng = pd.date_range("2023-01-01", periods=n, freq="D")
    return pd.DataFrame({
        "InvoiceNo":   [f"INV{i:04d}" for i in range(n)],
        "CustomerID":  [f"C{1000+(i%20):05d}" for i in range(n)],
        "UnitPrice":   [round(2.5+(i%5)*0.5,2) for i in range(n)],
        "Quantity":    [1+(i%10) for i in range(n)],
        "LineTotal":   [(2.5+(i%5)*0.5)*(1+i%10) for i in range(n)],
        "InvoiceDate": rng,
        "Country":     ["United Kingdom"]*n,
        "Status":      ["completed" if i%10!=0 else "cancelled" for i in range(n)],
    })


def _clean_customers(n=20):
    return pd.DataFrame({
        "CustomerID": [f"C{1000+i:05d}" for i in range(n)],
        "Country":    ["United Kingdom"]*n,
    })


# Quality checks 
#----------------------------------------------------------------------------------

class TestQualityChecks(unittest.TestCase):
    def setUp(self):
        self.fact = _clean_fact()
        self.cust = _clean_customers()

    def test_pk_duplicates_passes(self):
        self.assertTrue(check_pk_duplicates(self.fact, "fct", "InvoiceNo").passed)

    def test_pk_duplicates_fails(self):
        df = pd.DataFrame({"InvoiceNo":["I1","I1","I2"]})
        r  = check_pk_duplicates(df, "fct", "InvoiceNo")
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)

    def test_orphan_passes_no_orphans(self):
        r = check_orphan_transactions(self.fact, self.cust)
        self.assertTrue(r.passed)

    def test_orphan_fails_with_orphans(self):
        df_tx = pd.DataFrame({"CustomerID":["C01000","GHOST"],"InvoiceNo":["I1","I2"]})
        r = check_orphan_transactions(df_tx, self.cust)
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)

    def test_null_rates_passes(self):
        results = check_null_rates(self.fact, "fct", ["InvoiceNo","UnitPrice"])
        self.assertTrue(all(r.passed for r in results))

    def test_null_rates_fails_high_nulls(self):
        df = pd.DataFrame({"Amount":[None]*20+[1.0]*80})
        results = check_null_rates(df, "fct", ["Amount"], threshold=0.10)
        self.assertFalse(results[0].passed)

    def test_valid_status_passes(self):
        self.assertTrue(check_valid_status(self.fact, "fct").passed)

    def test_valid_status_fails_unknown(self):
        df = pd.DataFrame({"Status":["completed","UNKNOWN_STATUS"]})
        r  = check_valid_status(df, "fct")
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)

    def test_line_total_sign_passes(self):
        self.assertTrue(check_line_total_sign(self.fact, "fct").passed)

    def test_line_total_sign_detects_mismatch(self):
        df = pd.DataFrame({"Quantity":[-5,3],"LineTotal":[10.0,6.0]})
        r  = check_line_total_sign(df, "fct")
        self.assertFalse(r.passed)
        self.assertEqual(r.rows_affected, 1)

    def test_run_quality_checks_returns_list(self):
        results = run_quality_checks(self.fact, self.cust, self.fact)
        self.assertIsInstance(results, list)
        self.assertTrue(all(isinstance(r, QualityResult) for r in results))
        self.assertGreaterEqual(len(results), 5)

    def test_results_to_df(self):
        results = run_quality_checks(self.fact, self.cust, self.fact)
        df = dq_to_df(results, "2023-01-01")
        self.assertEqual(len(df), len(results))
        self.assertIn("check", df.columns)


# Anomaly detection 
#-----------------------------------------------------------------------------------

class TestAnomalyDetection(unittest.TestCase):
    def setUp(self):
        self.fact = _clean_fact()

    def test_daily_volume_no_anomaly_steady(self):
        results = detect_daily_volume_anomaly(self.fact)
        self.assertEqual(len([r for r in results if r.triggered]), 0)

    def test_daily_volume_detects_spike(self):
        normal = pd.DataFrame({
            "InvoiceDate": pd.date_range("2023-01-01", periods=29, freq="D").repeat(10)
        })
        spike = pd.DataFrame({
            "InvoiceDate": pd.date_range("2023-01-30", periods=1).repeat(200)
        })
        df = pd.concat([normal, spike], ignore_index=True)
        results = detect_daily_volume_anomaly(df, z_threshold=2.0)
        triggered = [r for r in results if r.triggered]
        self.assertGreaterEqual(len(triggered), 1)

    def test_amount_no_outliers(self):
        df = pd.DataFrame({"UnitPrice":[2.5]*100})
        self.assertFalse(detect_amount_anomaly(df, z_threshold=3.0)[0].triggered)

    def test_amount_detects_outlier(self):
        df = pd.DataFrame({"UnitPrice":[2.5]*99+[9999.99]})
        self.assertTrue(detect_amount_anomaly(df, z_threshold=3.0)[0].triggered)

    def test_cancellation_normal(self):
        df = pd.DataFrame({"Status":["completed"]*90+["cancelled"]*5})
        self.assertFalse(detect_cancellation_spike(df, threshold_pct=0.10).triggered)

    def test_cancellation_detected(self):
        df = pd.DataFrame({"Status":["completed"]*60+["cancelled"]*40})
        self.assertTrue(detect_cancellation_spike(df, threshold_pct=0.20).triggered)

    def test_null_rate_no_issue(self):
        results = detect_null_rate_spike(self.fact, ["InvoiceNo"], threshold=0.05)
        self.assertTrue(all(not r.triggered for r in results))

    def test_null_rate_spike_detected(self):
        df = pd.DataFrame({"CustomerID":[None]*50+["C001"]*50})
        results = detect_null_rate_spike(df, ["CustomerID"], threshold=0.05)
        self.assertTrue(any(r.triggered for r in results))

    def test_run_returns_list(self):
        results = run_anomaly_detection(self.fact)
        self.assertIsInstance(results, list)
        self.assertTrue(all(isinstance(r, AnomalyResult) for r in results))

    def test_results_to_df(self):
        results = run_anomaly_detection(self.fact)
        df = anomaly_to_df(results, "2023-01-01")
        self.assertEqual(len(df), len(results))
        self.assertIn("detector", df.columns)


if __name__ == "__main__":
    unittest.main()
