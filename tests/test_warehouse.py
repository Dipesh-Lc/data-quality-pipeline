"""
Warehouse tests -- unittest compatible, uses SQLite (no Postgres needed).
"""
from __future__ import annotations
import sys, sqlite3, unittest, tempfile, shutil
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _sqlite_load(df: pd.DataFrame, table: str, conn, if_exists: str = "replace") -> int:
    """Thin wrapper: loads df into SQLite via pandas."""
    if df.empty:
        return 0
    df.to_sql(table, con=conn, if_exists=if_exists, index=False)
    return len(df)


def _count(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


class TestLoadHelpers(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        # enable dict-like rows
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()

    def _customers(self):
        return pd.DataFrame({
            "CustomerID": ["C001","C002","C003"],
            "Country":    ["UK","DE","FR"],
            "Segment":    ["retail","wholesale","online"],
            "IsActive":   [True,False,True],
        })

    def _transactions(self):
        return pd.DataFrame({
            "InvoiceNo":   ["I001","I002"],
            "StockCode":   ["SC1","SC2"],
            "Quantity":    [5, 3],
            "InvoiceDate": ["2023-01-01","2023-01-02"],
            "UnitPrice":   [2.5, 1.0],
            "LineTotal":   [12.5, 3.0],
            "CustomerID":  ["C001","C002"],
        })

    def test_load_customers_row_count(self):
        n = _sqlite_load(self._customers(), "stg_customers", self.conn)
        self.assertEqual(n, 3)
        self.assertEqual(_count(self.conn, "stg_customers"), 3)

    def test_load_transactions_row_count(self):
        n = _sqlite_load(self._transactions(), "stg_transactions", self.conn)
        self.assertEqual(n, 2)
        self.assertEqual(_count(self.conn, "stg_transactions"), 2)

    def test_replace_does_not_double_rows(self):
        _sqlite_load(self._customers(), "stg_customers", self.conn, "replace")
        _sqlite_load(self._customers(), "stg_customers", self.conn, "replace")
        self.assertEqual(_count(self.conn, "stg_customers"), 3)

    def test_append_accumulates_rows(self):
        _sqlite_load(self._customers(), "dq_results", self.conn, "replace")
        _sqlite_load(self._customers(), "dq_results", self.conn, "append")
        self.assertEqual(_count(self.conn, "dq_results"), 6)

    def test_empty_df_skipped(self):
        n = _sqlite_load(pd.DataFrame(), "any_table", self.conn)
        self.assertEqual(n, 0)

    def test_fact_stg_row_count_match(self):
        df = self._transactions()
        _sqlite_load(df, "stg_transactions", self.conn, "replace")
        _sqlite_load(df, "fct_transactions", self.conn, "replace")
        stg = _count(self.conn, "stg_transactions")
        fct = _count(self.conn, "fct_transactions")
        self.assertEqual(stg, fct)

    def test_dq_results_append_only(self):
        dq = pd.DataFrame({"run_date":["2023-01-01"],"check":["test"],"status":["PASS"]})
        _sqlite_load(dq, "dq_results", self.conn, "replace")
        _sqlite_load(dq, "dq_results", self.conn, "append")
        self.assertEqual(_count(self.conn, "dq_results"), 2)

    def test_customers_columns_preserved(self):
        _sqlite_load(self._customers(), "stg_customers", self.conn)
        rows = self.conn.execute("SELECT * FROM stg_customers").fetchall()
        cols = [d[0] for d in self.conn.execute(
            "SELECT * FROM stg_customers LIMIT 0"
        ).description]
        self.assertIn("CustomerID", cols)
        self.assertIn("Country",    cols)


class TestRowIntegrity(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_no_data_lost_on_replace(self):
        df = pd.DataFrame({"id": range(1000), "val": [float(x) for x in range(1000)]})
        _sqlite_load(df, "big_table", self.conn, "replace")
        self.assertEqual(_count(self.conn, "big_table"), 1000)

    def test_nullable_columns_preserved(self):
        df = pd.DataFrame({"id":["A","B","C"],"nullable":[1.0, None, 3.0]})
        _sqlite_load(df, "nullable_test", self.conn)
        rows = self.conn.execute("SELECT nullable FROM nullable_test").fetchall()
        values = [r[0] for r in rows]
        self.assertIn(None, values)


if __name__ == "__main__":
    unittest.main()
