#data/samples/generate_samples.py

"""
Generate realistic sample data modelled on UCI Online Retail II.
Run once to populate data/samples/ before the pipeline ingests it.
"""
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

COUNTRIES = [
    "United Kingdom", "Germany", "France", "Netherlands", "Australia",
    "Spain", "Switzerland", "Belgium", "Portugal", "Norway",
    "Italy", "Denmark", "Sweden", "Finland", "Austria",
    "United States", "Japan", "Canada", "Singapore", "Brazil",
]

PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053",  "WHITE METAL LANTERN",                3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER",     2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE",3.39),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART",      3.39),
    ("22752",  "SET 7 BABUSHKA NESTING BOXES",        7.65),
    ("21730",  "GLASS STAR FROSTED T-LIGHT HOLDER",  4.25),
    ("22633",  "HAND WARMER UNION JACK",              1.85),
    ("22632",  "HAND WARMER RED POLKA DOT",           1.85),
    ("47566",  "PARTY BUNTING",                       4.95),
]

STATUS_WEIGHTS   = [0.88, 0.07, 0.05]
STATUS_VALUES    = ["completed", "cancelled", "refunded"]
PAYMENT_METHODS  = ["card", "paypal", "bank_transfer", "voucher"]


def _random_invoice() -> str:
    """Generate a random invoice number."""
    prefix = random.choice(["", "C"])  # "C" prefix = cancelled
    return prefix + str(random.randint(489400, 581590))


def generate_transactions(n: int = 5_000) -> pd.DataFrame:
    """Generate sample transactions."""
    start = datetime(2023, 1, 1)
    end   = datetime(2023, 12, 31)

    records = []
    customer_pool = [f"C{str(i).zfill(5)}" for i in range(12000, 18200)]
    # leave ~3 % of rows with null customer (guests)
    customer_pool += [None] * int(len(customer_pool) * 0.03)

    for _ in range(n):
        stock_code, description, unit_price = random.choice(PRODUCTS)
        quantity   = random.randint(-5, 50)           # negatives = returns
        invoice_dt = start + timedelta(
            seconds=random.randint(0, int((end - start).total_seconds()))
        )
        customer_id = random.choice(customer_pool)
        status      = random.choices(STATUS_VALUES, STATUS_WEIGHTS)[0]

        # inject a few dirty rows
        if random.random() < 0.01:
            unit_price = random.choice([-1.5, 0, None])
        if random.random() < 0.005:
            description = description.lower()   # inconsistent casing
        if random.random() < 0.003:
            invoice_dt  = datetime(2099, 1, 1)  # impossible future date

        records.append(
            {
                "InvoiceNo":     _random_invoice(),
                "StockCode":     stock_code,
                "Description":   description,
                "Quantity":      quantity,
                "InvoiceDate":   invoice_dt,
                "UnitPrice":     unit_price,
                "CustomerID":    customer_id,
                "Country":       random.choice(COUNTRIES),
                "Status":        status,
                "PaymentMethod": random.choices(PAYMENT_METHODS, [0.6, 0.2, 0.15, 0.05])[0],
            }
        )

    df = pd.DataFrame(records)
    # duplicate ~0.5 % rows
    dupes = df.sample(frac=0.005, random_state=SEED)
    df    = pd.concat([df, dupes], ignore_index=True)
    return df


def generate_customers(df_tx: pd.DataFrame) -> pd.DataFrame:
    """Generate sample customers from transaction customer IDs."""
    ids = df_tx["CustomerID"].dropna().unique().tolist()

    records = []
    base_date = datetime(2018, 1, 1)
    for cid in ids:
        signup = base_date + timedelta(days=random.randint(0, 1800))
        country = random.choice(COUNTRIES)

        # inject dirty data
        if random.random() < 0.02:
            country = country.upper()           # inconsistent casing
        if random.random() < 0.01:
            signup = None                       # missing signup date

        records.append(
            {
                "CustomerID":  cid,
                "Country":     country,
                "SignupDate":  signup,
                "Segment":     random.choice(["retail", "wholesale", "online"]),
                "IsActive":    random.choice([True, True, True, False]),
            }
        )

    df = pd.DataFrame(records)
    # add a few duplicate customers
    dupes = df.sample(n=min(20, len(df)), random_state=SEED)
    df    = pd.concat([df, dupes], ignore_index=True)
    return df


if __name__ == "__main__":
    out = Path(__file__).parent
    tx  = generate_transactions(5_000)
    cu  = generate_customers(tx)

    tx.to_csv(out / "transactions.csv", index=False)
    cu.to_csv(out / "customers.csv",    index=False)
    print(f"transactions : {len(tx):,} rows  →  {out/'transactions.csv'}")
    print(f"customers    : {len(cu):,} rows  →  {out/'customers.csv'}")
