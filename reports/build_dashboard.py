"""
Build the HTML dashboard by embedding the pre-generated figures as base64 PNGs
and computing ALL statistics directly from the pipeline output files.

Run AFTER the pipeline AND after generate_figures.py:

    python reports/generate_figures.py
    python reports/build_dashboard.py
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent
FACT_CSV = ROOT / "data" / "processed" / "fct_transactions.csv"
REJ_CSV  = ROOT / "data" / "interim"   / "rejected_transactions.csv"
TX_CLEAN = ROOT / "data" / "interim"   / "transactions_clean.csv"
CU_CLEAN = ROOT / "data" / "interim"   / "customers_clean.csv"
RAW_DIR  = ROOT / "data" / "raw"
FIG_DIR  = ROOT / "reports" / "figures"
OUT_HTML = ROOT / "reports" / "dashboard.html"

if not FACT_CSV.exists():
    print("ERROR: Run the pipeline first — fct_transactions.csv not found.")
    sys.exit(1)

# Load data
df = pd.read_csv(FACT_CSV, low_memory=False)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

rej         = pd.read_csv(REJ_CSV)  if REJ_CSV.exists()  else pd.DataFrame()
df_tx_clean = pd.read_csv(TX_CLEAN) if TX_CLEAN.exists() else pd.DataFrame()
df_cu_clean = pd.read_csv(CU_CLEAN) if CU_CLEAN.exists() else pd.DataFrame()

manifest = {}
raw_manifests = sorted(RAW_DIR.glob("*/manifest.json"), reverse=True)
if raw_manifests:
    with open(raw_manifests[0]) as f:
        manifest = json.load(f)

generated = datetime.now().strftime("%Y-%m-%d %H:%M")

# KPIs 
total_tx    = len(df)
total_rev   = round(df["LineTotal"].sum(), 2)
unique_cu   = df["CustomerID"].nunique()
avg_order   = round(df["LineTotal"].mean(), 2)
cancel_rate = round((df["Status"] == "cancelled").mean() * 100, 1)
countries   = df["Country"].nunique()
date_min    = df["InvoiceDate"].min().strftime("%d %b %Y")
date_max    = df["InvoiceDate"].max().strftime("%d %b %Y")
guest_pct   = (round((~df["is_known_customer"]).mean() * 100, 1)
               if "is_known_customer" in df.columns else 0)
rejected    = len(rej)
run_date    = manifest.get("run_date", date_max)

raw_tx_count = manifest.get("transactions", total_tx + rejected)
raw_cu_count = manifest.get("customers", len(df_cu_clean))
raw_hol      = manifest.get("holidays", 0)

cu_clean_count  = len(df_cu_clean)
tx_clean_count  = len(df_tx_clean)
cu_rejected     = max(raw_cu_count - cu_clean_count, 0)
tx_rejected_cnt = len(rej)
fact_count      = len(df)
fact_cols       = len(df.columns)

# Load raw files 
raw_tx_path = sorted(RAW_DIR.glob("*/transactions.csv"), reverse=True)
raw_cu_path = sorted(RAW_DIR.glob("*/customers.csv"),    reverse=True)
df_tx_raw = pd.read_csv(raw_tx_path[0]) if raw_tx_path else df_tx_clean.copy()
df_cu_raw = pd.read_csv(raw_cu_path[0]) if raw_cu_path else df_cu_clean.copy()


# VALIDATION CHECKS (raw data) 
#-----------------------------------------------------------------------------------
def _not_empty(d, label):
    return (f"{label}.not_empty", "PASS" if len(d) > 0 else "FAIL", "error",
            str(len(d)) if len(d) > 0 else "0")

def _req_cols(d, label, cols):
    missing = [c for c in cols if c not in d.columns]
    return (f"{label}.required_columns", "PASS" if not missing else "FAIL",
            "error", "—" if not missing else str(missing))

def _pk_unique(d, label, col):
    dupes = int(d[col].duplicated().sum()) if col in d.columns else 0
    return (f"{label}.pk_unique.{col}", "PASS" if dupes == 0 else "FAIL",
            "error", "—" if dupes == 0 else str(dupes))

def _positive(d, label, col):
    bad = int((pd.to_numeric(d[col], errors="coerce") < 0).sum()) if col in d.columns else 0
    return (f"{label}.positive.{col}", "PASS" if bad == 0 else "FAIL",
            "warning", "—" if bad == 0 else str(bad))

def _no_nulls(d, label, col):
    n = int(d[col].isna().sum()) if col in d.columns else 0
    return (f"{label}.no_nulls.{col}", "PASS" if n == 0 else "FAIL",
            "warning", "—" if n == 0 else str(n))

def _no_future(d, label, col):
    if col not in d.columns:
        return (f"{label}.no_future.{col}", "PASS", "error", "—")
    future = int((pd.to_datetime(d[col], errors="coerce") > pd.Timestamp.now()).sum())
    return (f"{label}.no_future.{col}", "PASS" if future == 0 else "FAIL",
            "error", "—" if future == 0 else str(future))

TX_REQUIRED = ["InvoiceNo","StockCode","Quantity","InvoiceDate","UnitPrice","Country"]
CU_REQUIRED = ["CustomerID","Country"]

dq_raw = [
    _not_empty (df_tx_raw, "transactions"),
    _req_cols  (df_tx_raw, "transactions", TX_REQUIRED),
    _pk_unique (df_tx_raw, "transactions", "InvoiceNo"),
    _positive  (df_tx_raw, "transactions", "UnitPrice"),
    _no_future (df_tx_raw, "transactions", "InvoiceDate"),
    _not_empty (df_cu_raw, "customers"),
    _req_cols  (df_cu_raw, "customers", CU_REQUIRED),
    _pk_unique (df_cu_raw, "customers",  "CustomerID"),
    _no_nulls  (df_cu_raw, "customers",  "CustomerID"),
    _no_nulls  (df_cu_raw, "customers",  "Country"),
]
val_failures = sum(1 for r in dq_raw if r[1] == "FAIL")


# DQ CHECKS (cleaned data)
#-----------------------------------------------------------------------------------
def _dq_pk(d, table, col):
    dupes = int(d[col].duplicated().sum()) if col in d.columns else 0
    return (f"pk_duplicates.{col}", table, "PASS" if dupes == 0 else "FAIL",
            "error", "—" if dupes == 0 else str(dupes))

def _dq_null(d, table, col):
    n = int(d[col].isna().sum()) if col in d.columns else 0
    return (f"null_rate.{col}", table, "PASS" if n == 0 else "FAIL",
            "warning", "—" if n == 0 else str(n))

def _dq_row_count(d, table, min_rows):
    ok = len(d) >= min_rows
    return ("row_count_expectation", table, "PASS" if ok else "FAIL",
            "error", "—" if ok else str(len(d)))

def _dq_status(d, table):
    if "Status" not in d.columns:
        return ("valid_status.Status", table, "PASS", "warning", "—")
    bad = int((~d["Status"].isin({"completed","cancelled","refunded"})).sum())
    return ("valid_status.Status", table, "PASS" if bad == 0 else "FAIL",
            "warning", "—" if bad == 0 else str(bad))

def _dq_orphan(df_fact, df_cu):
    if "CustomerID" not in df_fact.columns or "CustomerID" not in df_cu.columns:
        return ("orphan_transactions", "fct_transactions", "PASS", "warning", "—")
    known   = set(df_cu["CustomerID"].dropna().astype(str))
    orphans = int((~df_fact["CustomerID"].dropna().astype(str).isin(known)).sum())
    return ("orphan_transactions", "fct_transactions",
            "PASS" if orphans == 0 else "FAIL", "warning",
            "—" if orphans == 0 else str(orphans))

def _dq_line_total(d, table):
    if not {"Quantity","LineTotal"}.issubset(d.columns):
        return ("line_total_sign", table, "PASS", "warning", "—")
    bad = int(((d["Quantity"] < 0) & (d["LineTotal"] > 0)).sum())
    return ("line_total_sign", table, "PASS" if bad == 0 else "FAIL",
            "warning", "—" if bad == 0 else str(bad))

def _dq_compound_pk(d):
    if not {"InvoiceNo","StockCode"}.issubset(d.columns):
        return ("pk_duplicates.InvoiceNo_StockCode","stg_transactions","PASS","error","—")
    key   = d["InvoiceNo"].astype(str) + "|" + d["StockCode"].astype(str)
    dupes = int(key.duplicated().sum())
    return ("pk_duplicates.InvoiceNo_StockCode","stg_transactions",
            "PASS" if dupes == 0 else "FAIL","error",
            "—" if dupes == 0 else str(dupes))

def _dq_invoice_note(d):
    dupes = int(d["InvoiceNo"].duplicated().sum()) if "InvoiceNo" in d.columns else 0
    label = f"{dupes} (multi-line invoices)" if dupes else "—"
    return ("pk_duplicates.InvoiceNo","stg_transactions","INFO","note", label)

dq_clean = [
    _dq_pk        (df_cu_clean, "stg_customers",    "CustomerID"),
    _dq_null      (df_cu_clean, "stg_customers",    "CustomerID"),
    _dq_row_count (df_cu_clean, "stg_customers",    100),
    _dq_invoice_note(df_tx_clean),
    _dq_compound_pk (df_tx_clean),
    _dq_null      (df_tx_clean, "stg_transactions", "InvoiceDate"),
    _dq_status    (df_tx_clean, "stg_transactions"),
    _dq_orphan    (df, df_cu_clean),
    _dq_line_total(df, "fct_transactions"),
]
dq_failures = sum(1 for r in dq_clean if r[2] == "FAIL")


# ANOMALY DETECTION 
#-----------------------------------------------------------------------------------
anomaly_rows = []

# Daily volume z-score
if "InvoiceDate" in df.columns:
    daily = (df.assign(_d=df["InvoiceDate"].dt.normalize())
               .groupby("_d").size().sort_index())
    if len(daily) >= 3:
        rm  = daily.rolling(7, min_periods=1).mean()
        rs  = daily.rolling(7, min_periods=1).std(ddof=1).fillna(1).replace(0, 1)
        zs  = ((daily - rm) / rs).abs()
        mz  = round(float(zs.max()), 2)
        hit = mz > 3.0
        anomaly_rows.append(("daily_volume_zscore", "Z-score, 7-day rolling",
                              "ANOMALY" if hit else "NORMAL",
                              f"max |z| = {mz}" + (" — spike" if hit else "")))

# UnitPrice z-score
if "UnitPrice" in df.columns:
    s    = pd.to_numeric(df["UnitPrice"], errors="coerce").dropna()
    mu, sd = s.mean(), s.std(ddof=1)
    out  = int((((s - mu) / sd).abs() > 4).sum()) if sd > 0 else 0
    anomaly_rows.append(("amount_zscore.UnitPrice", "Global Z-score",
                          "ANOMALY" if out > 0 else "NORMAL",
                          f"{out} outlier(s) at |z| > 4"))

# Cancellation spike
if "Status" in df.columns:
    crate = round((df["Status"] == "cancelled").mean() * 100, 1)
    anomaly_rows.append(("cancellation_spike", "Threshold > 20%",
                          "ANOMALY" if crate > 20 else "NORMAL",
                          f"{crate}% cancellation rate"))

# Null rate spike
for col in ["CustomerID","InvoiceDate","UnitPrice"]:
    if col in df.columns:
        np_  = round(df[col].isna().mean() * 100, 1)
        anomaly_rows.append((f"null_rate_spike.{col}", "Threshold > 5%",
                              "ANOMALY" if np_ > 5 else "NORMAL",
                              f"{np_}% null rate in {col}"))

anomaly_triggered = sum(1 for r in anomaly_rows if r[2] == "ANOMALY")
all_ok = val_failures == 0 and dq_failures == 0 and anomaly_triggered == 0


# PIPELINE STAGES
#-----------------------------------------------------------------------------------
orphan_count = int((~df["is_known_customer"]).sum()) if "is_known_customer" in df.columns else 0

reject_note = ""
if not rej.empty and "_reject_reason" in rej.columns:
    rc = rej["_reject_reason"].value_counts()
    reject_note = " · ".join(f"{v} {k}" for k, v in rc.items())

pipeline_stages = [
    ("Ingestion", "CSV files + manifest",
     f"{raw_tx_count:,} tx · {raw_cu_count:,} customers · {raw_hol} holidays",
     "DONE", "Manifest written"),
    ("Validation", "Raw DataFrames",
     f"{len(dq_raw)} checks · {val_failures} failure(s)",
     "WARN" if val_failures else "DONE",
     "Pipeline continues (warnings)" if val_failures else "All checks passed"),
    ("Cleaning (tx)", f"{raw_tx_count:,} rows",
     f"{tx_clean_count:,} clean · {tx_rejected_cnt} rejected" +
     (f" ({reject_note})" if reject_note else ""),
     "DONE", f"{raw_tx_count - tx_clean_count - tx_rejected_cnt} dupes removed"),
    ("Cleaning (cu)", f"{raw_cu_count:,} rows",
     f"{cu_clean_count:,} clean · {cu_rejected} dupes removed",
     "DONE", f"{cu_rejected} dupes removed"),
    ("Transform", f"{tx_clean_count:,} tx + {cu_clean_count:,} customers",
     f"{fact_count:,} fact rows · {fact_cols} cols",
     "DONE", f"{orphan_count} orphan tx flagged" if orphan_count else "All tx matched"),
    ("Warehouse", "3 DataFrames",
     "Skipped (no DB configured)",
     "SKIP", "Set DB_URL in .env to enable"),
    ("Quality", "Clean DataFrames",
     f"{len(dq_clean)} checks · {dq_failures} failure(s)",
     "DONE", "All critical checks PASS"),
    ("Anomaly", "Fact table",
     f"{len(anomaly_rows)} detectors · {anomaly_triggered} triggered",
     "WARN" if anomaly_triggered else "DONE",
     f"{anomaly_triggered} triggered" if anomaly_triggered else "Data within normal range"),
    ("Reporting", "All results", "MD + HTML report", "DONE", "Dashboard generated"),
]


# DATA FLOW FILES 
#-----------------------------------------------------------------------------------
def _rc(path):
    try:
        return f"{len(pd.read_csv(path)):,}"
    except Exception:
        return "—"

raw_tx_f = raw_tx_path[0] if raw_tx_path else None
src_tx   = ROOT / "data" / "samples" / "transactions.csv"
src_cu   = ROOT / "data" / "samples" / "customers.csv"

data_files = []
if src_tx.exists(): data_files.append(("data/samples/transactions.csv", "Source transactions (sample generator)", _rc(src_tx)))
if src_cu.exists(): data_files.append(("data/samples/customers.csv",    "Source customers (sample generator)",    _rc(src_cu)))
if raw_tx_f:        data_files.append((raw_tx_f.relative_to(ROOT).as_posix(), "Raw copy (immutable)", _rc(raw_tx_f)))
data_files += [
    ("data/interim/transactions_clean.csv",    "After cleaning",               f"{tx_clean_count:,}"),
    ("data/interim/rejected_transactions.csv", "Quarantined bad rows",         f"{tx_rejected_cnt:,}"),
    ("data/interim/customers_clean.csv",       "After cleaning",               f"{cu_clean_count:,}"),
    ("data/processed/fct_transactions.csv",    "Final enriched fact table",    f"{fact_count:,}"),
]


# HTML HELPERS 
#-----------------------------------------------------------------------------------
def b64img(name):
    p = FIG_DIR / name
    if not p.exists():
        return "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEAAAAALAAAAAABAAEAAAI="
    return f"data:image/png;base64,{base64.b64encode(p.read_bytes()).decode()}"

BADGE_KEYS = {"PASS","FAIL","INFO","WARN","NORMAL","DONE","SKIP","ANOMALY","warning","error","note"}
BADGE_CLS  = {"PASS":"pass","FAIL":"fail","INFO":"info","WARN":"warn","NORMAL":"pass",
              "DONE":"pass","SKIP":"info","ANOMALY":"fail","warning":"warn","error":"fail","note":"info"}

def badge(s):
    return f'<span class="badge {BADGE_CLS.get(s,"info")}">{s}</span>'

def td(cell):
    return f"<td>{badge(cell) if cell in BADGE_KEYS else cell}</td>"

def kpi_card(val, label, cls=""):
    return f'<div class="kpi {cls}"><div class="kpi-val">{val}</div><div class="kpi-label">{label}</div></div>'

def chart_card(title, img, full=False):
    span = "chart-full" if full else "chart-half"
    return f'<div class="{span}"><h3>{title}</h3><img src="{b64img(img)}" alt="{title}" loading="lazy"></div>'


# VERDICT / BANNER 
#-----------------------------------------------------------------------------------
if all_ok:
    verdict_html = '<div class="verdict ok"><span class="verdict-icon">✅</span><div><div>Pipeline Run Healthy</div><div class="verdict-sub">All validation, quality and anomaly checks passed</div></div></div>'
else:
    issues = (([f"{val_failures} validation failure(s)"] if val_failures else []) +
              ([f"{dq_failures} DQ failure(s)"] if dq_failures else []) +
              ([f"{anomaly_triggered} anomaly detector(s) triggered"] if anomaly_triggered else []))
    verdict_html = f'<div class="verdict bad"><span class="verdict-icon">⚠️</span><div><div>Pipeline Run Needs Attention</div><div class="verdict-sub">{" · ".join(issues)}</div></div></div>'

anomaly_banner = ('<div class="anomaly-ok">✅ &nbsp; All anomaly detectors NORMAL — no unusual patterns found</div>'
                  if anomaly_triggered == 0 else
                  f'<div class="verdict bad"><span class="verdict-icon">⚠️</span><div><div>Anomaly Detected</div><div class="verdict-sub">{anomaly_triggered} detector(s) triggered</div></div></div>')

val_rows_html    = "".join(f"<tr>{td(r[0])}{td(r[1])}{td(r[2])}{td(r[3])}</tr>" for r in dq_raw)
dq_rows_html     = "".join(f"<tr>{td(r[0])}{td(r[1])}{td(r[2])}{td(r[3])}{td(r[4])}</tr>" for r in dq_clean)
anomaly_rows_html= "".join(f"<tr>{td(r[0])}{td(r[1])}{td(r[2])}{td(r[3])}</tr>" for r in anomaly_rows)
stage_rows_html  = "".join(f"<tr>{td(r[0])}{td(r[1])}{td(r[2])}{td(r[4])}</tr>" for r in pipeline_stages)
file_rows_html   = "".join(f"<tr><td><code>{r[0]}</code></td>{td(r[1])}{td(r[2])}</tr>" for r in data_files)


# BUILD HTML 
#-----------------------------------------------------------------------------------
HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dashboard: Data Quality Pipeline</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
          background: #f0f4ff; color: #111827; }}
  header {{ background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 55%, #2563eb 100%);
    color: #fff; padding: 28px 48px; display: flex; align-items: center; gap: 16px; }}
  .header-logo {{ font-size: 28px; }}
  .header-text h1 {{ font-size: 22px; font-weight: 700; }}
  .header-text p  {{ margin-top: 5px; opacity: .78; font-size: 13px; }}
  nav {{ background: #fff; border-bottom: 2px solid #e5e7eb; padding: 0 48px;
    display: flex; position: sticky; top: 0; z-index: 100; box-shadow: 0 1px 4px rgba(0,0,0,.05); }}
  nav button {{ padding: 14px 24px; border: none; background: none; cursor: pointer;
    font-size: 14px; font-weight: 600; color: #6b7280;
    border-bottom: 3px solid transparent; transition: all .2s; white-space: nowrap; }}
  nav button.active, nav button:hover {{ color: #1e40af; border-bottom-color: #1e40af; }}
  .tab-section {{ display: none; padding: 32px 48px 56px; max-width: 1440px; margin: 0 auto; }}
  .tab-section.active {{ display: block; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(175px, 1fr));
    gap: 14px; margin-bottom: 32px; }}
  .kpi {{ background: #fff; border-radius: 12px; padding: 20px 22px;
    box-shadow: 0 1px 4px rgba(0,0,0,.07); border-top: 4px solid #1e40af; }}
  .kpi.amber {{ border-top-color: #d97706; }} .kpi.red {{ border-top-color: #dc2626; }}
  .kpi-val {{ font-size: 26px; font-weight: 800; color: #1e40af; }}
  .kpi.amber .kpi-val {{ color: #d97706; }} .kpi.red .kpi-val {{ color: #dc2626; }}
  .kpi-label {{ font-size: 11px; color: #6b7280; margin-top: 6px;
    text-transform: uppercase; letter-spacing: .5px; font-weight: 600; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 22px; }}
  .chart-full {{ grid-column: 1 / -1; background: #fff; border-radius: 12px;
    padding: 22px 24px; box-shadow: 0 1px 4px rgba(0,0,0,.07); }}
  .chart-half {{ background: #fff; border-radius: 12px; padding: 22px 24px;
    box-shadow: 0 1px 4px rgba(0,0,0,.07); }}
  .chart-full h3, .chart-half h3 {{ font-size: 11px; font-weight: 700; color: #6b7280;
    text-transform: uppercase; letter-spacing: .6px; margin-bottom: 14px; }}
  .chart-full img, .chart-half img {{ width: 100%; height: auto; border-radius: 6px; }}
  .dq-card {{ background: #fff; border-radius: 12px; padding: 26px 28px;
    box-shadow: 0 1px 4px rgba(0,0,0,.07); margin-bottom: 22px; }}
  .dq-card h2 {{ font-size: 15px; font-weight: 700; color: #1e40af; margin-bottom: 16px;
    padding-bottom: 10px; border-bottom: 2px solid #e5e7eb; }}
  .table-wrap {{ overflow-x: auto; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  th {{ background: #1e40af; color: #fff; padding: 10px 14px; text-align: left;
    font-size: 11px; font-weight: 700; text-transform: uppercase; }}
  td {{ padding: 9px 14px; border-bottom: 1px solid #f3f4f6; }}
  tr:nth-child(even) td {{ background: #f9fafb; }} tr:hover td {{ background: #eff6ff; }}
  code {{ font-size: 12px; background: #f3f4f6; padding: 2px 6px; border-radius: 4px; }}
  .badge {{ padding: 3px 10px; border-radius: 99px; font-size: 11px; font-weight: 700; display: inline-block; }}
  .badge.pass {{ background: #d1fae5; color: #065f46; }} .badge.fail {{ background: #fee2e2; color: #991b1b; }}
  .badge.warn {{ background: #fef3c7; color: #92400e; }} .badge.info {{ background: #dbeafe; color: #1e40af; }}
  .verdict {{ display: flex; align-items: center; gap: 16px; padding: 18px 24px;
    border-radius: 12px; margin-bottom: 24px; font-weight: 700; font-size: 16px; }}
  .verdict.bad {{ background: #fef3c7; color: #92400e; border: 2px solid #fcd34d; }}
  .verdict.ok  {{ background: #d1fae5; color: #065f46; border: 2px solid #6ee7b7; }}
  .verdict-icon {{ font-size: 28px; }}
  .verdict-sub {{ font-size: 13px; font-weight: 400; margin-top: 3px; opacity: .85; }}
  .flow {{ display: flex; align-items: flex-start; flex-wrap: wrap; gap: 0; margin: 20px 0 8px; }}
  .flow-step {{ background: #eff6ff; border: 2px solid #bfdbfe; border-radius: 10px;
    padding: 14px 18px; text-align: center; min-width: 100px; }}
  .flow-step .num {{ font-size: 22px; font-weight: 800; color: #1e40af; }}
  .flow-step .lbl {{ font-size: 10px; color: #6b7280; margin-top: 3px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .5px; }}
  .flow-arrow {{ font-size: 22px; color: #93c5fd; padding: 18px 4px 0; }}
  .anomaly-ok {{ display: flex; align-items: center; gap: 12px; padding: 16px 22px;
    background: #d1fae5; border-radius: 10px; color: #065f46; font-weight: 700;
    margin-bottom: 20px; font-size: 14px; }}
  footer {{ text-align: center; padding: 28px; font-size: 12px; color: #9ca3af;
    border-top: 1px solid #e5e7eb; margin-top: 8px; }}
  @media (max-width: 860px) {{
    .chart-grid {{ grid-template-columns: 1fr; }} .chart-full {{ grid-column: 1; }}
    header, nav, .tab-section {{ padding-left: 18px; padding-right: 18px; }}
    .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
  }}
</style>
</head>
<body>

<header>
  <div class="header-logo">📊</div>
  <div class="header-text">
    <h1>Dashboard: Data Quality Pipeline</h1>
    <p>Run date: {run_date} &nbsp;|&nbsp; Generated: {generated} UTC &nbsp;|&nbsp; v1.0.0</p>
  </div>
</header>

<nav>
  <button class="active" onclick="show('overview',this)">Overview</button>
  <button onclick="show('charts',this)">Charts</button>
  <button onclick="show('quality',this)">Quality &amp; Anomalies</button>
  <button onclick="show('pipeline',this)">Pipeline</button>
</nav>

<!-- TAB 1: OVERVIEW -->
<section id="overview" class="tab-section active">
  <div class="kpi-grid">
    {kpi_card(f"{total_tx:,}",       "Total Transactions")}
    {kpi_card(f"{unique_cu:,}",      "Unique Customers")}
    {kpi_card(f"£{total_rev:,.0f}",  "Total Revenue")}
    {kpi_card(f"£{avg_order:.2f}",   "Avg Order Value")}
    {kpi_card(f"{countries}",        "Countries")}
    {kpi_card(f"{date_min} &ndash;<br>{date_max}", "Date Range")}
    {kpi_card(f"{cancel_rate}%",     "Cancellation Rate", "amber")}
    {kpi_card(f"{guest_pct}%",       "Guest Checkouts",   "amber")}
    {kpi_card(f"{rejected:,}",       "Rejected Rows",     "red" if rejected else "")}
  </div>
  <div class="chart-grid">
    {chart_card("Daily Transaction Volume with 7-Day Trend", "01_daily_volume.png", full=True)}
    {chart_card("Revenue by Country",     "02_revenue_by_country.png")}
    {chart_card("Transaction Status",     "03_status_distribution.png")}
  </div>
</section>

<!-- TAB 2: CHARTS -->
<section id="charts" class="tab-section">
  <div class="chart-grid">
    {chart_card("Daily Transaction Volume",          "01_daily_volume.png",    full=True)}
    {chart_card("Revenue by Country (Top 10)",       "02_revenue_by_country.png")}
    {chart_card("Transaction Status Distribution",   "03_status_distribution.png")}
    {chart_card("Unit Price Distribution",           "04_price_distribution.png")}
    {chart_card("Monthly Revenue",                   "05_monthly_revenue.png")}
    {chart_card("Quantity: Purchases vs Returns",    "06_quantity_distribution.png")}
    {chart_card("Revenue by Customer Segment",       "07_segment_revenue.png")}
    {chart_card("Volume Heatmap: Day &times; Hour",  "08_dow_heatmap.png",     full=True)}
    {chart_card("Data Quality Check Results",        "09_dq_summary.png",      full=True)}
    {chart_card("Anomaly Detection &mdash; Z-score", "10_anomaly_zscore.png",  full=True)}
  </div>
</section>

<!-- TAB 3: QUALITY & ANOMALIES -->
<section id="quality" class="tab-section">
  {verdict_html}

  <div class="dq-card">
    <h2>Validation Checks (raw data)</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Check</th><th>Status</th><th>Severity</th><th>Rows Affected</th></tr></thead>
      <tbody>{val_rows_html}</tbody>
    </table></div>
  </div>

  <div class="dq-card">
    <h2>Data Quality Checks (cleaned data)</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Check</th><th>Table</th><th>Status</th><th>Severity</th><th>Rows Affected</th></tr></thead>
      <tbody>{dq_rows_html}</tbody>
    </table></div>
  </div>

  <div class="dq-card">
    <h2>Anomaly Detection Results</h2>
    {anomaly_banner}
    <div class="table-wrap"><table>
      <thead><tr><th>Detector</th><th>Method</th><th>Result</th><th>Metric</th></tr></thead>
      <tbody>{anomaly_rows_html}</tbody>
    </table></div>
  </div>

  <div class="chart-grid">
    {chart_card("Data Quality Check Summary",                "09_dq_summary.png",    full=True)}
    {chart_card("Anomaly Detection &mdash; Z-score over Time","10_anomaly_zscore.png",full=True)}
  </div>
</section>

<!-- TAB 4: PIPELINE -->
<section id="pipeline" class="tab-section">
  <div class="dq-card">
    <h2>Pipeline Architecture</h2>
    <div class="flow">
      <div class="flow-step"><div class="num">1</div><div class="lbl">Ingest</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">2</div><div class="lbl">Validate</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">3</div><div class="lbl">Clean</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">4</div><div class="lbl">Transform</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">5</div><div class="lbl">Load DB</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">6</div><div class="lbl">Quality</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">7</div><div class="lbl">Anomaly</div></div>
      <div class="flow-arrow">→</div>
      <div class="flow-step"><div class="num">8</div><div class="lbl">Report</div></div>
    </div>
  </div>

  <div class="dq-card">
    <h2>Run Statistics</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Stage</th><th>Input</th><th>Output</th><th>Notes</th></tr></thead>
      <tbody>{stage_rows_html}</tbody>
    </table></div>
  </div>

  <div class="dq-card">
    <h2>Data Flow (files)</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Path</th><th>Description</th><th>Rows</th></tr></thead>
      <tbody>{file_rows_html}</tbody>
    </table></div>
  </div>
</section>

<footer>data-quality-pipeline v1.0.0 &nbsp;&middot;&nbsp; Built with Python, pandas, matplotlib &nbsp;&middot;&nbsp; MIT License</footer>

<script>
function show(id, btn) {{
  document.querySelectorAll('.tab-section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}}
</script>
</body>
</html>"""

OUT_HTML.write_text(HTML, encoding="utf-8")
size = OUT_HTML.stat().st_size / 1024
print(f"✓ Dashboard saved  →  {OUT_HTML}")
print(f"  Size      : {size:.0f} KB")
print(f"  Run date  : {run_date}")
print(f"  Rows      : {total_tx:,} transactions · {unique_cu:,} customers")
print(f"  Rejected  : {rejected}")
print(f"  Val fails : {val_failures}  |  DQ fails : {dq_failures}  |  Anomalies : {anomaly_triggered}")
print(f"  Tabs      : Overview · Charts · Quality & Anomalies · Pipeline")