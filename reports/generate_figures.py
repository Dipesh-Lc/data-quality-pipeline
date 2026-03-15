# reports/generate_figures.py

"""
Generate report figures from the processed fact table.
Saves PNGs to reports/figures/.
Run after the pipeline: python reports/generate_figures.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FIG_DIR = ROOT / "reports" / "figures"
FACT_CSV = ROOT / "data" / "processed" / "fct_transactions.csv"


#  Style
# ---------------------------------------------------------------------------------------

PALETTE = ["#1e40af", "#3b82f6", "#93c5fd", "#bfdbfe", "#dbeafe"]
BG = "#f9fafb"
ACCENT = "#1e40af"
WARN = "#d97706"
DANGER = "#dc2626"
SUCCESS = "#16a34a"

plt.rcParams.update(
    {
        "figure.facecolor": BG,
        "axes.facecolor": BG,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.edgecolor": "#9ca3af",
        "axes.labelcolor": "#374151",
        "xtick.color": "#6b7280",
        "ytick.color": "#6b7280",
        "text.color": "#111827",
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.titlepad": 12,
    }
)


def _save(fig, name: str) -> Path:
    """Save a figure and return its path."""
    p = FIG_DIR / name
    fig.savefig(p, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  ✓ {p.name}")
    return p


def load_data() -> pd.DataFrame:
    """Load the processed fact table."""
    df = pd.read_csv(FACT_CSV, low_memory=False)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["date"] = df["InvoiceDate"].dt.normalize()
    return df


#  Daily transaction volume
# ---------------------------------------------------------------------------------------

def fig_daily_volume(df: pd.DataFrame) -> None:
    """Plot daily transaction volume with a 7-day rolling mean."""
    daily = df.groupby("date").size().rename("tx_count").reset_index()
    daily["rolling7"] = daily["tx_count"].rolling(7, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(daily["date"], daily["tx_count"], alpha=0.25, color=ACCENT)
    ax.plot(daily["date"], daily["tx_count"], color=ACCENT, lw=1.2, alpha=0.7, label="Daily volume")
    ax.plot(daily["date"], daily["rolling7"], color=DANGER, lw=2.0, label="7-day rolling mean")
    ax.set_title("Daily Transaction Volume")
    ax.set_xlabel("Date")
    ax.set_ylabel("Transactions")
    ax.legend(frameon=False)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %Y"))
    fig.autofmt_xdate()
    _save(fig, "01_daily_volume.png")


#  Revenue by country
# ---------------------------------------------------------------------------------------

def fig_revenue_by_country(df: pd.DataFrame) -> None:
    """Plot top 10 countries by revenue."""
    rev = (
        df.groupby("Country")["LineTotal"]
        .sum()
        .sort_values(ascending=True)
        .tail(10)
    )
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(rev.index, rev.values, color=ACCENT, alpha=0.85, height=0.65)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x/1000:.0f}K"))
    ax.set_title("Top 10 Countries by Revenue")
    ax.set_xlabel("Total Revenue")
    for bar, val in zip(bars, rev.values):
        ax.text(
            val + rev.max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"£{val:,.0f}",
            va="center",
            fontsize=9,
            color="#374151",
        )
    _save(fig, "02_revenue_by_country.png")


#  Transaction status distribution
# ---------------------------------------------------------------------------------------

def fig_status_distribution(df: pd.DataFrame) -> None:
    """Plot transaction status distribution."""
    counts = df["Status"].value_counts()
    colors = [SUCCESS, DANGER, WARN, "#6b7280"][: len(counts)]

    fig, ax = plt.subplots(figsize=(6, 6))
    wedges, texts, autotexts = ax.pie(
        counts.values,
        labels=counts.index,
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.82,
        wedgeprops={"edgecolor": "white", "linewidth": 2},
    )
    for t in autotexts:
        t.set_fontsize(10)
        t.set_color("white")
        t.set_fontweight("bold")
    ax.set_title("Transaction Status Distribution")
    _save(fig, "03_status_distribution.png")


#  Unit price distribution
# ---------------------------------------------------------------------------------------

def fig_price_distribution(df: pd.DataFrame) -> None:
    """Plot unit price distribution on a log scale."""
    prices = df.loc[df["UnitPrice"] > 0, "UnitPrice"]

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(prices, bins=60, color=ACCENT, alpha=0.8, edgecolor="white")
    ax.set_xscale("log")
    ax.set_title("Unit Price Distribution (log scale)")
    ax.set_xlabel("Unit Price (£, log scale)")
    ax.set_ylabel("Count")
    median = prices.median()
    ax.axvline(median, color=DANGER, lw=2, linestyle="--", label=f"Median £{median:.2f}")
    ax.legend(frameon=False)
    _save(fig, "04_price_distribution.png")


#  Monthly revenue trend
# ---------------------------------------------------------------------------------------

def fig_monthly_revenue(df: pd.DataFrame) -> None:
    """Plot monthly revenue."""
    monthly = (
        df.assign(month=df["InvoiceDate"].dt.to_period("M"))
        .groupby("month")["LineTotal"]
        .sum()
        .reset_index()
    )
    monthly["month_dt"] = monthly["month"].dt.to_timestamp()

    fig, ax = plt.subplots(figsize=(11, 4))
    ax.bar(
        monthly["month_dt"],
        monthly["LineTotal"],
        color=ACCENT,
        alpha=0.85,
        width=20,
        align="center",
    )
    ax.set_title("Monthly Revenue")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue (£)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x/1000:.0f}K"))
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b"))
    fig.autofmt_xdate()
    _save(fig, "05_monthly_revenue.png")


#  Quantity distribution
# ---------------------------------------------------------------------------------------

def fig_quantity_dist(df: pd.DataFrame) -> None:
    """Plot quantity distribution for purchases and returns."""
    qty = df["Quantity"].clip(-50, 100)

    fig, ax = plt.subplots(figsize=(9, 4))
    neg_mask = qty < 0
    ax.hist(qty[~neg_mask], bins=40, color=ACCENT, alpha=0.8, label="Purchases (+)")
    ax.hist(qty[neg_mask], bins=20, color=DANGER, alpha=0.8, label="Returns (−)")
    ax.set_title("Quantity Distribution (purchases vs returns)")
    ax.set_xlabel("Quantity")
    ax.set_ylabel("Count")
    ax.legend(frameon=False)
    _save(fig, "06_quantity_distribution.png")


#  Customer segment revenue breakdown
# ---------------------------------------------------------------------------------------

def fig_segment_revenue(df: pd.DataFrame) -> None:
    """Plot revenue by customer segment."""
    if "CustomerSegment" not in df.columns:
        return

    seg = (
        df.dropna(subset=["CustomerSegment"])
        .groupby("CustomerSegment")["LineTotal"]
        .sum()
        .sort_values(ascending=False)
    )
    if seg.empty:
        return

    colors = [PALETTE[i % len(PALETTE)] for i in range(len(seg))]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(seg.index, seg.values, color=colors, alpha=0.9)
    ax.set_title("Revenue by Customer Segment")
    ax.set_xlabel("Segment")
    ax.set_ylabel("Revenue (£)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x/1000:.0f}K"))
    for bar, val in zip(bars, seg.values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val + seg.max() * 0.01,
            f"£{val:,.0f}",
            ha="center",
            fontsize=9,
            color="#374151",
        )
    _save(fig, "07_segment_revenue.png")


#  Day-of-week heatmap
# ---------------------------------------------------------------------------------------

def fig_dow_heatmap(df: pd.DataFrame) -> None:
    """Plot transaction volume by day of week and hour bin."""
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    hour_bins = list(range(0, 25, 2))
    hour_labels = [f"{h:02d}:00" for h in hour_bins[:-1]]

    if "TxDOW" not in df.columns:
        return

    df2 = df.copy()
    df2["hour"] = df2["InvoiceDate"].dt.hour
    df2["hour_bin"] = pd.cut(df2["hour"], bins=hour_bins, labels=hour_labels, right=False)
    df2["dow"] = pd.Categorical(df2["TxDOW"], categories=dow_order, ordered=True)
    heat = df2.groupby(["dow", "hour_bin"], observed=True).size().unstack(fill_value=0)

    fig, ax = plt.subplots(figsize=(13, 5))
    sns.heatmap(
        heat,
        ax=ax,
        cmap="Blues",
        linewidths=0.3,
        cbar_kws={"shrink": 0.8, "label": "Transactions"},
        annot=False,
    )
    ax.set_title("Transaction Volume: Day of Week × Hour of Day")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Day of Week")
    plt.xticks(rotation=45, ha="right")
    _save(fig, "08_dow_heatmap.png")


#  Data quality check summary
# ---------------------------------------------------------------------------------------

def fig_dq_summary() -> None:
    """Plot a static summary of validation and quality checks."""
    checks = {
        "not_empty": ("PASS", SUCCESS),
        "required_cols": ("PASS", SUCCESS),
        "pk_unique_tx": ("FAIL", DANGER),
        "pk_unique_cu": ("FAIL", DANGER),
        "no_future_dates": ("FAIL", DANGER),
        "no_neg_prices": ("FAIL", WARN),
        "null_rates": ("PASS", SUCCESS),
        "valid_status": ("PASS", SUCCESS),
        "orphan_tx": ("PASS", SUCCESS),
        "line_total_sign": ("PASS", SUCCESS),
    }
    labels = list(checks.keys())
    colors = [v[1] for v in checks.values()]
    vals = [1] * len(labels)

    fig, ax = plt.subplots(figsize=(11, 3))
    bars = ax.barh(labels, vals, color=colors, height=0.55)
    ax.set_xlim(0, 1.4)
    ax.set_xlabel("")
    ax.set_xticks([])
    ax.set_title("Validation & Quality Check Results")
    for bar, (name, (status, color)) in zip(bars, checks.items()):
        ax.text(
            1.02,
            bar.get_y() + bar.get_height() / 2,
            f"{'✓' if status == 'PASS' else '✗'}  {status}",
            va="center",
            fontsize=10,
            color=SUCCESS if status == "PASS" else DANGER,
        )
    _save(fig, "09_dq_summary.png")


#  Anomaly detection Z-score chart
# ---------------------------------------------------------------------------------------

def fig_anomaly_zscore(df: pd.DataFrame) -> None:
    """Plot daily volume and corresponding anomaly z-scores."""
    daily = df.groupby("date").size().rename("tx_count").sort_index()
    rolling_mean = daily.rolling(7, center=True, min_periods=1).mean()
    rolling_std = daily.rolling(7, center=True, min_periods=1).std(ddof=1).fillna(1)
    z_scores = (daily - rolling_mean) / rolling_std.replace(0, 1)

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

    axes[0].fill_between(daily.index, daily.values, alpha=0.2, color=ACCENT)
    axes[0].plot(daily.index, daily.values, color=ACCENT, lw=1.2, label="Daily volume")
    axes[0].plot(daily.index, rolling_mean, color=DANGER, lw=2, label="7-day mean")
    axes[0].fill_between(
        daily.index,
        rolling_mean - 3 * rolling_std,
        rolling_mean + 3 * rolling_std,
        alpha=0.12,
        color=DANGER,
        label="±3σ band",
    )
    axes[0].set_title("Daily Volume with Anomaly Detection Bands")
    axes[0].set_ylabel("Transactions")
    axes[0].legend(frameon=False, fontsize=9)

    colors_z = [DANGER if abs(z) > 3 else ACCENT for z in z_scores.values]
    axes[1].bar(z_scores.index, z_scores.values, color=colors_z, alpha=0.8, width=1)
    axes[1].axhline(3, color=DANGER, lw=1.5, linestyle="--", label="+3σ threshold")
    axes[1].axhline(-3, color=DANGER, lw=1.5, linestyle="--")
    axes[1].axhline(0, color="#9ca3af", lw=0.8)
    axes[1].set_title("Z-score (anomaly trigger at |z| > 3)")
    axes[1].set_ylabel("Z-score")
    axes[1].set_xlabel("Date")
    axes[1].legend(frameon=False, fontsize=9)
    axes[1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %Y"))

    fig.autofmt_xdate()
    fig.tight_layout()
    _save(fig, "10_anomaly_zscore.png")


#  Main
# ---------------------------------------------------------------------------------------

if __name__ == "__main__":
    import matplotlib.dates  # noqa: F401

    FIG_DIR.mkdir(parents=True, exist_ok=True)

    if not FACT_CSV.exists():
        print(f"ERROR: {FACT_CSV} not found. Run the pipeline first.")
        sys.exit(1)

    print("Loading fact table …")
    df = load_data()
    print(f"  {len(df):,} rows  ×  {len(df.columns)} columns")
    print("Generating figures …")

    fig_daily_volume(df)
    fig_revenue_by_country(df)
    fig_status_distribution(df)
    fig_price_distribution(df)
    fig_monthly_revenue(df)
    fig_quantity_dist(df)
    fig_segment_revenue(df)
    fig_dow_heatmap(df)
    fig_dq_summary()
    fig_anomaly_zscore(df)

    figs = sorted(FIG_DIR.glob("*.png"))
    print(f"\nDone — {len(figs)} figures in {FIG_DIR}")