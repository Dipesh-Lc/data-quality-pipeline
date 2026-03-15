# src/monitoring/reporting.py

"""
Reporting layer
===============
Generates human-readable pipeline run reports.

Outputs
-------
  reports/quality_report.md   - Markdown summary (machine-friendly)
  reports/quality_report.html - Styled HTML report (stakeholder-friendly)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.monitoring.anomaly_detection import AnomalyResult
from src.monitoring.quality_checks import QualityResult
from src.utils.logger import get_logger
from src.utils.paths import REPORTS

log = get_logger(__name__)


#  Markdown report
# ---------------------------------------------------------------------------------------

def build_markdown_report(
    run_date: str,
    ingestion_meta: dict[str, Any],
    validation_results: list[Any],
    quality_results: list[QualityResult],
    anomaly_results: list[AnomalyResult],
    load_counts: dict[str, int],
) -> str:
    """Build a Markdown summary report for a pipeline run."""
    lines: list[str] = []
    ts = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines += [
        "# Data Quality Pipeline -- Run Report",
        f"**Run date:** {run_date}  ",
        f"**Generated:** {ts}  ",
        "",
        "---",
        "",
        "## 1. Ingestion Summary",
        "",
        "| Source          | Rows loaded |",
        "|:----------------|------------:|",
    ]
    for k, v in ingestion_meta.items():
        lines.append(f"| {k:<16} | {v:>11,} |")
    lines.append("")

    if load_counts:
        lines += [
            "## 2. Warehouse Load",
            "",
            "| Table               | Rows |",
            "|:--------------------|-----:|",
        ]
        for tbl, cnt in load_counts.items():
            lines.append(f"| {tbl:<20} | {cnt:>4,} |")
        lines.append("")

    lines += [
        "## 3. Validation Checks",
        "",
        "| Check | Status | Severity | Rows Affected |",
        "|:------|:------:|:--------:|-------------:|",
    ]
    for r in validation_results:
        icon = "✅" if r.passed else "❌"
        lines.append(
            f"| {r.check:<50} | {icon} {r.status} | {r.severity:<8} | {r.rows_affected:>6,} |"
        )
    val_fail = sum(1 for r in validation_results if not r.passed)
    lines.append(f"\n**{len(validation_results)} checks — {val_fail} failures**\n")

    lines += [
        "## 4. Data Quality Checks",
        "",
        "| Check | Table | Status | Severity | Rows Affected |",
        "|:------|:------|:------:|:--------:|-------------:|",
    ]
    for r in quality_results:
        icon = "✅" if r.passed else "❌"
        lines.append(
            f"| {r.check:<40} | {r.table:<20} | {icon} {r.status} | {r.severity:<8} | {r.rows_affected:>6,} |"
        )
    dq_fail = sum(1 for r in quality_results if not r.passed)
    lines.append(f"\n**{len(quality_results)} checks - {dq_fail} failures**\n")

    triggered = [r for r in anomaly_results if r.triggered]
    lines += [
        "## 5. Anomaly Detection",
        "",
        f"**{len(anomaly_results)} detectors run - {len(triggered)} anomalies triggered**",
        "",
    ]
    if triggered:
        lines += [
            "| Detector | Severity | Metric | Message |",
            "|:---------|:--------:|-------:|:--------|",
        ]
        for r in triggered:
            lines.append(
                f"| {r.detector:<35} | {r.severity:<8} | {r.metric_value:>10.4f} | {r.message} |"
            )
    else:
        lines.append("_No anomalies detected._")
    lines.append("")

    all_ok = val_fail + dq_fail + len(triggered) == 0
    verdict = "✅ **PIPELINE RUN HEALTHY**" if all_ok else "⚠️ **PIPELINE RUN NEEDS ATTENTION**"
    lines += [
        "---",
        "## Overall Status",
        "",
        verdict,
        "",
        "_Report generated automatically by data-quality-pipeline v1.0.0_",
    ]
    return "\n".join(lines)


#  HTML report
# ---------------------------------------------------------------------------------------

def _md_table_to_html(md: str) -> str:
    """Wrap Markdown content in a preformatted block."""
    return f"<pre>{md}</pre>"


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>DQ Pipeline Report -- {run_date}</title>
<style>
  body   {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            max-width: 1100px; margin: 40px auto; padding: 0 20px;
            background: #f9fafb; color: #111827; }}
  h1     {{ color: #1e40af; border-bottom: 2px solid #1e40af; padding-bottom: 8px; }}
  h2     {{ color: #374151; margin-top: 32px; }}
  table  {{ border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 14px; }}
  th     {{ background: #1e40af; color: #fff; padding: 8px 12px; text-align: left; }}
  td     {{ padding: 7px 12px; border-bottom: 1px solid #e5e7eb; }}
  tr:nth-child(even) {{ background: #f3f4f6; }}
  .pass  {{ color: #16a34a; font-weight: 600; }}
  .fail  {{ color: #dc2626; font-weight: 600; }}
  .warn  {{ color: #d97706; font-weight: 600; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px;
            font-size: 12px; font-weight: 600; }}
  .badge.error    {{ background:#fee2e2; color:#991b1b; }}
  .badge.warning  {{ background:#fef3c7; color:#92400e; }}
  .badge.info     {{ background:#dbeafe; color:#1e40af; }}
  .badge.critical {{ background:#7f1d1d; color:#fff; }}
  .badge.high     {{ background:#fee2e2; color:#991b1b; }}
  .badge.medium   {{ background:#fef3c7; color:#92400e; }}
  .badge.low      {{ background:#d1fae5; color:#065f46; }}
  .verdict {{ font-size: 22px; padding: 16px 24px; border-radius: 8px;
              margin: 24px 0; }}
  .ok  {{ background: #d1fae5; color: #065f46; }}
  .bad {{ background: #fee2e2; color: #991b1b; }}
  footer {{ margin-top: 48px; color: #9ca3af; font-size: 12px; }}
</style>
</head>
<body>
<h1>📊 Data Quality Pipeline -- Run Report</h1>
<p><strong>Run date:</strong> {run_date} &nbsp;|&nbsp; <strong>Generated:</strong> {generated}</p>

{verdict_html}

{sections}

<footer>Generated by data-quality-pipeline v1.0.0</footer>
</body>
</html>"""


def _df_to_html_table(df: pd.DataFrame) -> str:
    """Convert a DataFrame to an HTML table."""
    return df.to_html(index=False, border=0, classes="", escape=True)


def build_html_report(
    run_date: str,
    ingestion_meta: dict[str, Any],
    validation_results: list[Any],
    quality_results: list[QualityResult],
    anomaly_results: list[AnomalyResult],
    load_counts: dict[str, int],
) -> str:
    """Build an HTML summary report for a pipeline run."""
    sections = []

    df_ing = pd.DataFrame(
        [{"Source": k, "Rows": f"{v:,}"} for k, v in ingestion_meta.items()]
    )
    sections.append(f"<h2>1. Ingestion Summary</h2>\n{_df_to_html_table(df_ing)}")

    if load_counts:
        df_lc = pd.DataFrame(
            [{"Table": k, "Rows": f"{v:,}"} for k, v in load_counts.items()]
        )
        sections.append(f"<h2>2. Warehouse Load</h2>\n{_df_to_html_table(df_lc)}")

    def _icon(passed: bool) -> str:
        return "✅ PASS" if passed else "❌ FAIL"

    rows_v = [
        {
            "Check": r.check,
            "Status": _icon(r.passed),
            "Severity": f'<span class="badge {r.severity}">{r.severity}</span>',
            "Rows Affected": r.rows_affected,
        }
        for r in validation_results
    ]
    df_v = pd.DataFrame(rows_v)
    sections.append(
        f"<h2>3. Validation ({sum(1 for r in validation_results if not r.passed)} failures)</h2>\n"
        f"{_df_to_html_table(df_v)}"
    )

    rows_q = [
        {
            "Check": r.check,
            "Table": r.table,
            "Status": _icon(r.passed),
            "Severity": f'<span class="badge {r.severity}">{r.severity}</span>',
            "Rows Affected": r.rows_affected,
            "Message": r.message,
        }
        for r in quality_results
    ]
    df_q = pd.DataFrame(rows_q)
    sections.append(
        f"<h2>4. Quality Checks ({sum(1 for r in quality_results if not r.passed)} failures)</h2>\n"
        f"{_df_to_html_table(df_q)}"
    )

    triggered = [r for r in anomaly_results if r.triggered]
    rows_a = [
        {
            "Detector": r.detector,
            "Status": "🚨 ANOMALY" if r.triggered else "✅ NORMAL",
            "Severity": f'<span class="badge {r.severity}">{r.severity}</span>',
            "Metric": r.metric_value,
            "Message": r.message,
        }
        for r in anomaly_results
    ]
    df_a = pd.DataFrame(rows_a)
    sections.append(
        f"<h2>5. Anomaly Detection ({len(triggered)} triggered)</h2>\n"
        f"{_df_to_html_table(df_a)}"
    )

    all_ok = (
        sum(1 for r in validation_results if not r.passed)
        + sum(1 for r in quality_results if not r.passed)
        + len(triggered)
    ) == 0
    verdict = "✅ PIPELINE RUN HEALTHY" if all_ok else "⚠️ PIPELINE RUN NEEDS ATTENTION"
    verdict_html = f'<div class="verdict {"ok" if all_ok else "bad"}">{verdict}</div>'

    return _HTML_TEMPLATE.format(
        run_date=run_date,
        generated=datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S UTC"),
        verdict_html=verdict_html,
        sections="\n\n".join(sections),
    )


#  Save reports
# ---------------------------------------------------------------------------------------

def save_reports(
    run_date: str,
    ingestion_meta: dict[str, Any],
    validation_results: list[Any],
    quality_results: list[QualityResult],
    anomaly_results: list[AnomalyResult],
    load_counts: dict[str, int],
) -> dict[str, Path]:
    """Save Markdown and HTML reports to disk."""
    REPORTS.mkdir(parents=True, exist_ok=True)

    md = build_markdown_report(
        run_date,
        ingestion_meta,
        validation_results,
        quality_results,
        anomaly_results,
        load_counts,
    )
    html = build_html_report(
        run_date,
        ingestion_meta,
        validation_results,
        quality_results,
        anomaly_results,
        load_counts,
    )

    md_path = REPORTS / "quality_report.md"
    html_path = REPORTS / "quality_report.html"

    md_path.write_text(md, encoding="utf-8")
    html_path.write_text(html, encoding="utf-8")

    log.info("Reports saved  md=%s  html=%s", md_path, html_path)
    return {"markdown": md_path, "html": html_path}
