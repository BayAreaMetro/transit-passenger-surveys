"""Generate an HTML report of routed distance distributions.

Usage::

    uv run python scripts/report_routed_distances.py
    # Opens report in browser, or:
    uv run python scripts/report_routed_distances.py --output distances_report.html
"""

import argparse
import tempfile
import webbrowser
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

DATA_ROOT = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data"
)
RESPONSES_PATH = DATA_ROOT / "survey_responses.parquet"

DISTANCE_BINS = [0, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 50, float("inf")]
BIN_LABELS = [
    "0-0.25", "0.25-0.5", "0.5-1", "1-2", "2-3",
    "3-5", "5-10", "10-20", "20-50", "50+",
]


def load_data() -> pl.DataFrame:
    """Load relevant columns from the warehouse."""
    return pl.read_parquet(
        RESPONSES_PATH,
        columns=[
            "canonical_operator",
            "survey_year",
            "access_mode",
            "egress_mode",
            "distance_orig_first_board_routed",
            "distance_last_alight_dest_routed",
        ],
    )


def compute_stats(series: pl.Series) -> dict:
    """Compute summary statistics for a distance series."""
    s = series.drop_nulls()
    if len(s) == 0:
        return {"n": 0}
    return {
        "n": len(s),
        "mean": s.mean(),
        "median": s.median(),
        "std": s.std(),
        "p25": s.quantile(0.25),
        "p75": s.quantile(0.75),
        "p90": s.quantile(0.90),
        "p95": s.quantile(0.95),
        "p99": s.quantile(0.99),
        "max": s.max(),
    }


def compute_histogram(series: pl.Series) -> list[dict]:
    """Bin distances into a histogram."""
    s = series.drop_nulls()
    total = len(s)
    if total == 0:
        return []
    rows = []
    for i in range(len(DISTANCE_BINS) - 1):
        lo, hi = DISTANCE_BINS[i], DISTANCE_BINS[i + 1]
        count = s.filter((s >= lo) & (s < hi)).len()
        rows.append({
            "bin": BIN_LABELS[i],
            "count": count,
            "pct": 100 * count / total,
        })
    return rows


def compute_mode_stats(df: pl.DataFrame, dist_col: str, mode_col: str) -> pl.DataFrame:
    """Compute distance stats grouped by access/egress mode."""
    return (
        df.filter(pl.col(dist_col).is_not_null())
        .group_by(mode_col)
        .agg(
            pl.len().alias("n"),
            pl.col(dist_col).mean().alias("mean"),
            pl.col(dist_col).median().alias("median"),
            pl.col(dist_col).quantile(0.75).alias("p75"),
            pl.col(dist_col).quantile(0.90).alias("p90"),
        )
        .sort("n", descending=True)
    )


def compute_operator_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Compute per-operator summary statistics."""
    return (
        df.group_by("canonical_operator")
        .agg(
            pl.len().alias("n_total"),
            pl.col("distance_orig_first_board_routed").is_not_null().sum().alias("n_access_routed"),
            pl.col("distance_last_alight_dest_routed").is_not_null().sum().alias("n_egress_routed"),
            pl.col("distance_orig_first_board_routed").mean().alias("access_mean"),
            pl.col("distance_orig_first_board_routed").median().alias("access_median"),
            pl.col("distance_orig_first_board_routed").quantile(0.90).alias("access_p90"),
            pl.col("distance_last_alight_dest_routed").mean().alias("egress_mean"),
            pl.col("distance_last_alight_dest_routed").median().alias("egress_median"),
            pl.col("distance_last_alight_dest_routed").quantile(0.90).alias("egress_p90"),
        )
        .sort("canonical_operator")
    )


def fmt(val: float | None, decimals: int = 2) -> str:
    """Format a number or return '—' for None."""
    if val is None:
        return "—"
    if isinstance(val, int):
        return f"{val:,}"
    return f"{val:,.{decimals}f}"


def histogram_bar_html(pct: float, max_pct: float) -> str:
    """Render a CSS bar for a histogram row."""
    width = (pct / max_pct * 100) if max_pct > 0 else 0
    return (
        f'<div class="bar-container">'
        f'<div class="bar" style="width:{width:.1f}%"></div>'
        f'</div>'
    )


def render_stats_table(stats: dict, total_rows: int) -> str:
    """Render a summary stats dict as an HTML table."""
    if stats["n"] == 0:
        return "<p>No data</p>"
    coverage_pct = 100 * stats["n"] / total_rows if total_rows > 0 else 0
    n_str = f"{fmt(stats['n'])} / {fmt(total_rows)} ({coverage_pct:.1f}%)"
    return f"""
    <table class="stats-table">
        <tr><td>Coverage</td><td>{n_str}</td></tr>
        <tr><td>Mean</td><td>{fmt(stats['mean'])} mi</td></tr>
        <tr><td>Median</td><td>{fmt(stats['median'])} mi</td></tr>
        <tr><td>Std Dev</td><td>{fmt(stats['std'])} mi</td></tr>
        <tr><td>P25</td><td>{fmt(stats['p25'])} mi</td></tr>
        <tr><td>P75</td><td>{fmt(stats['p75'])} mi</td></tr>
        <tr><td>P90</td><td>{fmt(stats['p90'])} mi</td></tr>
        <tr><td>P95</td><td>{fmt(stats['p95'])} mi</td></tr>
        <tr><td>P99</td><td>{fmt(stats['p99'])} mi</td></tr>
        <tr><td>Max</td><td>{fmt(stats['max'], 1)} mi</td></tr>
    </table>
    """


def render_histogram(hist: list[dict]) -> str:
    """Render histogram as an HTML bar chart."""
    if not hist:
        return "<p>No data</p>"
    max_pct = max(row["pct"] for row in hist)
    rows_html = ""
    for row in hist:
        rows_html += f"""
        <tr>
            <td class="bin-label">{row['bin']}</td>
            <td class="count">{row['count']:,}</td>
            <td class="pct">{row['pct']:.1f}%</td>
            <td class="bar-cell">{histogram_bar_html(row['pct'], max_pct)}</td>
        </tr>"""
    return f"""
    <table class="hist-table">
        <thead><tr><th>Range (mi)</th><th>Count</th><th>%</th><th></th></tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    """


def render_mode_table(mode_df: pl.DataFrame, mode_col: str) -> str:
    """Render mode breakdown as an HTML table."""
    if mode_df.is_empty():
        return "<p>No data</p>"
    rows_html = ""
    for row in mode_df.iter_rows(named=True):
        mode = row[mode_col] if row[mode_col] is not None else "(null)"
        rows_html += f"""
        <tr>
            <td>{mode}</td>
            <td class="num">{row['n']:,}</td>
            <td class="num">{fmt(row['mean'])}</td>
            <td class="num">{fmt(row['median'])}</td>
            <td class="num">{fmt(row['p75'])}</td>
            <td class="num">{fmt(row['p90'])}</td>
        </tr>"""
    header = (
        "<tr><th>Mode</th><th>N</th><th>Mean (mi)</th>"
        "<th>Median (mi)</th><th>P75 (mi)</th><th>P90 (mi)</th></tr>"
    )
    return f"""
    <table class="data-table">
        <thead>{header}</thead>
        <tbody>{rows_html}</tbody>
    </table>
    """


def render_operator_table(op_df: pl.DataFrame) -> str:
    """Render operator-level summary table."""
    rows_html = ""
    for row in op_df.iter_rows(named=True):
        pct_access = (
            100 * row["n_access_routed"] / row["n_total"]
            if row["n_total"] > 0
            else 0
        )
        rows_html += f"""
        <tr>
            <td>{row['canonical_operator']}</td>
            <td class="num">{row['n_total']:,}</td>
            <td class="num">{row['n_access_routed']:,} ({pct_access:.0f}%)</td>
            <td class="num">{fmt(row['access_mean'])}</td>
            <td class="num">{fmt(row['access_median'])}</td>
            <td class="num">{fmt(row['access_p90'])}</td>
            <td class="num">{fmt(row['egress_mean'])}</td>
            <td class="num">{fmt(row['egress_median'])}</td>
            <td class="num">{fmt(row['egress_p90'])}</td>
        </tr>"""
    return f"""
    <table class="data-table">
        <thead><tr>
            <th>Operator</th><th>Rows</th><th>Routed</th>
            <th>Acc Mean</th><th>Acc Med</th><th>Acc P90</th>
            <th>Egr Mean</th><th>Egr Med</th><th>Egr P90</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    """


def render_operator_detail(df: pl.DataFrame, operator: str) -> str:
    """Render a collapsible detail section for one operator."""
    op_df = df.filter(pl.col("canonical_operator") == operator)
    n = len(op_df)

    access_stats = compute_stats(op_df["distance_orig_first_board_routed"])
    egress_stats = compute_stats(op_df["distance_last_alight_dest_routed"])
    access_hist = compute_histogram(op_df["distance_orig_first_board_routed"])
    access_by_mode = compute_mode_stats(
        op_df, "distance_orig_first_board_routed", "access_mode"
    )
    egress_by_mode = compute_mode_stats(
        op_df, "distance_last_alight_dest_routed", "egress_mode"
    )

    years = sorted(op_df["survey_year"].unique().to_list())
    years_str = ", ".join(str(y) for y in years)

    return f"""
    <details class="operator-detail">
        <summary><strong>{operator}</strong> — {n:,} rows ({years_str})</summary>
        <div class="detail-content">
            <div class="two-col">
                <div>
                    <h4>Access (origin → first boarding)</h4>
                    {render_stats_table(access_stats, n)}
                    <h5>By mode</h5>
                    {render_mode_table(access_by_mode, 'access_mode')}
                </div>
                <div>
                    <h4>Egress (last alighting → destination)</h4>
                    {render_stats_table(egress_stats, n)}
                    <h5>By mode</h5>
                    {render_mode_table(egress_by_mode, 'egress_mode')}
                </div>
            </div>
            <h4>Access distance distribution</h4>
            {render_histogram(access_hist)}
        </div>
    </details>
    """


CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       max-width: 1200px; margin: 0 auto; padding: 20px; color: #333; }
h1 { color: #1a5276; border-bottom: 2px solid #1a5276; padding-bottom: 8px; }
h2 { color: #2c3e50; margin-top: 2em; }
h3 { color: #34495e; }
h4 { color: #555; margin: 1em 0 0.5em; }
h5 { color: #777; margin: 0.8em 0 0.3em; font-size: 0.9em; }
table { border-collapse: collapse; margin: 0.5em 0 1em; font-size: 0.9em; }
th, td { padding: 4px 10px; text-align: left; border-bottom: 1px solid #eee; }
th { background: #f8f9fa; font-weight: 600; border-bottom: 2px solid #dee2e6; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
.stats-table td:first-child { font-weight: 500; color: #555; width: 80px; }
.stats-table td:last-child { font-variant-numeric: tabular-nums; }
.data-table { width: 100%; }
.hist-table { width: 100%; }
.hist-table .bin-label { width: 80px; }
.hist-table .count { text-align: right; width: 70px; }
.hist-table .pct { text-align: right; width: 50px; }
.hist-table .bar-cell { width: 50%; }
.bar-container { background: #f0f0f0; height: 16px; border-radius: 3px; }
.bar { background: #3498db; height: 100%; border-radius: 3px; min-width: 1px; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 2em; }
@media (max-width: 800px) { .two-col { grid-template-columns: 1fr; } }
details { margin: 0.5em 0; }
summary { cursor: pointer; padding: 8px; background: #f8f9fa; border-radius: 4px;
           border: 1px solid #eee; }
summary:hover { background: #e8f4fd; }
.detail-content { padding: 1em; border: 1px solid #eee; border-top: none;
                   border-radius: 0 0 4px 4px; }
.operator-detail { margin: 4px 0; }
.timestamp { color: #999; font-size: 0.85em; }
"""


def generate_report(df: pl.DataFrame) -> str:
    """Generate the full HTML report."""
    n_total = len(df)
    access_stats = compute_stats(df["distance_orig_first_board_routed"])
    egress_stats = compute_stats(df["distance_last_alight_dest_routed"])
    access_hist = compute_histogram(df["distance_orig_first_board_routed"])
    egress_hist = compute_histogram(df["distance_last_alight_dest_routed"])
    access_by_mode = compute_mode_stats(
        df, "distance_orig_first_board_routed", "access_mode"
    )
    egress_by_mode = compute_mode_stats(
        df, "distance_last_alight_dest_routed", "egress_mode"
    )
    op_summary = compute_operator_stats(df)
    operators = sorted(df["canonical_operator"].unique().to_list())

    operator_details = "\n".join(
        render_operator_detail(df, op) for op in operators
    )

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Routed Distance Report</title>
    <style>{CSS}</style>
</head>
<body>
    <h1>Routed Distance Report</h1>
    <p class="timestamp">Generated: {timestamp} &mdash; {n_total:,} total responses</p>

    <h2>Overall Summary</h2>
    <div class="two-col">
        <div>
            <h3>Access (origin → first boarding)</h3>
            {render_stats_table(access_stats, n_total)}
        </div>
        <div>
            <h3>Egress (last alighting → destination)</h3>
            {render_stats_table(egress_stats, n_total)}
        </div>
    </div>

    <h3>Access distance distribution</h3>
    {render_histogram(access_hist)}

    <h3>Egress distance distribution</h3>
    {render_histogram(egress_hist)}

    <h2>By Access/Egress Mode</h2>
    <div class="two-col">
        <div>
            <h3>Access mode</h3>
            {render_mode_table(access_by_mode, 'access_mode')}
        </div>
        <div>
            <h3>Egress mode</h3>
            {render_mode_table(egress_by_mode, 'egress_mode')}
        </div>
    </div>

    <h2>By Operator</h2>
    {render_operator_table(op_summary)}

    <h2>Operator Details</h2>
    <p><em>Click to expand</em></p>
    {operator_details}
</body>
</html>"""


def main() -> None:
    """Generate and optionally open the HTML report."""
    parser = argparse.ArgumentParser(description="Generate routed distance report")
    parser.add_argument(
        "--output", "-o", type=Path, default=None,
        help="Output HTML path (default: opens temp file in browser)",
    )
    args = parser.parse_args()

    print("Loading warehouse data...")
    df = load_data()
    print(f"  {len(df):,} rows loaded")

    print("Generating report...")
    html = generate_report(df)

    out_path = args.output or Path(tempfile.gettempdir()) / "routed_distances_report.html"

    out_path.write_text(html, encoding="utf-8")
    print(f"Report written to: {out_path}")

    webbrowser.open(out_path.as_uri())


if __name__ == "__main__":
    main()
