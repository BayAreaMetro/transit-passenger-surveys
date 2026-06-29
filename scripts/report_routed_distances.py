"""Generate an HTML report of routed distance distributions.

Usage::

    uv run python scripts/report_routed_distances.py
    # Opens report in browser, or:
    uv run python scripts/report_routed_distances.py --output distances_report.html

    # Single survey (operator + year):
    uv run python scripts/report_routed_distances.py --survey BART_2024
    uv run python scripts/report_routed_distances.py --list
"""

import argparse
import json
import math
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

# Map access/egress modes to one of three display buckets for the trace map.
# Mirrors _MODE_TO_PROFILE in pipeline/routed_distances.py:57 (foot/bicycle/
# driving) — keep in sync if that mapping changes.
_MODE_TO_BUCKET: dict[str | None, str] = {
    "Walk": "Walk", "Bike": "Bike",
    "PNR": "Drive", "KNR": "Drive", "TNC": "Drive",
    "Transit": "Drive", "Other": "Drive", None: "Drive",
}
_DEFAULT_BUCKET = "Drive"

# Per-mode routed-distance ceilings (miles) beyond which a leg is implausible and
# split into a toggleable flagged layer. Mirrors IMPLAUSIBLE_MILES in
# pipeline/routed_distances.py — keep in sync.
IMPLAUSIBLE_MI = {"Walk": 2.0, "Bike": 10.0}
_FLAG_BUCKET = "Flagged"
_BUCKETS = ("Walk", "Bike", "Drive", _FLAG_BUCKET)
_BUCKET_LABELS = {
    "Walk": "Walk",
    "Bike": "Bike",
    "Drive": "Drive",
    _FLAG_BUCKET: "Flagged ("
    + ", ".join(f"{m} >{t:g}mi" for m, t in IMPLAUSIBLE_MI.items())
    + ")",
}

# Routes are aggregated into unique road links (segments between consecutive
# OSRM vertices); overlapping links collapse into one line weighted by how many
# trips traverse it. Every link is kept and rendered (deck.gl / WebGL handles
# the full ~10^5 to 10^6 link count); nothing is dropped.
COORD_DECIMALS = 5  # ~1.1 m; matches source precision and snaps shared links


def decode_polyline(encoded: str, precision: int = 5) -> list[tuple[float, float]]:
    """Decode an OSRM/Google encoded polyline into (lat, lon) pairs.

    OSRM with ``geometries=polyline`` uses precision 5 (factor 1e5).
    Returns an empty list for ``None`` or empty input. (lat, lon) order is
    what Leaflet.heat and ``L.latLngBounds`` expect — do not swap.
    """
    if not encoded:  # handles None and ""
        return []
    factor = float(10 ** precision)
    coords: list[tuple[float, float]] = []
    index = lat = lon = 0
    length = len(encoded)
    while index < length:
        for axis in range(2):  # 0 = latitude, 1 = longitude
            result = shift = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:  # noqa: PLR2004 — canonical polyline chunk terminator
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if axis == 0:
                lat += delta
            else:
                lon += delta
        coords.append((lat / factor, lon / factor))
    return coords


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


def survey_slug(operator: str, year: int) -> str:
    """Build a filesystem/CLI-friendly slug for an operator/year survey."""
    return f"{operator.replace(' ', '_')}_{year}"


def available_surveys(df: pl.DataFrame) -> dict[str, tuple[str, int]]:
    """Map each survey slug to its (operator, year) pair."""
    pairs = (
        df.select("canonical_operator", "survey_year")
        .unique()
        .drop_nulls()
        .sort("canonical_operator", "survey_year")
    )
    return {
        survey_slug(op, yr): (op, yr)
        for op, yr in pairs.iter_rows()
    }


def load_survey_traces(operator: str, year: int) -> pl.DataFrame:
    """Load only the encoded-geometry + mode rows for one survey.

    Uses a lazy scan so the predicate and column projection push down into the
    Parquet reader — the large geometry string columns are read only for the
    target survey, never for the whole warehouse.
    """
    return (
        pl.scan_parquet(RESPONSES_PATH)
        .filter(
            (pl.col("canonical_operator") == operator)
            & (pl.col("survey_year") == year)
        )
        .select(
            "access_mode",
            "egress_mode",
            "geometry_orig_first_board_routed",
            "geometry_last_alight_dest_routed",
            "distance_orig_first_board_routed",
            "distance_last_alight_dest_routed",
        )
        .collect()
    )


# A link key is an undirected, coordinate-rounded segment between two
# consecutive route vertices: ((lat1, lon1), (lat2, lon2)) with the endpoints
# ordered so a→b and b→a aggregate together.
_LinkKey = tuple[tuple[float, float], tuple[float, float]]


def _add_leg_links(
    counters: dict[str, dict[_LinkKey, int]],
    routes: dict[str, int],
    geom: str | None,
    mode: str | None,
    dist: float | None,
) -> None:
    """Decode one leg's route, tally its road links, and count it as a route."""
    if not geom:
        return
    try:
        pts = decode_polyline(geom)
    except (IndexError, ValueError):
        return  # one malformed geometry must never abort the report
    if len(pts) < 2:  # noqa: PLR2004 — need 2 vertices to form a link
        return
    bucket = _MODE_TO_BUCKET.get(mode, _DEFAULT_BUCKET)
    # Split implausibly long walk/bike legs into the flagged layer.
    ceiling = IMPLAUSIBLE_MI.get(bucket)
    if ceiling is not None and dist is not None and dist > ceiling:
        bucket = _FLAG_BUCKET
    routes[bucket] += 1
    counter = counters[bucket]
    prev = (round(pts[0][0], COORD_DECIMALS), round(pts[0][1], COORD_DECIMALS))
    for lat, lon in pts[1:]:
        cur = (round(lat, COORD_DECIMALS), round(lon, COORD_DECIMALS))
        if cur != prev:  # skip zero-length links
            key = (prev, cur) if prev <= cur else (cur, prev)
            counter[key] = counter.get(key, 0) + 1
            prev = cur


def build_trace_links(
    traces: pl.DataFrame,
) -> tuple[dict[str, list[list[float]]], dict[str, dict[str, int]]]:
    """Aggregate access + egress routes into per-mode weighted links.

    Each output link is ``[lat1, lon1, lat2, lon2, count]`` where ``count`` is
    the number of trips that traverse that road segment. Access and egress legs
    for a mode share a bucket; overlapping links collapse to one line. Every
    distinct link is kept — nothing is dropped.

    Returns ``(links, stats)`` where ``stats[bucket]`` carries ``routes`` (number
    of trip legs drawn into this mode) and ``links`` (distinct road links).
    """
    counters: dict[str, dict[_LinkKey, int]] = {b: {} for b in _BUCKETS}
    routes: dict[str, int] = dict.fromkeys(_BUCKETS, 0)
    for row in traces.iter_rows(named=True):
        _add_leg_links(
            counters, routes,
            row["geometry_orig_first_board_routed"], row["access_mode"],
            row["distance_orig_first_board_routed"],
        )
        _add_leg_links(
            counters, routes,
            row["geometry_last_alight_dest_routed"], row["egress_mode"],
            row["distance_last_alight_dest_routed"],
        )

    links: dict[str, list[list[float]]] = {}
    stats: dict[str, dict[str, int]] = {}
    for name, counter in counters.items():
        links[name] = [
            [a[0], a[1], b[0], b[1], count] for (a, b), count in counter.items()
        ]
        stats[name] = {"routes": routes[name], "links": len(counter)}
    return links, stats


def compute_view(links: dict[str, list[list[float]]]) -> dict[str, float]:
    """Derive a deck.gl initial view (center + zoom) framing the bulk of links.

    Uses trip-weighted 2nd-98th percentiles of endpoint coordinates: busy links
    cluster at the stations (the core we want framed), while far-flung outlier
    routes (e.g. a trip out to Tahoe) are nearly all single-trip, so weighting
    keeps them from zooming the default view out. The user can still pan to them.
    """
    total = sum(len(v) for v in links.values())
    if total == 0:
        return {"longitude": -122.3, "latitude": 37.8, "zoom": 9.0}

    # Sample endpoints to bound the percentile cost on very large surveys.
    step = max(1, total // 100_000)
    lat_pairs: list[tuple[float, int]] = []
    lon_pairs: list[tuple[float, int]] = []
    i = 0
    for mode_links in links.values():
        for lat1, lon1, lat2, lon2, count in mode_links:
            if i % step == 0:
                lat_pairs.append((lat1, count))
                lat_pairs.append((lat2, count))
                lon_pairs.append((lon1, count))
                lon_pairs.append((lon2, count))
            i += 1

    def wpct(pairs: list[tuple[float, int]], lo: float, hi: float) -> tuple[float, float]:
        """Weighted lo/hi percentiles of (coord, weight) pairs."""
        pairs.sort()
        total_w = sum(w for _, w in pairs)
        lo_t, hi_t = lo * total_w, hi * total_w
        cum = 0
        lo_v = hi_v = pairs[-1][0]
        got_lo = False
        for coord, w in pairs:
            cum += w
            if not got_lo and cum >= lo_t:
                lo_v, got_lo = coord, True
            if cum >= hi_t:
                hi_v = coord
                break
        return lo_v, hi_v

    min_lat, max_lat = wpct(lat_pairs, 0.02, 0.98)
    min_lon, max_lon = wpct(lon_pairs, 0.02, 0.98)
    lat_span = max(max_lat - min_lat, 1e-3)
    lon_span = max(max_lon - min_lon, 1e-3)
    # Fit the lon/lat span into the container (~1100x600 px, 512 px world tile).
    zoom_lon = math.log2(1100 / 512 * 360 / lon_span)
    zoom_lat = math.log2(600 / 512 * 360 / lat_span)
    zoom = min(zoom_lon, zoom_lat) - 0.3  # small padding
    return {
        "longitude": (min_lon + max_lon) / 2,
        "latitude": (min_lat + max_lat) / 2,
        "zoom": max(3.0, min(15.0, zoom)),
    }


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
#map { position: relative; height: 600px; width: 100%; margin: 0.5em 0 1em;
       border: 1px solid #dee2e6; border-radius: 4px; overflow: hidden; }
.no-traces { color: #999; font-style: italic; }
.map-legend { display: flex; flex-wrap: wrap; gap: 1.2em; margin: 0.4em 0; }
.map-toggle { font-size: 0.85em; color: #555; cursor: pointer;
              display: inline-flex; align-items: center; }
.map-toggle .swatch { display: inline-block; width: 12px; height: 12px;
                      border-radius: 2px; margin: 0 4px 0 2px; }
"""


# deck.gl standalone (WebGL) from CDN, injected into the per-survey report
# <head> only. The OSM basemap is drawn by a deck.gl TileLayer (no API token).
# Viewing the map requires internet (script + OSM tiles) and a WebGL browser.
_MAP_HEAD = """
    <script src="https://unpkg.com/deck.gl@9.0.0/dist.min.js"></script>
"""

# Per-bucket RGB colors, shared by the JS layers and the HTML legend swatches.
_MODE_COLORS = {
    "Walk": (26, 166, 65), "Bike": (232, 96, 28), "Drive": (8, 81, 156),
    _FLAG_BUCKET: (214, 40, 40),
}

# JS init kept as a template (not an f-string) — it is full of literal braces.
# LINKS_JSON_PLACEHOLDER -> {"Walk":[[lat1,lon1,lat2,lon2,count],...], ...};
# VIEW_JSON_PLACEHOLDER -> {longitude, latitude, zoom}. Every link is drawn as a
# GPU line; width scales with each link's trip count (log scale).
_MAP_JS_TEMPLATE = """
    <script>
    window.addEventListener('load', function () {
      var data = LINKS_JSON_PLACEHOLDER;
      var view = VIEW_JSON_PLACEHOLDER;
      var colors = {
        Walk: [26, 166, 65], Bike: [232, 96, 28], Drive: [8, 81, 156],
        Flagged: [214, 40, 40]
      };
      var labels = {
        Walk: 'Walk', Bike: 'Bike', Drive: 'Drive', Flagged: 'Flagged (implausible)'
      };
      var modes = ['Walk', 'Bike', 'Drive', 'Flagged'];
      // Flagged layer starts off (matches its unchecked legend box).
      var visible = {Walk: true, Bike: true, Drive: true, Flagged: false};

      var logMax = {};
      modes.forEach(function (m) {
        var mx = 1, a = data[m] || [];
        for (var i = 0; i < a.length; i++) { if (a[i][4] > mx) mx = a[i][4]; }
        logMax[m] = Math.log(mx + 1);
      });
      function widthFor(m, c) {
        var t = logMax[m] > 0 ? Math.log(c + 1) / logMax[m] : 0;  // 0..1
        return 0.5 + 5 * t;
      }

      var basemap = new deck.TileLayer({
        id: 'osm', data: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        minZoom: 0, maxZoom: 19, tileSize: 256,
        renderSubLayers: function (props) {
          var b = props.tile.bbox;
          return new deck.BitmapLayer(props, {
            data: null, image: props.data,
            bounds: [b.west, b.south, b.east, b.north]
          });
        }
      });
      function lineLayers() {
        return modes.map(function (m) {
          return new deck.LineLayer({
            id: m, data: data[m] || [], visible: visible[m], pickable: true,
            getSourcePosition: function (d) { return [d[1], d[0]]; },
            getTargetPosition: function (d) { return [d[3], d[2]]; },
            getColor: colors[m],
            getWidth: function (d) { return widthFor(m, d[4]); },
            widthUnits: 'pixels', widthMinPixels: 0.5
          });
        });
      }
      var deckgl = new deck.DeckGL({
        container: 'map',
        initialViewState: {
          longitude: view.longitude, latitude: view.latitude, zoom: view.zoom
        },
        controller: true,
        layers: [basemap].concat(lineLayers()),
        getTooltip: function (info) {
          return info.object
            ? {text: (labels[info.layer.id] || info.layer.id) + ': '
                     + info.object[4] + ' trips'}
            : null;
        }
      });
      modes.forEach(function (m) {
        var el = document.getElementById('chk-' + m);
        if (el) el.addEventListener('change', function () {
          visible[m] = el.checked;
          deckgl.setProps({layers: [basemap].concat(lineLayers())});
        });
      });
    });
    </script>
"""


def _link_legend(stats: dict[str, dict[str, int]]) -> str:
    """Per-mode legend with a toggle checkbox, color swatch, and link counts."""
    rows = []
    for name in _BUCKETS:
        s = stats.get(name)
        if not s or not s["links"]:
            continue
        r, g, b = _MODE_COLORS[name]
        # The flagged layer starts hidden (it's noisy); the rest start on.
        checked = "" if name == _FLAG_BUCKET else " checked"
        rows.append(
            f'<label class="map-toggle">'
            f'<input type="checkbox" id="chk-{name}"{checked}> '
            f'<span class="swatch" style="background:rgb({r},{g},{b})"></span>'
            f'{_BUCKET_LABELS[name]} — {s["routes"]:,} routes, '
            f'{s["links"]:,} unique links'
            f"</label>"
        )
    return '<div class="map-legend">' + "".join(rows) + "</div>" if rows else ""


def render_map(
    links: dict[str, list[list[float]]],
    stats: dict[str, dict[str, int]] | None = None,
) -> str:
    """Render the per-mode aggregated-link section as an HTML fragment."""
    total = sum(len(v) for v in links.values())
    if total == 0:
        return (
            "<h2>Routed link volumes</h2>"
            '<p class="no-traces">No routed traces available for this survey.</p>'
        )
    payload = json.dumps(links, separators=(",", ":"))
    view = json.dumps(compute_view(links), separators=(",", ":"))
    js = _MAP_JS_TEMPLATE.replace("LINKS_JSON_PLACEHOLDER", payload).replace(
        "VIEW_JSON_PLACEHOLDER", view
    )
    return (
        "<h2>Routed link volumes</h2>"
        "<p><em>Access + egress routes aggregated by shared road link, colored "
        "by mode. Every distinct link is drawn; line width scales with the number "
        "of trips using it. Implausibly long walk (&gt; 2 mi) and bike (&gt; 10 mi) "
        "legs are split into a red flagged layer (off by default). Toggle layers "
        "below; hover a link for its trip count.</em></p>"
        f"{_link_legend(stats or {})}"
        '<div id="map"></div>'
        f"{js}"
    )


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


def generate_survey_report(
    df: pl.DataFrame,
    operator: str,
    year: int,
    trace_links: dict[str, list[list[float]]] | None = None,
    trace_stats: dict[str, dict[str, int]] | None = None,
) -> str:
    """Generate an HTML report scoped to a single operator/year survey.

    ``trace_links``/``trace_stats`` are the per-mode aggregated-link payload and
    summary from :func:`build_trace_links`, used to render the route link-volume
    map. When ``None`` an empty map (the "no traces" note) is rendered.
    """
    sub = df.filter(
        (pl.col("canonical_operator") == operator)
        & (pl.col("survey_year") == year)
    )
    n_total = len(sub)
    label = f"{operator} {year}"
    links = trace_links or {b: [] for b in _BUCKETS}

    access_stats = compute_stats(sub["distance_orig_first_board_routed"])
    egress_stats = compute_stats(sub["distance_last_alight_dest_routed"])
    access_hist = compute_histogram(sub["distance_orig_first_board_routed"])
    egress_hist = compute_histogram(sub["distance_last_alight_dest_routed"])
    access_by_mode = compute_mode_stats(
        sub, "distance_orig_first_board_routed", "access_mode"
    )
    egress_by_mode = compute_mode_stats(
        sub, "distance_last_alight_dest_routed", "egress_mode"
    )

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Routed Distance Report &mdash; {label}</title>
    <style>{CSS}</style>
    {_MAP_HEAD}
</head>
<body>
    <h1>Routed Distance Report &mdash; {label}</h1>
    <p class="timestamp">Generated: {timestamp} &mdash; {n_total:,} responses</p>

    {render_map(links, trace_stats)}

    <h2>Summary</h2>
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
</body>
</html>"""


def main() -> None:
    """Generate and optionally open the HTML report."""
    parser = argparse.ArgumentParser(description="Generate routed distance report")
    parser.add_argument(
        "--output", "-o", type=Path, default=None,
        help="Output HTML path (default: opens temp file in browser)",
    )
    parser.add_argument(
        "--survey", "-s", default=None,
        help="Scope the report to a single survey, e.g. BART_2024 "
             "(use --list to see available surveys)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available survey slugs and exit",
    )
    args = parser.parse_args()

    print("Loading warehouse data...")
    df = load_data()
    print(f"  {len(df):,} rows loaded")

    surveys = available_surveys(df)

    if args.list:
        print(f"\n{len(surveys)} available surveys:")
        for slug, (op, yr) in surveys.items():
            n = len(
                df.filter(
                    (pl.col("canonical_operator") == op)
                    & (pl.col("survey_year") == yr)
                )
            )
            print(f"  {slug:<40} {n:>8,} rows")
        return

    if args.survey:
        if args.survey not in surveys:
            print(f"\nUnknown survey: {args.survey!r}")
            print("Available surveys (use --list for row counts):")
            for slug in surveys:
                print(f"  {slug}")
            return
        operator, year = surveys[args.survey]
        print(f"Generating report for {operator} {year}...")
        print("  Loading and aggregating routed links...")
        trace_links, trace_stats = build_trace_links(load_survey_traces(operator, year))
        for name in _BUCKETS:
            s = trace_stats[name]
            print(f"    {name}: {s['routes']:,} routes, {s['links']:,} unique links")
        html = generate_survey_report(df, operator, year, trace_links, trace_stats)
        default_name = f"routed_distances_{args.survey}.html"
    else:
        print("Generating report...")
        html = generate_report(df)
        default_name = "routed_distances_report.html"

    out_path = args.output or Path(tempfile.gettempdir()) / default_name

    out_path.write_text(html, encoding="utf-8")
    print(f"Report written to: {out_path}")

    webbrowser.open(out_path.as_uri())


if __name__ == "__main__":
    main()
