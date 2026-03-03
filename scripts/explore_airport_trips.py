"""Explore airport-related transit trips in the survey database.

Identifies which routes and stops serve Bay Area airports (SFO, OAK, SJC)
for BART, AC Transit, and SamTrans. Uses a lat/lon proximity search since
AC Transit and SamTrans have no station name fields — only boarding/alighting
coordinates.

Each airport has one or more **stop zones** — coordinates placed at the actual
transit stops or ground-transportation curbs rather than the airport centroid
(which would land on the tarmac).  A trip counts as "airport-related" if any
of its origin, destination, boarding, or alighting lat/lon falls within
``RADIUS`` of *any* stop zone for that airport.

Stop-zone coordinates (approximate):
  SFO
    bart       37.6160, -122.3921   SFO BART station (Int'l Terminal)
    curbside   37.6148, -122.3908   Int'l Terminal Ground Transportation
  OAK
    bart       37.7133, -122.2124   OAK BART (Coliseum connector terminus)
    curbside   37.7126, -122.2205   Terminal 1 curbside (AC Transit)
  SJC
    vta        37.3706, -121.9277   Metro / Airport VTA light-rail station
    curbside   37.3647, -121.9289   Terminal B curbside

Findings (2026-03-02):
- BART: station names directly identify SFO/OAK trips
- AC Transit: no station names; routes 21 and 73 dominate OAK-area trips (~225);
  negligible SFO/SJC presence (~17 combined, likely just nearby origins)
- SamTrans: no station names; routes 292, ECR, KX, 398 serve SFO (~112 boardings);
  negligible OAK presence (1 trip)

Usage:
    uv run scripts/explore_airport_trips.py
"""

import polars as pl

from transit_passenger_tools.database import DATA_ROOT

# ---------------------------------------------------------------------------
# Airport stop zones — keyed by airport code, each value is a list of
# (label, lat, lon) tuples placed where transit vehicles actually stop.
# ---------------------------------------------------------------------------
AIRPORT_STOP_ZONES: dict[str, list[tuple[str, float, float]]] = {
    "SFO": [
        ("bart", 37.6160, -122.3921),  # SFO BART station (Int'l Terminal level)
        ("curbside", 37.6148, -122.3908),  # Int'l Terminal Ground Transportation
    ],
    "OAK": [
        ("bart", 37.7133, -122.2124),  # OAK Airport BART connector station
        ("curbside", 37.7126, -122.2205),  # Terminal 1 ground-transportation curb
    ],
    "SJC": [
        ("vta", 37.3706, -121.9277),  # Metro / Airport VTA light-rail stop
        ("curbside", 37.3647, -121.9289),  # Terminal B curbside
    ],
}

# ~1.1 km radius in decimal degrees (rough approximation at Bay Area latitude,
# where 1° lat ≈ 111 km and 1° lon ≈ 85 km)
RADIUS = 0.01

OPERATORS = ["BART", "AC TRANSIT", "SAMTRANS"]

LAT_LON_PAIRS = [
    ("orig_lat", "orig_lon", "orig"),
    ("dest_lat", "dest_lon", "dest"),
    ("survey_board_lat", "survey_board_lon", "board"),
    ("survey_alight_lat", "survey_alight_lon", "alight"),
]


def _near_any_zone(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    zones: list[tuple[str, float, float]],
    radius: float,
) -> pl.DataFrame:
    """Return rows where (lat_col, lon_col) is within *radius* of any zone."""
    expr = pl.lit(value=False)
    for _label, lat, lon in zones:
        expr = expr | (
            ((pl.col(lat_col) - lat).abs() < radius)
            & ((pl.col(lon_col) - lon).abs() < radius)
        )
    return df.filter(expr)


def main() -> None:  # noqa: C901, PLR0912, PLR0915
    """Run the airport trip exploration."""
    df = pl.read_parquet(
        DATA_ROOT / "survey_responses.parquet",
        columns=[
            "survey_id", "route",
            "board_stop_name", "alight_stop_name",
            "orig_lat", "orig_lon", "dest_lat", "dest_lon",
            "survey_board_lat", "survey_board_lon",
            "survey_alight_lat", "survey_alight_lon",
        ],
    )

    # ---------- Field availability by operator ----------
    print("=" * 70)
    print("FIELD AVAILABILITY")
    print("=" * 70)
    for op in OPERATORS:
        sub = df.filter(pl.col("survey_id").str.starts_with(op + "_"))
        print(f"\n  {op} ({len(sub):,} rows)")
        print(f"    board_stop_name:    {sub['board_stop_name'].drop_nulls().len():,}")
        print(f"    alight_stop_name:   {sub['alight_stop_name'].drop_nulls().len():,}")
        print(f"    survey_board_lat:    {sub['survey_board_lat'].drop_nulls().len():,}")
        print(f"    dest_lat:            {sub['dest_lat'].drop_nulls().len():,}")

    # ---------- Proximity search ----------
    radius_km = RADIUS * 111
    print(f"\n{'=' * 70}")
    print(f"TRIPS NEAR AIRPORT STOP ZONES (radius ≈ {radius_km:.1f} km)")
    print("=" * 70)

    for op in OPERATORS:
        sub = df.filter(pl.col("survey_id").str.starts_with(op + "_"))

        for airport, zones in AIRPORT_STOP_ZONES.items():
            counts: dict[str, int] = {}
            frames: list[pl.DataFrame] = []

            for lat_col, lon_col, label in LAT_LON_PAIRS:
                nearby = _near_any_zone(sub, lat_col, lon_col, zones, RADIUS)
                counts[label] = nearby.height
                if nearby.height > 0:
                    frames.append(nearby)

            total = sum(counts.values())
            if total == 0:
                continue

            zone_desc = ", ".join(lbl for lbl, _, _ in zones)
            print(f"\n  {op} near {airport} ({zone_desc}):")
            for label, n in counts.items():
                print(f"    {label:10s} {n:>5,}")

            all_near = pl.concat(frames).unique(subset=["survey_id"])
            routes = (
                all_near["route"]
                .drop_nulls()
                .value_counts()
                .sort("count", descending=True)
                .head(10)
            )
            print(f"    top routes:\n{routes}")

    # ---------- Stop names for airport trips ----------
    print(f"\n{'=' * 70}")
    print("STOP NAMES AT AIRPORTS (from GTFS backfill + original data)")
    print("=" * 70)

    for op in OPERATORS:
        sub = df.filter(pl.col("survey_id").str.starts_with(op + "_"))

        for airport, zones in AIRPORT_STOP_ZONES.items():
            # Collect unique trips near this airport (any point type)
            frames: list[pl.DataFrame] = []
            for lat_col, lon_col, _label in LAT_LON_PAIRS:
                nearby = _near_any_zone(sub, lat_col, lon_col, zones, RADIUS)
                if nearby.height > 0:
                    frames.append(nearby)

            if not frames:
                continue

            all_near = pl.concat(frames).unique(subset=["survey_id"])

            board_names = (
                all_near["board_stop_name"]
                .drop_nulls()
                .value_counts()
                .sort("count", descending=True)
            )
            alight_names = (
                all_near["alight_stop_name"]
                .drop_nulls()
                .value_counts()
                .sort("count", descending=True)
            )

            if board_names.height == 0 and alight_names.height == 0:
                continue

            print(f"\n  {op} near {airport} ({all_near.height} unique trips):")
            if board_names.height > 0:
                print(f"    board_stop_name ({board_names.height} unique):")
                for row in board_names.iter_rows(named=True):
                    print(f"      {row['count']:>5,}  {row['board_stop_name']}")
            if alight_names.height > 0:
                print(f"    alight_stop_name ({alight_names.height} unique):")
                for row in alight_names.iter_rows(named=True):
                    print(f"      {row['count']:>5,}  {row['alight_stop_name']}")


if __name__ == "__main__":
    main()
