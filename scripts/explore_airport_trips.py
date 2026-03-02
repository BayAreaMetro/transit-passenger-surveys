"""Explore airport-related transit trips in the survey database.

Identifies which routes and stops serve Bay Area airports (SFO, OAK, SJC)
for BART, AC Transit, and SamTrans. Uses a lat/lon proximity search since
AC Transit and SamTrans have no station name fields — only boarding/alighting
coordinates.

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

# Airport coordinates (approximate terminal centroids)
AIRPORTS = {
    "SFO": (37.6213, -122.3790),
    "OAK": (37.7126, -122.2197),
    "SJC": (37.3639, -121.9289),
}

# ~2 km radius in decimal degrees (rough approximation at Bay Area latitude)
RADIUS = 0.02

OPERATORS = ["BART", "AC TRANSIT", "SAMTRANS"]

LAT_LON_PAIRS = [
    ("orig_lat", "orig_lon", "orig"),
    ("dest_lat", "dest_lon", "dest"),
    ("survey_board_lat", "survey_board_lon", "board"),
    ("survey_alight_lat", "survey_alight_lon", "alight"),
]


def main() -> None:
    """Run the airport trip exploration."""
    df = pl.read_parquet(
        DATA_ROOT / "survey_responses.parquet",
        columns=[
            "survey_id", "route",
            "onoff_enter_station", "onoff_exit_station",
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
        print(f"    onoff_enter_station: {sub['onoff_enter_station'].drop_nulls().len():,}")
        print(f"    onoff_exit_station:  {sub['onoff_exit_station'].drop_nulls().len():,}")
        print(f"    survey_board_lat:    {sub['survey_board_lat'].drop_nulls().len():,}")
        print(f"    dest_lat:            {sub['dest_lat'].drop_nulls().len():,}")

    # ---------- Proximity search ----------
    print(f"\n{'=' * 70}")
    print(f"TRIPS NEAR AIRPORTS (radius ≈ {RADIUS * 111:.0f} km)")
    print("=" * 70)

    for op in OPERATORS:
        sub = df.filter(pl.col("survey_id").str.starts_with(op + "_"))

        for airport, (lat, lon) in AIRPORTS.items():
            counts = {}
            frames = []

            for lat_col, lon_col, label in LAT_LON_PAIRS:
                nearby = sub.filter(
                    ((pl.col(lat_col) - lat).abs() < RADIUS)
                    & ((pl.col(lon_col) - lon).abs() < RADIUS)
                )
                counts[label] = nearby.height
                if nearby.height > 0:
                    frames.append(nearby)

            total = sum(counts.values())
            if total == 0:
                continue

            print(f"\n  {op} near {airport}:")
            for label, n in counts.items():
                print(f"    {label:10s} {n:>5,}")

            all_near = pl.concat(frames)
            routes = (
                all_near["route"]
                .drop_nulls()
                .value_counts()
                .sort("count", descending=True)
                .head(10)
            )
            print(f"    top routes:\n{routes}")


if __name__ == "__main__":
    main()
