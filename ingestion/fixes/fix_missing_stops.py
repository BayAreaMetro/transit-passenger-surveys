"""Backfill missing board_stop_name / alight_stop_name using GTFS stop data.

For bus operators (and some older rail surveys) the legacy standardized CSV
has no stop/station name — only boarding/alighting coordinates.  This fix
snaps those coordinates to the nearest GTFS stop within 250 m, using the
Caltrans all-California transit-stops GeoJSON that is already on the network
drive.

**Conservative approach:**
- Only surveys from 2017 or later are attempted (the GTFS snapshot is ~2023).
- Only null stop-name rows are touched — existing values are never overwritten.
- Matches beyond 250 m are left null rather than risk a false assignment.

Usage (standalone):
    uv run -m ingestion.fixes.fix_missing_stops

Called from ``rebuild_database.run_data_corrections()`` during a full rebuild.
"""

import logging

import geopandas as gpd
import polars as pl

from transit_passenger_tools.database import DATA_ROOT
from transit_passenger_tools.pipeline.geocoding import snap_to_nearest_stop

logger = logging.getLogger(__name__)

# Caltrans all-California transit stops (from 511 GTFS feeds)
UNC_PREFIX = r"\\models.ad.mtc.ca.gov\data\models"
STOPS_GEOJSON = (
    f"{UNC_PREFIX}/Data/OnBoard/Data and Reports/Geography Files/"
    "cdot_ca_transit_stops_4312132402745178866.geojson"
)

# Maximum snap distance in metres
MAX_DISTANCE_M = 250.0

# Only attempt surveys from this year forward (GTFS snapshot is ~2023)
MIN_SURVEY_YEAR = 2017

# Maps our canonical operator prefix (from survey_id) to the agency
# name(s) used in the Caltrans GeoJSON ``agency`` field.  Multiple
# patterns are OR-ed with case-insensitive substring matching.
OPERATOR_AGENCY_MAP: dict[str, list[str]] = {
    "AC TRANSIT": ["Alameda-Contra Costa Transit"],
    "MUNI": ["City and County of San Francisco"],
    "VTA": ["Santa Clara Valley Transportation Authority"],
    "SAMTRANS": ["San Mateo County Transit"],
    "GOLDEN GATE TRANSIT": ["Golden Gate Bridge"],
    "LAVTA": ["Livermore-Amador Valley"],
    "TRI-DELTA": ["Eastern Contra Costa Transit"],
    "COUNTY CONNECTION": ["Central Contra Costa Transit"],
    "WESTCAT": ["Western Contra Costa Transit"],
    "SOLTRANS": ["Solano Transportation Authority"],
    "UNION CITY": ["City of Union City"],
    "NAPA VINE": ["Napa Valley"],
    "PETALUMA TRANSIT": ["City of Petaluma"],
    "Santa Rosa CityBus": ["City of Santa Rosa"],
    "Sonoma County Transit": ["Sonoma County"],
    "FAST": ["City of Fremont"],  # Fremont/Newark/Union City
    "DUMBARTON": ["Dumbarton"],
    "VACAVILLE CITY COACH": ["City of Vacaville"],
    "RIO-VISTA": ["Rio Vista"],
    "SF BAY FERRY": ["San Francisco Bay Area Water"],
    # Rail operators — already have stop names for most rows,
    # but a few may be missing.
    "BART": ["Bay Area Rapid Transit", "BART"],
    "CALTRAIN": ["Peninsula Corridor Joint Powers", "Caltrain"],
    "SMART": ["Sonoma-Marin Area Rail"],
    "CAPITOL CORRIDOR": ["Capitol Corridor"],
    "ACE": ["Altamont Corridor Express"],
}


def fix_missing_stops() -> int:
    """Snap null stop names to nearest GTFS stop for recent surveys.

    Returns:
        Total number of records filled across both columns.
    """
    responses_path = DATA_ROOT / "survey_responses.parquet"
    if not responses_path.exists():
        msg = f"survey_responses.parquet not found at {responses_path}"
        raise FileNotFoundError(msg)

    df = pl.read_parquet(responses_path)

    # Count how many are null before we start
    board_null_before = df["board_stop_name"].is_null().sum()
    alight_null_before = df["alight_stop_name"].is_null().sum()

    # Only work on surveys >= MIN_SURVEY_YEAR
    df = df.with_columns(
        pl.col("survey_id")
        .str.extract(r"_(\d{4})$", 1)
        .cast(pl.Int32)
        .alias("_survey_year")
    )

    # Load GTFS stops once
    logger.info("Loading GTFS stops from %s", STOPS_GEOJSON)
    stops_gdf = gpd.read_file(STOPS_GEOJSON)
    logger.info("Loaded %s stops", len(stops_gdf))

    # Process each operator that has eligible rows
    for operator, agency_names in OPERATOR_AGENCY_MAP.items():
        op_mask = (
            pl.col("survey_id").str.starts_with(operator + "_")
            & (pl.col("_survey_year") >= MIN_SURVEY_YEAR)
        )
        op_df = df.filter(op_mask)

        if op_df.height == 0:
            continue

        board_nulls = op_df["board_stop_name"].is_null().sum()
        alight_nulls = op_df["alight_stop_name"].is_null().sum()
        if board_nulls == 0 and alight_nulls == 0:
            continue

        logger.info(
            "%s (>= %d): %s rows, %s null board, %s null alight",
            operator,
            MIN_SURVEY_YEAR,
            op_df.height,
            board_nulls,
            alight_nulls,
        )

        # Snap board
        if board_nulls > 0:
            new_board = snap_to_nearest_stop(
                op_df,
                stops_gdf,
                lat_col="survey_board_lat",
                lon_col="survey_board_lon",
                stop_name_col="board_stop_name",
                max_distance_m=MAX_DISTANCE_M,
                agency_filter=agency_names,
            )
            op_df = op_df.with_columns(new_board)

        # Snap alight
        if alight_nulls > 0:
            new_alight = snap_to_nearest_stop(
                op_df,
                stops_gdf,
                lat_col="survey_alight_lat",
                lon_col="survey_alight_lon",
                stop_name_col="alight_stop_name",
                max_distance_m=MAX_DISTANCE_M,
                agency_filter=agency_names,
            )
            op_df = op_df.with_columns(new_alight)

        # Write updated rows back into the main DataFrame
        # Use an anti-join + concat approach to replace the operator slice
        op_ids = op_df.select("response_id")
        rest = df.join(op_ids, on="response_id", how="anti")
        df = pl.concat([rest, op_df.select(df.columns)], how="vertical")

    # Drop helper column and write back
    df = df.drop("_survey_year")
    board_null_after = df["board_stop_name"].is_null().sum()
    alight_null_after = df["alight_stop_name"].is_null().sum()

    total_filled = (board_null_before - board_null_after) + (alight_null_before - alight_null_after)

    if total_filled == 0:
        logger.info("fix_missing_stops: no new stop names assigned")
    else:
        logger.info(
            "fix_missing_stops: filled %s stop names "
            "(board: %s → %s null, alight: %s → %s null)",
            total_filled,
            board_null_before,
            board_null_after,
            alight_null_before,
            alight_null_after,
        )
        df.write_parquet(responses_path, compression="zstd", statistics=True)

    return int(total_filled)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
    filled = fix_missing_stops()
    print(f"Done — filled {filled} stop names")
