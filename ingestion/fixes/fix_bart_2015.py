"""Fix BART legacy data issues in the database.

This script fixes known bugs in the legacy R code for BART surveys:
1. heavy_rail_present incorrectly set to 0 (should be 1 for BART Heavy Rail)
2. route field inconsistently normalized (periods and slashes)
3. 2015 airport station names using abbreviated forms (should match 2024 naming)

Applies canonical normalization against the flat survey_responses.parquet file.
"""

import logging

import polars as pl

from transit_passenger_tools.database import DATA_ROOT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_RESPONSES_FILE = "survey_responses.parquet"


def _read_responses() -> pl.DataFrame:
    path = DATA_ROOT / _RESPONSES_FILE
    if not path.exists():
        msg = f"{_RESPONSES_FILE} not found at {path}"
        raise FileNotFoundError(msg)
    return pl.read_parquet(path)


def _write_responses(df: pl.DataFrame) -> None:
    df.write_parquet(DATA_ROOT / _RESPONSES_FILE, compression="zstd", statistics=True)


# ---------------------------------------------------------------------------
# Public fix functions
# ---------------------------------------------------------------------------


def fix_bart_heavy_rail() -> int:
    """Fix heavy_rail_present=1 where vehicle_tech='Heavy Rail' for BART.

    Returns:
        Number of records fixed.
    """
    df = _read_responses()

    is_bart_heavy = (
        pl.col("survey_id").str.starts_with("BART_")
        & (pl.col("vehicle_tech") == "Heavy Rail")
        & (pl.col("heavy_rail_present") == 0)
    )
    fix_count = df.filter(is_bart_heavy).height

    if fix_count == 0:
        logger.info("BART heavy_rail_present already correct — no fix needed")
        return 0

    df = df.with_columns(
        pl.when(
            pl.col("survey_id").str.starts_with("BART_")
            & (pl.col("vehicle_tech") == "Heavy Rail")
        )
        .then(pl.lit(1))
        .otherwise(pl.col("heavy_rail_present"))
        .cast(pl.Int64)
        .alias("heavy_rail_present")
    )

    _write_responses(df)
    logger.info("BART: fixed heavy_rail_present for %d records", fix_count)
    return fix_count


def fix_bart_2015_airport_station_names() -> int:
    """Fix 2015 BART airport station names to match 2024 naming convention.

    Updates:
    - 'San Francisco Intl Airport' -> 'San Francisco International Airport'
    - 'Oakland International Airport Station' -> 'Oakland International Airport'

    Returns:
        Number of records fixed.
    """
    station_name_fixes = {
        "San Francisco Intl Airport": "San Francisco International Airport",
        "Oakland International Airport Station": "Oakland International Airport",
    }

    df = _read_responses()

    is_bart_2015 = pl.col("survey_id") == "BART_2015"

    enter_needs_fix = is_bart_2015 & pl.col("onoff_enter_station").is_in(
        list(station_name_fixes.keys())
    )
    exit_needs_fix = is_bart_2015 & pl.col("onoff_exit_station").is_in(
        list(station_name_fixes.keys())
    )
    fix_count = df.filter(enter_needs_fix | exit_needs_fix).height

    if fix_count == 0:
        logger.info("BART 2015 airport station names already correct — no fix needed")
        return 0

    df = df.with_columns(
        pl.when(is_bart_2015)
        .then(
            pl.col("onoff_enter_station").replace(
                station_name_fixes, default=pl.col("onoff_enter_station")
            )
        )
        .otherwise(pl.col("onoff_enter_station"))
        .alias("onoff_enter_station"),
        pl.when(is_bart_2015)
        .then(
            pl.col("onoff_exit_station").replace(
                station_name_fixes, default=pl.col("onoff_exit_station")
            )
        )
        .otherwise(pl.col("onoff_exit_station"))
        .alias("onoff_exit_station"),
    )

    _write_responses(df)
    logger.info("BART 2015: fixed %d airport station name records", fix_count)
    return fix_count


def fix_bart_route_field() -> int:
    """Regenerate BART route field with canonical normalization.

    Returns:
        Number of records fixed.
    """
    df = _read_responses()

    is_bart = pl.col("survey_id").str.starts_with("BART_")
    has_stations = (
        pl.col("onoff_enter_station").is_not_null()
        & pl.col("onoff_exit_station").is_not_null()
    )

    canonical_route = (
        pl.lit("BART___")
        + pl.col("onoff_enter_station")
        .str.replace_all(r"\.", "", literal=False)
        .str.replace_all("/UN ", " UN ", literal=True)
        + pl.lit("&&&")
        + pl.col("onoff_exit_station")
        .str.replace_all(r"\.", "", literal=False)
        .str.replace_all("/UN ", " UN ", literal=True)
    )

    bart_df = df.filter(is_bart & has_stations)
    mismatches = bart_df.filter(pl.col("route") != canonical_route).height

    if mismatches == 0:
        logger.info("BART route field already canonical — no fix needed")
        return 0

    df = df.with_columns(
        pl.when(is_bart & has_stations)
        .then(canonical_route)
        .otherwise(pl.col("route"))
        .alias("route")
    )

    _write_responses(df)
    logger.info("BART: fixed %d route records", mismatches)
    return mismatches


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger.info("=" * 80)
    logger.info("BART LEGACY DATA FIXES")
    logger.info("=" * 80)
    logger.info("")

    heavy_rail_fixes = fix_bart_heavy_rail()
    airport_station_fixes = fix_bart_2015_airport_station_names()
    route_fixes = fix_bart_route_field()

    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info("  heavy_rail_present fixes:   %d", heavy_rail_fixes)
    logger.info("  airport station name fixes: %d", airport_station_fixes)
    logger.info("  route field fixes:          %d", route_fixes)
    total_all_fixes = heavy_rail_fixes + airport_station_fixes + route_fixes
    logger.info("  Total fixes:                %d", total_all_fixes)
    logger.info("=" * 80)
