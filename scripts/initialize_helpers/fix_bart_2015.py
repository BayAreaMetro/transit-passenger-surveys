"""Fix BART legacy data issues in the database.

This script fixes known bugs in the legacy R code for BART surveys:
1. heavy_rail_present incorrectly set to 0 (should be 1 for BART Heavy Rail)
2. route field inconsistently normalized (periods and slashes)

Applies canonical normalization consistently across all BART survey years.
"""

import logging

import polars as pl

from transit_passenger_tools import database

logger = logging.getLogger(__name__)


def fix_bart_heavy_rail() -> int:
    """Fix heavy_rail_present=1 where vehicle_tech='Heavy Rail'.

    Returns:
        Number of records fixed
    """
    bart_operator_path = database.HIVE_ROOT / "survey_responses" / "operator=BART"

    if not bart_operator_path.exists():
        msg = f"BART operator path does not exist: {bart_operator_path}"
        raise FileNotFoundError(msg)

    year_dirs = [
        d for d in bart_operator_path.iterdir() if d.is_dir() and d.name.startswith("year=")
    ]

    if not year_dirs:
        msg = f"No year directories found in: {bart_operator_path}"
        raise FileNotFoundError(msg)

    logger.info("Fixing heavy_rail_present for BART surveys...")
    total_fixed = 0

    for year_dir in sorted(year_dirs):
        year = year_dir.name.split("=")[1]

        parquet_files = list(year_dir.glob("data-*.parquet"))
        if not parquet_files:
            continue

        for parquet_file in parquet_files:
            df = pl.read_parquet(parquet_file)

            if "heavy_rail_present" in df.columns and "vehicle_tech" in df.columns:
                before = df.filter(
                    (pl.col("vehicle_tech") == "Heavy Rail") & (pl.col("heavy_rail_present") == 0)
                ).height

                if before > 0:
                    df = df.with_columns(
                        pl.when(pl.col("vehicle_tech") == "Heavy Rail")
                        .then(pl.lit(1))
                        .otherwise(pl.col("heavy_rail_present"))
                        .cast(pl.Int64)
                        .alias("heavy_rail_present")
                    )
                    df.write_parquet(parquet_file)
                    logger.info("  BART %s: Fixed %d heavy_rail_present records", year, before)
                    total_fixed += before

    if total_fixed == 0:
        logger.info("  No heavy_rail_present fixes needed")
    logger.info("")
    return total_fixed


def fix_bart_route_field() -> int:
    """Regenerate BART route field with canonical normalization.

    Returns:
        Number of records fixed
    """
    bart_operator_path = database.HIVE_ROOT / "survey_responses" / "operator=BART"

    if not bart_operator_path.exists():
        msg = f"BART operator path does not exist: {bart_operator_path}"
        raise FileNotFoundError(msg)

    year_dirs = [
        d for d in bart_operator_path.iterdir() if d.is_dir() and d.name.startswith("year=")
    ]

    if not year_dirs:
        msg = f"No year directories found in: {bart_operator_path}"
        raise FileNotFoundError(msg)

    logger.info("Fixing route field normalization for BART surveys...")
    total_fixed = 0

    for year_dir in sorted(year_dirs):
        year = year_dir.name.split("=")[1]

        parquet_files = list(year_dir.glob("data-*.parquet"))
        if not parquet_files:
            continue

        for parquet_file in parquet_files:
            df = pl.read_parquet(parquet_file)

            if (
                "onoff_enter_station" in df.columns
                and "onoff_exit_station" in df.columns
                and "route" in df.columns
            ):
                # Regenerate route with canonical normalization
                df_fixed = df.with_columns(
                    (
                        pl.lit("BART___")
                        + pl.col("onoff_enter_station")
                        .str.replace_all(r"\.", "", literal=False)  # Remove periods
                        .str.replace_all("/UN ", " UN ", literal=True)  # Fix /UN → UN
                        + pl.lit("&&&")
                        + pl.col("onoff_exit_station")
                        .str.replace_all(r"\.", "", literal=False)  # Remove periods
                        .str.replace_all("/UN ", " UN ", literal=True)  # Fix /UN → UN
                    ).alias("route_new")
                )

                mismatches = df_fixed.filter(pl.col("route") != pl.col("route_new")).height

                if mismatches > 0:
                    df = df_fixed.with_columns(pl.col("route_new").alias("route")).drop("route_new")
                    df.write_parquet(parquet_file)
                    logger.info("  BART %s: Fixed %d route records", year, mismatches)
                    total_fixed += mismatches

    if total_fixed == 0:
        logger.info("  No route fixes needed")
    logger.info("")
    return total_fixed


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger.info("=" * 80)
    logger.info("BART LEGACY DATA FIXES")
    logger.info("=" * 80)
    logger.info("")

    heavy_rail_fixes = fix_bart_heavy_rail()
    route_fixes = fix_bart_route_field()

    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info("  heavy_rail_present fixes: %d", heavy_rail_fixes)
    logger.info("  route field fixes:        %d", route_fixes)
    logger.info("  Total fixes:              %d", heavy_rail_fixes + route_fixes)
    logger.info("=" * 80)
