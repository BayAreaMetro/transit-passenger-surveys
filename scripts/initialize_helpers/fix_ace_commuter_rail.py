"""Fix commuter_rail_present for ACE 2023 in the database.

The legacy R code had a bug where it only checked transfer technology fields,
missing the surveyed vehicle itself. This causes ACE (single-ride commuter rail)
to incorrectly have commuter_rail_present=0 instead of 1.

This script updates the ACE 2023 Parquet files directly.
"""
import logging

import polars as pl

from transit_passenger_tools import database

logger = logging.getLogger(__name__)


def fix_ace_commuter_rail() -> None:
    """Update ACE 2023 records to set commuter_rail_present=1."""
    ace_2023_path = database.HIVE_ROOT / "survey_responses" / "operator=ACE" / "year=2023"

    if not ace_2023_path.exists():
        msg = f"ACE 2023 path does not exist: {ace_2023_path}"
        raise FileNotFoundError(msg)

    # Find Parquet files
    parquet_files = list(ace_2023_path.glob("*.parquet"))
    if not parquet_files:
        msg = f"No Parquet files found in: {ace_2023_path}"
        raise FileNotFoundError(msg)

    logger.info("Found %d Parquet file(s) for ACE 2023", len(parquet_files))

    for parquet_file in parquet_files:
        logger.info("Processing: %s", parquet_file.name)

        # Read the Parquet file
        df = pl.read_parquet(parquet_file)

        logger.info("  Records: %d", len(df))
        logger.info(
            "  Current commuter_rail_present: %s",
            df["commuter_rail_present"].unique().to_list()
        )

        # Update commuter_rail_present to 1
        df = df.with_columns(pl.lit(1).cast(pl.Int64).alias("commuter_rail_present"))

        logger.info(
            "  Updated commuter_rail_present: %s",
            df["commuter_rail_present"].unique().to_list()
        )

        # Write back to the same file
        df.write_parquet(parquet_file)
        logger.info("  Updated %s", parquet_file.name)

    logger.info("ACE 2023 commuter_rail_present fixed successfully")

