"""Fix commuter_rail_present for ACE 2023 in the database.

The legacy R code had a bug where it only checked transfer technology fields,
missing the surveyed vehicle itself. This causes ACE (single-ride commuter rail)
to incorrectly have commuter_rail_present=0 instead of 1.

This script updates the flat survey_responses.parquet file.
"""

import logging

import polars as pl

from transit_passenger_tools.database import DATA_ROOT

logger = logging.getLogger(__name__)


def fix_ace_commuter_rail() -> None:
    """Update ACE 2023 records to set commuter_rail_present=1."""
    responses_path = DATA_ROOT / "survey_responses.parquet"

    if not responses_path.exists():
        msg = f"survey_responses.parquet not found at {responses_path}"
        raise FileNotFoundError(msg)

    df = pl.read_parquet(responses_path)

    is_ace_2023 = (pl.col("canonical_operator") == "ACE") & (pl.col("survey_year") == 2023)  # noqa: PLR2004
    needs_fix = is_ace_2023 & (pl.col("commuter_rail_present") != 1)
    fix_count = df.filter(needs_fix).height

    if fix_count == 0:
        logger.info("ACE 2023 commuter_rail_present already correct — no fix needed")
        return

    df = df.with_columns(
        pl.when(is_ace_2023)
        .then(pl.lit(1).cast(pl.Int64))
        .otherwise(pl.col("commuter_rail_present"))
        .alias("commuter_rail_present")
    )

    df.write_parquet(responses_path, compression="zstd", statistics=True)
    logger.info("ACE 2023: fixed commuter_rail_present for %d records", fix_count)
