"""One-off fix that updates the purposes to "Airport" for a specific set of survey IDs for SamTrans.

Usage (standalone):
    uv run -m ingestion.fixes.fix_samtrans_airport

Called from ``rebuild_database.run_data_corrections()`` during a full rebuild.
"""

import logging

import polars as pl

from transit_passenger_tools.database import DATA_ROOT

logger = logging.getLogger(__name__)

_RESPONSES_FILE = "survey_responses.parquet"

# "Airport" value from the Purpose enum
_AIRPORT = "Airport"

# Only apply to these specific SamTrans survey years
_TARGET_SURVEYS = ["SAMTRANS_2019", "SAMTRANS_2022"]

# Original IDs whose dest_purp should be set to "Airport"
_DEST_PURP_IDS: set[str] = {
    "283",
    "280",
    "285",
    "371",
    "633",
    "856",
    "919",
    "972",
    "1003",
    "1301",
    "1389",
    "1390",
    "1466",
    "608",
    "204",
    "294",
    "214",
    "609",
    "487"
}

# Original IDs whose orig_purp should be set to "Airport"
_ORIG_PURP_IDS: set[str] = {
    "269",
    "23",
    "281",
    "284",
    "289",
    "291",
    "516",
    "635",
    "883",
    "1449",
    "293",
    "675",
    "1007",
    "1215",
    "615",
}


def _read_responses() -> pl.DataFrame:
    path = DATA_ROOT / _RESPONSES_FILE
    if not path.exists():
        msg = f"{_RESPONSES_FILE} not found at {path}"
        raise FileNotFoundError(msg)
    return pl.read_parquet(path)


def _write_responses(df: pl.DataFrame) -> None:
    df.write_parquet(DATA_ROOT / _RESPONSES_FILE, compression="zstd", statistics=True)


def fix_samtrans_dest_purp_airport() -> int:
    """Set dest_purp='Airport' for specific SamTrans original_ids.

    Returns:
        Number of records fixed.
    """
    df = _read_responses()

    is_target = pl.col("survey_id").is_in(_TARGET_SURVEYS)
    needs_fix = (
        is_target
        & pl.col("original_id").is_in(list(_DEST_PURP_IDS))
        & (pl.col("dest_purp") != _AIRPORT)
    )
    fix_count = df.filter(needs_fix).height

    if fix_count == 0:
        logger.info("SamTrans dest_purp Airport already correct — no fix needed")
        return 0

    df = df.with_columns(
        pl.when(is_target & pl.col("original_id").is_in(list(_DEST_PURP_IDS)))
        .then(pl.lit(_AIRPORT))
        .otherwise(pl.col("dest_purp"))
        .alias("dest_purp")
    )

    _write_responses(df)
    logger.info("SamTrans: set dest_purp='Airport' for %d records", fix_count)
    return fix_count


def fix_samtrans_orig_purp_airport() -> int:
    """Set orig_purp='Airport' for specific SamTrans original_ids.

    Returns:
        Number of records fixed.
    """
    df = _read_responses()

    is_target = pl.col("survey_id").is_in(_TARGET_SURVEYS)
    needs_fix = (
        is_target
        & pl.col("original_id").is_in(list(_ORIG_PURP_IDS))
        & (pl.col("orig_purp") != _AIRPORT)
    )
    fix_count = df.filter(needs_fix).height

    if fix_count == 0:
        logger.info("SamTrans orig_purp Airport already correct — no fix needed")
        return 0

    df = df.with_columns(
        pl.when(is_target & pl.col("original_id").is_in(list(_ORIG_PURP_IDS)))
        .then(pl.lit(_AIRPORT))
        .otherwise(pl.col("orig_purp"))
        .alias("orig_purp")
    )

    _write_responses(df)
    logger.info("SamTrans: set orig_purp='Airport' for %d records", fix_count)
    return fix_count


def fix_samtrans_airport() -> int:
    """Apply both dest_purp and orig_purp Airport fixes for SamTrans.

    Returns:
        Total number of records fixed.
    """
    dest_fixes = fix_samtrans_dest_purp_airport()
    orig_fixes = fix_samtrans_orig_purp_airport()
    total = dest_fixes + orig_fixes
    logger.info("SamTrans Airport fix complete: %d total records updated", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    fix_samtrans_airport()
