"""Add TM1 SuperDistrict columns to the survey response warehouse.

The migration derives SuperDistricts from existing ``*_tm1_taz`` values. It
does not rerun spatial geocoding. The current responses file is archived before
the migrated file replaces it.

Run from the repository root::

    uv run python scripts/migrate_add_superdistricts.py --dry-run
    uv run python scripts/migrate_add_superdistricts.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from transit_passenger_tools.config import get_config
from transit_passenger_tools.database import DATA_ROOT, archive_file
from transit_passenger_tools.pipeline.geocoding import (
    SUPERDISTRICT_LOCATIONS,
    assign_superdistricts_from_tm1_taz,
    load_tm1_superdistrict_crosswalk,
)

logger = logging.getLogger(__name__)


def migrate(responses_path: Path, *, dry_run: bool = False) -> dict[str, dict[str, int]]:
    """Populate SuperDistrict fields and return per-location coverage counts."""
    if not responses_path.exists():
        msg = f"Responses parquet not found: {responses_path}"
        raise FileNotFoundError(msg)

    config = get_config()
    tm1_zone = next(
        (zone for zone in config.geocoding_zones if zone.zone_type == "tm1_taz"),
        None,
    )
    if tm1_zone is None:
        msg = "No tm1_taz geocoding zone is configured"
        raise ValueError(msg)

    tm1_path = config.resolve_shapefile_path(tm1_zone.shapefile_key)
    crosswalk = load_tm1_superdistrict_crosswalk(
        tm1_path,
        taz_field=tm1_zone.field_name,
        superdistrict_field=config.tm1_superdistrict_field,
    )
    responses = pl.read_parquet(responses_path)
    migrated = assign_superdistricts_from_tm1_taz(
        responses,
        crosswalk,
        taz_field=tm1_zone.field_name,
        superdistrict_field=config.tm1_superdistrict_field,
    )

    report: dict[str, dict[str, int]] = {}
    for location in SUPERDISTRICT_LOCATIONS:
        taz_col = f"{location}_tm1_taz"
        sd_col = f"{location}_superdistrict"
        if taz_col not in migrated.columns or sd_col not in migrated.columns:
            continue
        report[location] = {
            "populated": migrated[sd_col].is_not_null().sum(),
            "source_null": migrated[taz_col].is_null().sum(),
            "unmatched": migrated.filter(
                pl.col(taz_col).is_not_null() & pl.col(sd_col).is_null()
            ).height,
        }
        logger.info("%s: %s", location, report[location])

    if dry_run:
        logger.info("Dry run: warehouse was not changed")
        return report

    temp_path = responses_path.with_suffix(".superdistricts.tmp.parquet")
    migrated.write_parquet(temp_path, compression="zstd", statistics=True)
    archived_path = archive_file(responses_path)
    temp_path.replace(responses_path)
    logger.info("Archived original warehouse to %s", archived_path)
    logger.info("Wrote %s migrated responses to %s", len(migrated), responses_path)
    return report


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--responses-path",
        type=Path,
        default=DATA_ROOT / "survey_responses.parquet",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    migrate(args.responses_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
