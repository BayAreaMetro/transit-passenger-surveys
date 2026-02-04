"""Initialize the DuckDB Hive warehouse for transit passenger surveys.

This script creates the folder structure, DuckDB database, backfills legacy data,
and applies any necessary data corrections. Single point of entry for database setup.
"""

import argparse
import logging
import sys
from pathlib import Path

from initialize_helpers.backfill_ingestion import (
    extract_and_ingest_metadata,
    extract_and_ingest_weights,
    ingest_survey_batches,
)
from initialize_helpers.backfill_loader import load_survey_data, reorder_columns_to_match_schema
from initialize_helpers.backfill_validation import validate_schema
from initialize_helpers.fix_ace_commuter_rail import fix_ace_commuter_rail

from transit_passenger_tools import database
from transit_passenger_tools.pipeline import auto_sufficiency

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Standardized data directory to use for backfill
STANDARDIZED_DIR = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\
        _data_Standardized\standardized_2025-10-13"
    )
CSV_FILENAME = "survey_combined.csv"


def log_step(step: str) -> None:
    """Log a major step with separator."""
    logger.info("\n%s", "=" * 80)
    logger.info(step)
    logger.info("=" * 80)


def create_folder_structure() -> None:
    """Create the Hive warehouse folder structure."""
    for name in ["survey_responses", "survey_metadata", "survey_weights"]:
        (database.HIVE_ROOT / name).mkdir(parents=True, exist_ok=True)

def backfill_legacy_data(csv_path: Path) -> None:
    """Run the backfill process from standardized CSV files."""
    if not csv_path.exists():
        logger.error("%s not found", csv_path)
        sys.exit(1)

    logger.info("Using data: %s", csv_path)

    df = load_survey_data(csv_path)
    logger.info("Loaded %s records from %s operators (%s-%s)",
                f"{len(df):,}", df["canonical_operator"].n_unique(),
                df["survey_year"].min(), df["survey_year"].max())

    df = auto_sufficiency.derive_auto_sufficiency(df)
    validate_schema(df)
    df = reorder_columns_to_match_schema(df)

    total_records, num_batches, error_summary, version_info = ingest_survey_batches(
        df, collect_errors=False
    )

    if error_summary:
        logger.error("Validation errors found - no data written for failed batches")
        sys.exit(1)

    num_metadata = extract_and_ingest_metadata(df, version_info, collect_errors=False)
    num_weights = extract_and_ingest_weights(df, collect_errors=False)

    logger.info("Backfill complete: %s batches, %s records, %s metadata, %s weights",
                num_batches, f"{total_records:,}", num_metadata, f"{num_weights:,}")


def main(standardized_dir: Path = STANDARDIZED_DIR) -> None:
    """Run full database initialization pipeline.

    Args:
        standardized_dir: Optional path to specific standardized data directory.
                         If None, uses STANDARDIZED_DIR constant.
    """
    log_step("TRANSIT SURVEY DATABASE INITIALIZATION")

    if not database.HIVE_ROOT.parent.exists():
        logger.error("Network share not accessible: %s", database.HIVE_ROOT.parent)
        sys.exit(1)

    # Step 1: Setup
    log_step("STEP 1: CREATE FOLDER STRUCTURE")
    create_folder_structure()
    logger.info("Folder structure created")

    log_step("STEP 2: RUN DATABASE MIGRATIONS")
    database.run_migrations()
    logger.info("Migrations complete")

    # Step 3: Backfill
    log_step("STEP 3: BACKFILL LEGACY DATA")
    backfill_legacy_data(csv_path = standardized_dir / CSV_FILENAME)

    # Step 4: Corrections
    log_step("STEP 4: APPLY DATA CORRECTIONS")
    fix_ace_commuter_rail()

    # Step 5: Cache
    log_step("STEP 5: REFRESH DUCKDB CACHE")
    database.sync_to_duckdb_cache()
    logger.info("DuckDB cache refreshed")

    log_step("DATABASE INITIALIZATION COMPLETE")
    logger.info("Hive warehouse: %s", database.HIVE_ROOT)
    logger.info("DuckDB cache: %s", database.DUCKDB_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Initialize transit survey database from standardized CSV files"
    )
    parser.add_argument(
        "--standardized-dir",
        type=Path,
        default=None,
        help="Path to specific standardized data directory (default: find latest)"
    )

    args = parser.parse_args()
    main(standardized_dir=args.standardized_dir)
