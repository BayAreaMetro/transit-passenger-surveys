"""Initialize the DuckDB data lake for transit passenger surveys.

This script creates the folder structure and DuckDB database file with tables
and views. Run this once before ingesting survey data.
"""

import logging
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transit_passenger_tools import database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_folder_structure() -> None:
    """Create the data lake folder structure."""
    for name in ["survey_responses", "survey_metadata", "survey_weights"]:
        (database.DATA_LAKE_ROOT / name).mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Run initialization."""
    logger.info("Transit Survey Data Lake Initialization")

    if not database.DATA_LAKE_ROOT.parent.exists():
        logger.error("Network share not accessible: %s", database.DATA_LAKE_ROOT.parent)
        sys.exit(1)

    logger.info("Creating folder structure...")
    create_folder_structure()

    logger.info("Running database migrations...")
    database.run_migrations()

    logger.info("\nData lake initialized: %s", database.DATA_LAKE_ROOT)
    logger.info("Next: Run backfill_database.py to populate data and create views")


if __name__ == "__main__":
    main()
