"""Initialize the DuckDB data lake for transit passenger surveys.

This script creates the folder structure and DuckDB database file with views
over Parquet files. Run this once before ingesting survey data.
"""

import logging
import sys
from pathlib import Path

import duckdb

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transit_passenger_tools.db import DATA_LAKE_ROOT, DUCKDB_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_folder_structure() -> None:
    """Create the data lake folder structure."""
    for name in ["survey_responses", "survey_metadata", "survey_weights"]:
        (DATA_LAKE_ROOT / name).mkdir(parents=True, exist_ok=True)


def create_duckdb_database() -> None:
    """Create DuckDB database with tables.

    Note: Views are created by backfill_database.py after data is ingested.
    """
    logger.info("Creating database: %s", DUCKDB_PATH)

    conn = duckdb.connect(str(DUCKDB_PATH))

    try:
        # Create tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS survey_weights (
                unique_id VARCHAR NOT NULL,
                weight_scheme VARCHAR(50) NOT NULL,
                weight DOUBLE NOT NULL,
                trip_weight DOUBLE,
                expansion_factor DOUBLE,
                description VARCHAR(500),
                PRIMARY KEY (unique_id, weight_scheme)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description VARCHAR(500),
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT INTO schema_migrations (version, description)
            VALUES (1, 'Initial data lake setup with Parquet + DuckDB')
            ON CONFLICT DO NOTHING
        """)

        logger.info("Database initialized (views will be created by backfill_database.py)")

    finally:
        conn.close()


def main() -> None:
    """Run initialization."""
    logger.info("Transit Survey Data Lake Initialization")

    if not DATA_LAKE_ROOT.parent.exists():
        logger.error("Network share not accessible: %s", DATA_LAKE_ROOT.parent)
        sys.exit(1)

    create_folder_structure()
    create_duckdb_database()

    logger.info("Data lake: %s", DATA_LAKE_ROOT)
    logger.info("Next: Run backfill_database.py to populate data")


if __name__ == "__main__":
    main()
