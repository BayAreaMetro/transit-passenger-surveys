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


def create_folder_structure():
    """Create the data lake folder structure."""
    for name in ["survey_responses", "survey_metadata", "survey_weights"]:
        (DATA_LAKE_ROOT / name).mkdir(parents=True, exist_ok=True)


def create_duckdb_database():
    """Create DuckDB database with views over Parquet files."""
    logger.info(f"Creating database: {DUCKDB_PATH}")

    conn = duckdb.connect(str(DUCKDB_PATH))

    try:
        # Try to create views (will fail if no data exists yet)
        responses_pattern = f"{DATA_LAKE_ROOT}/survey_responses/**/*.parquet"
        metadata_path = DATA_LAKE_ROOT / "survey_metadata" / "metadata.parquet"

        try:
            conn.execute(f"""
                CREATE OR REPLACE VIEW survey_responses AS
                SELECT * FROM read_parquet('{responses_pattern}', 
                    hive_partitioning=true, union_by_name=true, filename=false)
            """)
            conn.execute(f"""
                CREATE OR REPLACE VIEW survey_metadata AS
                SELECT * FROM read_parquet('{metadata_path}', union_by_name=true)
            """)
            logger.info("Views created successfully")
        except Exception:
            logger.warning("Views will be created after first data ingestion (run backfill_database.py)")

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

        logger.info("Database initialization complete")

    finally:
        conn.close()


def main():
    """Run initialization."""
    logger.info("Transit Survey Data Lake Initialization")

    if not DATA_LAKE_ROOT.parent.exists():
        logger.error(f"Network share not accessible: {DATA_LAKE_ROOT.parent}")
        sys.exit(1)

    create_folder_structure()
    create_duckdb_database()

    logger.info(f"Data lake: {DATA_LAKE_ROOT}")
    logger.info("Next: Run backfill_database.py to populate data")


if __name__ == "__main__":
    main()
