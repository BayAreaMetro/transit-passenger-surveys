"""Refresh DuckDB cache after schema changes or new data ingestion.

This script syncs Parquet files into DuckDB tables for fast BI tool access.
Run this after:
- Ingesting new survey data
- Manual Parquet file modifications
- Any time Tableau/DBeaver queries are slow

The cache is automatically refreshed during normal ingestion (ingest_survey_batch,
ingest_survey_metadata, ingest_survey_weights) unless refresh_cache=False.

Usage:
    python scripts/refresh_cache.py
"""

import logging

from transit_passenger_tools import database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Refresh DuckDB cache from Parquet files."""
    logger.info("=" * 80)
    logger.info("REFRESHING DUCKDB CACHE")
    logger.info("=" * 80)
    logger.info("Database: %s", database.DUCKDB_PATH)
    logger.info("")

    try:
        # Check cache freshness before syncing
        freshness = database.check_cache_freshness()
        logger.info("Cache status: %s", freshness["message"])
        logger.info("")

        # Sync data
        database.sync_to_duckdb_cache()

        logger.info("")
        logger.info("=" * 80)
        logger.info("CACHE REFRESHED SUCCESSFULLY")
        logger.info("=" * 80)

    except Exception:
        logger.exception("Failed to refresh cache")
        raise


if __name__ == "__main__":
    main()
