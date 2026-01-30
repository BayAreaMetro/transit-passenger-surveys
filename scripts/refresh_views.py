"""Refresh DuckDB views after schema changes or new data ingestion.

This script recreates all DuckDB views over the Parquet data lake.
Run this after:
- Schema migrations
- Ingesting new survey data with potentially different column types
- Any time you see "Binder Error: Contents of view were altered"

Usage:
    python scripts/refresh_views.py
"""

import logging

from transit_passenger_tools import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Refresh all DuckDB views."""
    logger.info("=" * 80)
    logger.info("REFRESHING DUCKDB VIEWS")
    logger.info("=" * 80)
    logger.info("Database: %s", db.DUCKDB_PATH)
    logger.info("")
    
    try:
        db.create_views()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("VIEWS REFRESHED SUCCESSFULLY")
        logger.info("=" * 80)
        
    except Exception:
        logger.exception("Failed to refresh views")
        raise


if __name__ == "__main__":
    main()
