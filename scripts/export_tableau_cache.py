"""Export DuckDB cache to Tableau-friendly formats.

This script exports the DuckDB cache to formats that are easier for Tableau
users to work with, eliminating the need to install DuckDB/MotherDuck.

Exports both .hyper and .parquet formats to the default location (HIVE_ROOT).
The exports contain a fully joined dataset with survey_responses, pivoted
weights, and metadata.

Usage:
    python scripts/export_tableau_cache.py
"""

import logging

from transit_passenger_tools import database

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Export DuckDB cache to Tableau-friendly formats."""
    logger.info("=" * 80)
    logger.info("EXPORTING TABLEAU CACHE")
    logger.info("=" * 80)
    logger.info("Database: %s", database.DUCKDB_PATH)
    logger.info("")

    try:
        # Check cache freshness
        freshness = database.check_cache_freshness()
        logger.info("Cache status: %s", freshness["message"])
        if not freshness["is_fresh"]:
            logger.warning("<!>  Cache may be stale - consider running refresh_cache.py first")
        logger.info("")

        # Export both formats
        output_paths = database.export_cache()

        logger.info("")
        logger.info("=" * 80)
        logger.info("EXPORT COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        for fmt, path in output_paths.items():
            size_mb = path.stat().st_size / (1024 * 1024)
            logger.info("  [%s] %s (%.2f MB)", fmt.upper(), path.name, size_mb)

    except (FileNotFoundError, RuntimeError) as e:
        logger.exception("Export failed")
        logger.info("Try running: python scripts/refresh_cache.py")
        raise SystemExit(1) from e
    except Exception:
        logger.exception("Export failed with unexpected error")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
