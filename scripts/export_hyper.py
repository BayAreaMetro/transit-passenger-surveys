"""Export survey data to Tableau Hyper format.

Reads the flat Parquet files and produces a relational .hyper extract
with three tables (survey_responses, survey_weights, survey_metadata).

Usage:
    python scripts/export_hyper.py
"""

import logging

from transit_passenger_tools.database import DATA_ROOT, export_to_hyper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Export survey Parquet files to Tableau Hyper."""
    logger.info("=" * 80)
    logger.info("EXPORTING TABLEAU HYPER")
    logger.info("=" * 80)
    logger.info("Data directory: %s", DATA_ROOT)
    logger.info("")

    try:
        hyper_path = DATA_ROOT / "surveys_export.hyper"
        row_count = export_to_hyper(hyper_path)

        size_mb = hyper_path.stat().st_size / (1024 * 1024)
        logger.info("")
        logger.info("=" * 80)
        logger.info("EXPORT COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("  [HYPER] %s (%.2f MB, %s rows)", hyper_path.name, size_mb, f"{row_count:,}")

    except FileNotFoundError:
        logger.exception("Export failed — Parquet data not found")
        logger.info("Ensure survey_responses.parquet exists in %s", DATA_ROOT)
        raise SystemExit(1) from None
    except Exception:
        logger.exception("Export failed with unexpected error")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
