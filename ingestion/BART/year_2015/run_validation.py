"""Run validation for BART 2015 preprocessing against database.

Compares new preprocessing output with existing database records to validate
that the new pipeline produces identical results to legacy R script.
"""

import logging
from pathlib import Path

from preprocessing import preprocess

from transit_passenger_tools.legacy_validation import compare

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Output directory for validation reports
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def main() -> None:
    """Run BART 2015 validation."""
    compare.compare_outputs(
        operator="BART",
        year=2015,
        preprocess_func=preprocess,
        output_dir=OUTPUT_DIR,
        skip_geocoding=True,
    )


if __name__ == "__main__":
    main()
