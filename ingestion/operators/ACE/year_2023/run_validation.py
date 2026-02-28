"""Run validation for ACE 2023 preprocessing against legacy database.

Simple runner that uses the shared validation module to compare
ACE 2023 preprocessing output against the legacy database.
"""

import logging
import sys
from pathlib import Path

# Add ingestion/ to path so legacy_validation is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from legacy_validation import compare
from preprocessing import preprocess

# Setup logging
OUTPUT_DIR = Path(__file__).parent / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "validation.log"),
    ],
)


def main() -> None:
    """Run ACE 2023 validation."""
    compare.compare_outputs(
        operator="ACE",
        year=2023,
        preprocess_func=preprocess,
        output_dir=OUTPUT_DIR,
        skip_geocoding=True,
    )


if __name__ == "__main__":
    main()
