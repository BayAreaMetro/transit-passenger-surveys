"""Rebuild or update the Parquet data warehouse for transit passenger surveys.

Convention-based discovery: walks ``ingestion/`` for operator/year folders
that contain an ``ingest.py`` with a ``main()`` function.  The legacy
backfill (``ingestion/backfill/``) is always run first.

Usage::

    # Append only — skip operator/years already in the warehouse
    uv run python scripts/rebuild_database.py

    # Full rebuild — wipe DATA_ROOT and re-ingest everything
    uv run python scripts/rebuild_database.py --force-rebuild
"""

import argparse
import importlib
import logging
import sys
from pathlib import Path

# Ensure repo root is importable so ``ingestion.*`` resolves
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.backfill.backfill_ingestion import (
    extract_and_ingest_metadata,
    extract_and_ingest_weights,
    ingest_survey_batches,
)
from ingestion.backfill.backfill_loader import load_survey_data, reorder_columns_to_match_schema
from ingestion.backfill.backfill_validation import validate_schema
from ingestion.fixes.fix_ace_2023 import fix_ace_commuter_rail
from ingestion.fixes.fix_bart_2015 import (
    fix_bart_2015_airport_station_names,
    fix_bart_heavy_rail,
    fix_bart_route_field,
)
from transit_passenger_tools.database import (
    DATA_ROOT,
    archive_file,
    export_to_hyper,
    get_ingested_operator_years,
    ingest,
    validate_warehouse_integrity,
)
from transit_passenger_tools.pipeline import auto_sufficiency

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
OPERATORS_DIR = REPO_ROOT / "ingestion" / "operators"

# Standardized data directory for legacy backfill
STANDARDIZED_DIR = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports"
    r"\_data_Standardized\standardized_2025-10-13"
)
CSV_FILENAME = "survey_combined.csv"


# ========== Discovery ==========


def discover_ingestion_sources() -> list[dict]:
    """Find all ``ingestion/operators/{Operator}/{year}/ingest.py`` scripts.

    Returns a list of dicts with keys:
    ``operator``, ``year``, ``module_path`` (dotted import path).
    """
    sources: list[dict] = []
    for ingest_py in sorted(OPERATORS_DIR.rglob("ingest.py")):
        parts = ingest_py.relative_to(OPERATORS_DIR).parts
        # Expect: {operator}/{year_dir}/ingest.py
        if len(parts) != 3:  # noqa: PLR2004
            continue
        operator_dir = parts[0]
        year_dir = parts[1]
        # Extract numeric year from directory name (e.g. "year_2024" -> 2024, "2023" -> 2023)
        year_str = year_dir.replace("year_", "")
        try:
            year = int(year_str)
        except ValueError:
            logger.warning("Skipping %s — cannot parse year from '%s'", ingest_py, year_dir)
            continue
        module_path = f"ingestion.operators.{operator_dir}.{year_dir}.ingest"
        sources.append({
            "operator": operator_dir,
            "year": year,
            "module_path": module_path,
        })
    return sources


# ========== Backfill ==========


def run_backfill(csv_path: Path, *, force_rebuild: bool = False) -> None:
    """Run the legacy backfill from standardized CSV files.

    Skipped when metadata already exists (meaning backfill ran before),
    unless *force_rebuild* is ``True``.
    """
    already = get_ingested_operator_years()
    if already and not force_rebuild:
        logger.info(
            "Legacy backfill — skipped (%d operators already present)",
            len(already),
        )
        return

    if not csv_path.exists():
        logger.error("%s not found", csv_path)
        sys.exit(1)

    logger.info("Using data: %s", csv_path)

    df = load_survey_data(csv_path)
    logger.info(
        "Loaded %s records from %s operators (%s-%s)",
        f"{len(df):,}",
        df["canonical_operator"].n_unique(),
        df["survey_year"].min(),
        df["survey_year"].max(),
    )

    df = auto_sufficiency.derive_auto_sufficiency(df)
    validate_schema(df)
    df = reorder_columns_to_match_schema(df)

    total_records, num_batches, error_summary = ingest_survey_batches(
        df, collect_errors=False,
    )
    if error_summary:
        logger.error("Validation errors — no data written for failed batches")
        sys.exit(1)

    num_metadata = extract_and_ingest_metadata(df, collect_errors=False)
    num_weights = extract_and_ingest_weights(df, collect_errors=False)

    logger.info(
        "Backfill complete: %s batches, %s records, %s metadata, %s weights",
        num_batches, f"{total_records:,}", num_metadata, f"{num_weights:,}",
    )



# ========== Post-ingestion corrections ==========


def run_data_corrections() -> None:
    """Apply targeted corrections to ingested data.

    These fix known issues in legacy survey data that cannot be handled
    by the general-purpose normalization/typo-fix pipeline.
    """
    logger.info("Applying data corrections...")
    fix_ace_commuter_rail()
    fix_bart_heavy_rail()
    fix_bart_2015_airport_station_names()
    fix_bart_route_field()


# ========== Per-operator ingestion ==========


def run_operator_ingestion(*, force_rebuild: bool = False) -> None:
    """Discover and run ``ingestion/operators/{Operator}/{year}/ingest.py`` scripts.

    Each script's ``main()`` returns ``(responses, weights, metadata)``.
    All three tables are written and referential integrity is validated
    via :func:`database.ingest`.

    Skips any (operator, year) pair already present in the metadata
    parquet, unless *force_rebuild* is ``True``.
    """
    already = get_ingested_operator_years()
    sources = discover_ingestion_sources()
    logger.info("Discovered %d ingestion source(s)", len(sources))

    ran = 0
    for src in sources:
        key = (src["operator"], src["year"])

        if key in already and not force_rebuild:
            logger.info("  %s/%s — already ingested, skipping", *key)
            continue

        logger.info("  %s/%s — running %s", *key, src["module_path"])
        try:
            mod = importlib.import_module(src["module_path"])
            responses, weights, metadata = mod.main()
            ingest(responses, weights, metadata)
            ran += 1
        except Exception:
            logger.exception("  FAILED: %s/%s", *key)
            raise

    logger.info("Ran %d ingestion source(s) (%d skipped)", ran, len(sources) - ran)


# ========== Orchestrator ==========


def main(
    *,
    force_rebuild: bool = False,
    standardized_dir: Path | None = None,
) -> None:
    """Run the database build pipeline.

    Default (append):
        Checks what's already in the warehouse and only runs
        ingestion sources whose (operator, year) pair is missing.

    --force-rebuild:
        Archives the current parquet files, wipes the warehouse,
        and re-ingests everything from scratch.
    """
    sep = "=" * 80
    standardized_dir = standardized_dir or STANDARDIZED_DIR

    mode = "REBUILD (force)" if force_rebuild else "UPDATE"
    logger.info("\n%s\n%s\n%s", sep, f"TRANSIT SURVEY DATABASE — {mode}", sep)

    # -------------------------------------------------------------------
    # Pre-flight: make sure the network share is reachable
    # -------------------------------------------------------------------
    if not DATA_ROOT.parent.exists():
        logger.error("Network share not accessible: %s", DATA_ROOT.parent)
        sys.exit(1)

    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / "archive").mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------
    # Force rebuild: archive the three parquet files, then start clean
    # -------------------------------------------------------------------
    if force_rebuild:
        logger.info("Archiving existing warehouse files before rebuild...")
        for pq in DATA_ROOT.glob("*.parquet"):
            archive_file(pq)
            logger.info("  Archived %s", pq.name)

    # -------------------------------------------------------------------
    # Step 1 — Legacy backfill (74 operator/year batches from a single CSV)
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 1: LEGACY BACKFILL\n%s", sep, sep)
    csv_path = standardized_dir / CSV_FILENAME
    run_backfill(csv_path, force_rebuild=force_rebuild)

    # -------------------------------------------------------------------
    # Step 2 — Targeted data corrections for known issues in legacy data
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 2: DATA CORRECTIONS\n%s", sep, sep)
    run_data_corrections()

    # -------------------------------------------------------------------
    # Step 3 — Per-operator ingestion (convention-based discovery)
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 3: OPERATOR/YEAR INGESTION\n%s", sep, sep)
    run_operator_ingestion(force_rebuild=force_rebuild)

    # -------------------------------------------------------------------
    # Step 4 — Validate referential integrity across all tables
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 4: REFERENTIAL INTEGRITY\n%s", sep, sep)
    counts = validate_warehouse_integrity()
    logger.info(
        "Integrity check passed (orphaned weights: %d, orphaned responses: %d)",
        counts["orphaned_weights"],
        counts["orphaned_responses"],
    )

    # -------------------------------------------------------------------
    # Step 5 — Export everything to a single Tableau Hyper file
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 5: EXPORT TABLEAU HYPER\n%s", sep, sep)
    export_to_hyper(DATA_ROOT / "surveys_export.hyper")
    logger.info("Hyper export complete")

    # -------------------------------------------------------------------
    label = "REBUILD" if force_rebuild else "UPDATE"
    logger.info("\n%s\nDATABASE %s COMPLETE\n%s", sep, label, sep)
    logger.info("Data directory: %s", DATA_ROOT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build or rebuild transit survey database",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Wipe existing data and re-ingest everything from scratch",
    )
    parser.add_argument(
        "--standardized-dir",
        type=Path,
        default=None,
        help=f"Path to standardized data directory (default: {STANDARDIZED_DIR})",
    )
    args = parser.parse_args()
    main(force_rebuild=args.force_rebuild, standardized_dir=args.standardized_dir)
