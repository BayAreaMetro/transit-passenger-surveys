"""Rebuild or update the Parquet data warehouse for transit passenger surveys.

Convention-based discovery: walks ``ingestion/`` for operator/year folders
that contain an ``ingest.py`` with a ``main()`` function.  The legacy
backfill (``ingestion/backfill/``) is always run first.

Usage::

    # Append only — skip operator/years already in the warehouse
    uv run python -m ingestion.rebuild_database

    # Re-ingest specific operator/year combos
    uv run python -m ingestion.rebuild_database --rebuild BART/2024 ACE/2023

    # Full rebuild — wipe DATA_ROOT and re-ingest everything (requires confirmation)
    uv run python -m ingestion.rebuild_database --rebuild ALL

    # List discovered ingestion sources and their status
    uv run python -m ingestion.rebuild_database --list
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
from ingestion.fixes.fix_missing_stops import fix_missing_stops
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
        sources.append(
            {
                "operator": operator_dir,
                "year": year,
                "module_path": module_path,
            }
        )
    return sources


def parse_rebuild_target(target: str) -> tuple[str, int]:
    """Validate and parse an ``OPERATOR/YEAR`` string.

    Returns ``(operator, year)`` or raises :class:`argparse.ArgumentTypeError`.
    """
    parts = target.strip().split("/")
    if len(parts) != 2:  # noqa: PLR2004
        msg = f"Expected OPERATOR/YEAR (e.g. BART/2024), got '{target}'"
        raise argparse.ArgumentTypeError(msg)
    operator, year_str = parts
    try:
        year = int(year_str)
    except ValueError as exc:
        msg = f"Year must be an integer, got '{year_str}'"
        raise argparse.ArgumentTypeError(msg) from exc
    return (operator, year)


def list_sources() -> None:
    """Print discovered ingestion sources and whether they are already ingested."""
    sources = discover_ingestion_sources()
    already = get_ingested_operator_years()

    print(f"\nDiscovered {len(sources)} ingestion source(s):\n")
    for src in sources:
        key = (src["operator"], src["year"])
        status = "ingested" if key in already else "not ingested"
        print(f"  {src['operator']}/{src['year']}  [{status}]")

    # Also show legacy backfill status
    if already:
        legacy = {k for k in already if k not in {(s["operator"], s["year"]) for s in sources}}
        if legacy:
            print(f"\nLegacy backfill operators ({len(legacy)}):")
            for op, yr in sorted(legacy):
                print(f"  {op}/{yr}  [ingested]")
    print()


# ========== Backfill ==========


def run_backfill(csv_path: Path, *, force_rebuild: bool = False) -> None:
    """Run the legacy backfill from standardized CSV files.

    Skipped when responses already exist (meaning backfill ran before),
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

    # Stamp metadata fields (source, inflation_year, ingestion_timestamp, etc.)
    # onto every response row before writing to parquet.
    df = extract_and_ingest_metadata(df, collect_errors=False)

    total_records, num_batches, error_summary = ingest_survey_batches(
        df,
        collect_errors=False,
    )
    if error_summary:
        logger.error("Validation errors — no data written for failed batches")
        sys.exit(1)

    num_weights = extract_and_ingest_weights(df, collect_errors=False)

    logger.info(
        "Backfill complete: %s batches, %s records, %s weights",
        num_batches,
        f"{total_records:,}",
        f"{num_weights:,}",
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
    fix_missing_stops()


# ========== Per-operator ingestion ==========


def run_operator_ingestion(
    *,
    force_rebuild: bool = False,
    rebuild_targets: set[tuple[str, int]] | None = None,
) -> None:
    """Discover and run ``ingestion/operators/{Operator}/{year}/ingest.py`` scripts.

    Each script's ``main()`` returns ``(responses, weights)``.
    Both tables are written and referential integrity is validated
    via :func:`database.ingest`.

    Parameters
    ----------
    force_rebuild:
        Re-run every discovered source regardless of ingestion status.
    rebuild_targets:
        If provided, **only** run sources whose ``(operator, year)`` is in
        this set — and always run them (ignore "already ingested" check).
    """
    already = get_ingested_operator_years()
    sources = discover_ingestion_sources()
    logger.info("Discovered %d ingestion source(s)", len(sources))

    # When rebuild targets are given, filter to just those
    if rebuild_targets is not None:
        sources = [s for s in sources if (s["operator"], s["year"]) in rebuild_targets]
        matched = {(s["operator"], s["year"]) for s in sources}
        missing = rebuild_targets - matched
        if missing:
            for op, yr in sorted(missing):
                logger.error("  %s/%s — no ingest.py found", op, yr)
            sys.exit(1)

    ran = 0
    for src in sources:
        key = (src["operator"], src["year"])

        if rebuild_targets is None and key in already and not force_rebuild:
            logger.info("  %s/%s — already ingested, skipping", *key)
            continue

        logger.info("  %s/%s — running %s", *key, src["module_path"])
        try:
            mod = importlib.import_module(src["module_path"])
            responses, weights = mod.main()
            ingest(responses, weights)
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
    rebuild_targets: set[tuple[str, int]] | None = None,
) -> None:
    """Run the database build pipeline.

    Default (append):
        Checks what's already in the warehouse and only runs
        ingestion sources whose (operator, year) pair is missing.

    --rebuild OPERATOR/YEAR ...:
        Re-ingest only the specified operator/year combos (skips
        backfill, corrections, and integrity checks).

    --rebuild ALL:
        Archives the current parquet files, wipes the warehouse,
        and re-ingests everything from scratch.
    """
    sep = "=" * 80
    standardized_dir = standardized_dir or STANDARDIZED_DIR

    # -------------------------------------------------------------------
    # Pre-flight: make sure the network share is reachable
    # -------------------------------------------------------------------
    if not DATA_ROOT.parent.exists():
        logger.error("Network share not accessible: %s", DATA_ROOT.parent)
        sys.exit(1)

    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / "archive").mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------
    # Targeted rebuild: --rebuild BART/2024 ACE/2023
    # -------------------------------------------------------------------
    if rebuild_targets:
        targets_label = ", ".join(f"{op}/{yr}" for op, yr in sorted(rebuild_targets))
        logger.info("\n%s\nTRANSIT SURVEY DATABASE — REBUILD: %s\n%s", sep, targets_label, sep)

        run_operator_ingestion(rebuild_targets=rebuild_targets)

        logger.info("\n%s\nEXPORT TABLEAU HYPER\n%s", sep, sep)
        export_to_hyper(DATA_ROOT / "surveys_export.hyper")
        logger.info("Hyper export complete")

        logger.info("\n%s\nREBUILD COMPLETE\n%s", sep, sep)
        logger.info("Data directory: %s", DATA_ROOT)
        return

    # -------------------------------------------------------------------
    # Full pipeline: backfill → corrections → operators → integrity → hyper
    # -------------------------------------------------------------------
    mode = "REBUILD ALL" if force_rebuild else "UPDATE"
    logger.info("\n%s\nTRANSIT SURVEY DATABASE — %s\n%s", sep, mode, sep)

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
        "Integrity check passed (orphaned weights: %d)",
        counts["orphaned_weights"],
    )

    # -------------------------------------------------------------------
    # Step 5 — Export everything to a single Tableau Hyper file
    # -------------------------------------------------------------------
    logger.info("\n%s\nSTEP 5: EXPORT TABLEAU HYPER\n%s", sep, sep)
    export_to_hyper(DATA_ROOT / "surveys_export.hyper")
    logger.info("Hyper export complete")

    # -------------------------------------------------------------------
    label = "REBUILD ALL" if force_rebuild else "UPDATE"
    logger.info("\n%s\nDATABASE %s COMPLETE\n%s", sep, label, sep)
    logger.info("Data directory: %s", DATA_ROOT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build or rebuild transit survey database",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--rebuild",
        nargs="+",
        metavar="TARGET",
        help=(
            "Re-ingest specific operator/year combos (e.g. BART/2024 ACE/2023) "
            "or pass ALL to wipe and rebuild the entire warehouse"
        ),
    )
    mode_group.add_argument(
        "--list",
        action="store_true",
        dest="list_sources",
        help="List discovered ingestion sources and their status, then exit",
    )

    parser.add_argument(
        "--standardized-dir",
        type=Path,
        default=None,
        help=f"Path to standardized data directory (default: {STANDARDIZED_DIR})",
    )
    args = parser.parse_args()

    if args.list_sources:
        list_sources()
        sys.exit(0)

    force_rebuild = False
    rebuild_targets = None

    if args.rebuild:
        if args.rebuild == ["ALL"]:
            # Confirmation prompt for the destructive full rebuild
            print(
                "\n  WARNING: --rebuild ALL will archive and wipe all parquet files\n"
                f"  in {DATA_ROOT} and re-ingest everything from scratch.\n"
            )
            answer = input("  Continue? [y/N] ").strip().lower()
            if answer != "y":
                print("Aborted.")
                sys.exit(0)
            force_rebuild = True
        else:
            rebuild_targets = {parse_rebuild_target(t) for t in args.rebuild}

    main(
        force_rebuild=force_rebuild,
        standardized_dir=args.standardized_dir,
        rebuild_targets=rebuild_targets,
    )
