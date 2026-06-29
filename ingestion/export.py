"""Extract the survey warehouse to CSV or Tableau Hyper.

Reads the flat Parquet warehouse and writes either:

* ``--format csv`` (default): one combined CSV (survey responses with weights
  left-joined by ``response_id``; the latest weight per response is kept unless
  ``--weight-scheme`` is given).
* ``--format hyper``: a relational ``.hyper`` extract with two tables
  (``survey_responses``, ``survey_weights``).

The ``--operator`` / ``--year`` / ``--survey-id`` filters apply to both
formats.  Output defaults to the system temp directory.

Usage::

    # Combined CSV of the whole warehouse -> system temp dir
    uv run python -m ingestion.export

    # Tableau Hyper extract
    uv run python -m ingestion.export --format hyper

    # One operator/year, either format
    uv run python -m ingestion.export --operator BART --year 2024
    uv run python -m ingestion.export --format hyper --survey-id BART_2024

    # Specific weight scheme, into the repo scratch/ folder
    uv run python -m ingestion.export --weight-scheme 2015_ridership --output-dir scratch

    # Responses only (CSV), explicit output path
    uv run python -m ingestion.export --no-weights --output C:/tmp/responses.csv

    # List the available weight schemes and exit
    uv run python -m ingestion.export --list-schemes
"""

import argparse
import logging
import tempfile
from pathlib import Path

from transit_passenger_tools.database import (
    DATA_ROOT,
    available_weight_schemes,
    export_to_csv,
    export_to_hyper,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _default_name(*, fmt: str, include_weights: bool, parts: list) -> str:
    """Build the default output filename, embedding any active filter parts.

    e.g. ``survey_combined_BART_2024.csv`` or ``surveys_export_BART_2024.hyper``.
    """
    if fmt == "csv":
        stem = "survey_combined" if include_weights else "survey_responses"
        ext = "csv"
    else:
        stem = "surveys_export"
        ext = "hyper"
    slug = "_".join(str(p) for p in parts if p is not None)
    return f"{stem}_{slug}.{ext}" if slug else f"{stem}.{ext}"


def main(
    *,
    fmt: str = "csv",
    output: Path | None = None,
    output_dir: Path | None = None,
    operator: str | None = None,
    year: int | None = None,
    survey_id: str | None = None,
    weight_scheme: str | None = None,
    include_weights: bool = True,
) -> None:
    """Export the warehouse to *fmt* (``csv`` or ``hyper``).

    The destination is *output* when given, otherwise ``<output_dir>/<name>``
    with a format-appropriate default name (filters appended) and *output_dir*
    defaulting to the system temp dir.
    """
    if output is not None:
        output_path = output
    else:
        out_dir = output_dir or Path(tempfile.gettempdir())
        filter_parts = [survey_id] if survey_id is not None else [operator, year]
        name = _default_name(fmt=fmt, include_weights=include_weights, parts=filter_parts)
        output_path = out_dir / name

    logger.info("=" * 80)
    logger.info("EXPORTING SURVEY %s", fmt.upper())
    logger.info("=" * 80)
    logger.info("Data directory: %s", DATA_ROOT)
    logger.info("Output: %s", output_path)
    logger.info("")

    try:
        if fmt == "csv":
            row_count = export_to_csv(
                output_path,
                operator=operator,
                year=year,
                survey_id=survey_id,
                weight_scheme=weight_scheme,
                include_weights=include_weights,
            )
        else:
            row_count = export_to_hyper(
                output_path,
                operator=operator,
                year=year,
                survey_id=survey_id,
                weight_scheme=weight_scheme,
            )
    except FileNotFoundError:
        logger.exception("Export failed — Parquet data not found")
        logger.info("Ensure survey_responses.parquet exists in %s", DATA_ROOT)
        raise SystemExit(1) from None
    except Exception:
        logger.exception("Export failed with unexpected error")
        raise SystemExit(1) from None

    logger.info("")
    logger.info("=" * 80)
    logger.info("EXPORT COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info("  [%s] %s (%s rows)", fmt.upper(), output_path, f"{row_count:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract the survey warehouse to CSV or Tableau Hyper",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "hyper"],
        default="csv",
        dest="fmt",
        help="Output format (default: csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Full output file path (overrides --output-dir and the default name)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for the output file (default: system temp dir)",
    )
    parser.add_argument(
        "--operator",
        default=None,
        metavar="NAME",
        help="Export only this canonical_operator (case-insensitive, e.g. BART)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Export only this survey_year (e.g. 2024)",
    )
    parser.add_argument(
        "--survey-id",
        default=None,
        metavar="ID",
        help="Export only this survey_id (the operator_year value, e.g. BART_2024)",
    )
    parser.add_argument(
        "--weight-scheme",
        default=None,
        metavar="NAME",
        help="Keep only this weight scheme (csv: latest weight per response if omitted)",
    )
    parser.add_argument(
        "--no-weights",
        action="store_true",
        help="CSV only: export the responses table without joining weights",
    )
    parser.add_argument(
        "--list-schemes",
        action="store_true",
        help="List the available weight schemes in the warehouse, then exit",
    )
    args = parser.parse_args()

    if args.list_schemes:
        schemes = available_weight_schemes()
        if schemes:
            print("\nAvailable weight schemes:\n")
            for scheme in schemes:
                print(f"  {scheme}")
            print()
        else:
            print("\nNo weight schemes found (survey_weights.parquet missing or empty).\n")
        raise SystemExit(0)

    if args.no_weights and args.fmt == "hyper":
        parser.error("--no-weights only applies to --format csv")

    main(
        fmt=args.fmt,
        output=args.output,
        output_dir=args.output_dir,
        operator=args.operator,
        year=args.year,
        survey_id=args.survey_id,
        weight_scheme=args.weight_scheme,
        include_weights=not args.no_weights,
    )
