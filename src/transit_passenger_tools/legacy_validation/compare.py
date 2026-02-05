"""Compare preprocessing output against legacy database.

Main comparison logic for validating new preprocessing against existing database records.
"""

import logging
from collections.abc import Callable
from pathlib import Path

import polars as pl

from transit_passenger_tools import database
from transit_passenger_tools.pipeline import process_survey

from . import analysis, reports

logger = logging.getLogger(__name__)


def process_pipeline(
    preprocess_func: Callable[..., pl.DataFrame],
    operator: str,
    year: int,
    skip_geocoding: bool = True,
) -> pl.DataFrame:
    """Process survey data through preprocessing and canonical pipeline.

    This is the core processing logic used by both ingestion and validation.

    Args:
        preprocess_func: Preprocessing function to call
        operator: Canonical operator name
        year: Survey year
        skip_geocoding: Whether to skip geocoding in pipeline

    Returns:
        Polars DataFrame with fully processed survey data
    """
    logger.info("Running preprocessing...")
    survey_df = preprocess_func(
        canonical_operator=operator,
        survey_year=year,
    )

    logger.info("Running canonical pipeline...")
    survey_df = process_survey.process_survey(
        survey_df,
        survey_name=f"{operator} {year}",
        skip_geocoding=skip_geocoding,
    )

    logger.info("Generated %s records from pipeline", f"{len(survey_df):,}")

    return survey_df


def load_legacy_data(operator: str, year: int) -> pl.DataFrame:
    """Load survey data from existing database.

    Args:
        operator: Canonical operator name (e.g., "ACE", "BART")
        year: Survey year

    Returns:
        Polars DataFrame with legacy database records
    """
    logger.info("Querying database for %s %s records...", operator, year)

    conn = database.connect(read_only=True)

    query = f"""
        SELECT *
        FROM survey_responses
        WHERE canonical_operator = '{operator}'
          AND year = {year}
    """  # noqa: S608

    legacy_df = conn.execute(query).pl()
    conn.close()

    logger.info("Loaded %s records from database", f"{len(legacy_df):,}")

    return legacy_df


def compare_record_counts(
    legacy_df: pl.DataFrame,
    new_df: pl.DataFrame,
    output_dir: Path,
) -> pl.DataFrame | None:
    """Compare and report missing records between datasets.

    Args:
        legacy_df: Legacy database records
        new_df: New pipeline records
        output_dir: Directory to write missing record reports

    Returns:
        DataFrame with matched records (inner join on response_id), or None if no matches
    """
    logger.info("%s", "\n" + "=" * 80)
    logger.info("RECORD-LEVEL COMPARISON")
    logger.info("%s", "=" * 80)

    # Total counts
    logger.info("%s", "\nRecord Counts:")
    logger.info("  Legacy:       %s", f"{len(legacy_df):,}")
    logger.info("  New Pipeline: %s", f"{len(new_df):,}")
    logger.info("  Difference:   %s", f"{len(new_df) - len(legacy_df):,}")

    # Find missing records using anti-joins
    missing_in_new = legacy_df.join(new_df, on="response_id", how="anti")
    missing_in_legacy = new_df.join(legacy_df, on="response_id", how="anti")

    logger.info("\nMissing Records:")
    logger.info("  In legacy but not new:     %s", f"{len(missing_in_new):,}")
    logger.info("  In new but not legacy:     %s", f"{len(missing_in_legacy):,}")

    # Write missing records to CSV
    if len(missing_in_new) > 0:
        missing_in_new_path = output_dir / "missing_in_new.csv"
        missing_in_new.select(["response_id", "original_id"]).write_csv(missing_in_new_path)
        logger.info("  Saved missing_in_new to: %s", missing_in_new_path)
        logger.info(
            "  First 20 response_ids: %s",
            missing_in_new.select("response_id").head(20)["response_id"].to_list(),
        )

    if len(missing_in_legacy) > 0:
        missing_in_legacy_path = output_dir / "missing_in_legacy.csv"
        missing_in_legacy.select(["response_id", "original_id"]).write_csv(missing_in_legacy_path)
        logger.info("  Saved missing_in_legacy to: %s", missing_in_legacy_path)
        logger.info(
            "  First 20 response_ids: %s",
            missing_in_legacy.select("response_id").head(20)["response_id"].to_list(),
        )

    # Inner join for matched records
    matched_df = legacy_df.join(new_df, on="response_id", how="inner", suffix="_new")
    logger.info("\nMatched Records: %s", f"{len(matched_df):,}")

    if len(matched_df) == 0:
        logger.error("[FAIL] NO MATCHED RECORDS! Cannot perform field comparison.")
        return None

    return matched_df


def compare_outputs(
    operator: str,
    year: int,
    preprocess_func: Callable[..., pl.DataFrame],
    output_dir: str | Path = "output",
    skip_geocoding: bool = True,
    field_mapping: dict[str, str] | None = None,
) -> None:
    """Run comprehensive comparison between legacy database and new pipeline output.

    Args:
        operator: Canonical operator name (e.g., "ACE", "BART")
        year: Survey year
        preprocess_func: Preprocessing function for this survey
        output_dir: Directory to write validation reports (relative to caller or absolute)
        skip_geocoding: Whether to skip geocoding in pipeline
        field_mapping: Optional legacy->canonical field name mapping
    """
    output_dir = Path(output_dir)

    logger.info("=" * 80)
    logger.info("%s %s PIPELINE VALIDATION - STRICT COMPARISON", operator, year)
    logger.info("=" * 80)

    # Clear and recreate output directory (skip log files that may be in use)
    if output_dir.exists():
        for file in output_dir.glob("*"):
            if file.is_file() and not file.name.endswith(".log"):
                file.unlink()

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    logger.info("%s", "\n" + "=" * 80)
    logger.info("LOADING DATA")
    logger.info("%s", "=" * 80)

    legacy_df = load_legacy_data(operator, year)
    new_df = process_pipeline(preprocess_func, operator, year, skip_geocoding)

    # Apply field mapping if provided
    if field_mapping:
        logger.info("\nApplying field mappings: %s", field_mapping)
        legacy_df = legacy_df.rename(field_mapping)

    # Compare record counts and find missing records
    matched_df = compare_record_counts(legacy_df, new_df, output_dir)

    # Weight comparison
    reports.compare_weight_totals(legacy_df, new_df)

    # Field-by-field comparison
    field_summary_df = None
    if matched_df is not None and len(matched_df) > 0:
        field_summary_df = reports.compare_field_by_field(matched_df, output_dir)

        # Derived field analysis
        analysis.analyze_derived_fields(matched_df, field_summary_df, output_dir)

    # Final message
    logger.info("%s", "\n" + "=" * 80)
    logger.info("COMPARISON COMPLETE")
    logger.info("%s", "=" * 80)
    logger.info("\nAll reports saved to: %s", output_dir)
