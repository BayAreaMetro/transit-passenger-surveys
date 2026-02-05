"""Derived field root cause analysis."""

import logging
from pathlib import Path

import polars as pl

from transit_passenger_tools.pipeline.validate import get_field_dependencies

logger = logging.getLogger(__name__)

MATCH_RATE = 100.0


def _analyze_single_derived_field(
    derived_field: str,
    row: dict,
    input_fields: list[str],
    matched_df: pl.DataFrame,
    output_dir: Path,
) -> dict | None:
    """Analyze a single derived field for mismatches.

    Args:
        derived_field: Name of the derived field
        row: Row from field_summary_df
        input_fields: List of input field dependencies
        matched_df: DataFrame from inner join on response_id
        output_dir: Directory to write analysis samples

    Returns:
        Dictionary with analysis results or None if no mismatches
    """
    legacy_col = derived_field
    new_col = f"{derived_field}_new"

    mismatch_mask = ~(
        (pl.col(legacy_col).is_null() & pl.col(new_col).is_null())
        | (pl.col(legacy_col) == pl.col(new_col))
    )

    mismatched_records = matched_df.filter(mismatch_mask)

    if len(mismatched_records) == 0:
        return None

    logger.info("\n  %s (%.2f%% match):", derived_field, row["match_rate"])
    logger.info("    Input fields: %s", input_fields)

    # Check if input fields differ
    input_differ_count = 0
    for input_field in input_fields:
        input_legacy = input_field
        input_new = f"{input_field}_new"

        if (
            input_legacy not in mismatched_records.columns
            or input_new not in mismatched_records.columns
        ):
            continue

        input_differ = mismatched_records.filter(
            ~(
                (pl.col(input_legacy).is_null() & pl.col(input_new).is_null())
                | (pl.col(input_legacy) == pl.col(input_new))
            )
        ).height

        if input_differ > 0:
            input_differ_count += input_differ
            logger.info("      %s: %s records differ", input_field, f"{input_differ:,}")

    issue_type = "logic_change" if input_differ_count == 0 else "data_issue"
    if input_differ_count == 0:
        logger.warning("      [LOGIC CHANGE] All inputs match but output differs")
    else:
        logger.info("      [DATA ISSUE] Input data differences detected")

    # Write detailed samples for first 100 mismatches
    sample_records = mismatched_records.head(100)
    select_cols = ["response_id", legacy_col, new_col]

    for input_field in input_fields:
        input_legacy = input_field
        input_new = f"{input_field}_new"
        if input_legacy in sample_records.columns and input_new in sample_records.columns:
            select_cols.extend([input_legacy, input_new])

    sample_df = sample_records.select(select_cols)
    sample_path = output_dir / f"derived_{derived_field}_analysis.csv"
    sample_df.write_csv(sample_path)
    logger.info("      Saved sample to: %s", sample_path.name)

    return {
        "derived_field": derived_field,
        "mismatch_count": row["mismatches"],
        "match_rate": row["match_rate"],
        "input_fields": ", ".join(input_fields),
        "issue_type": issue_type,
    }


def analyze_derived_fields(
    matched_df: pl.DataFrame,
    field_summary_df: pl.DataFrame,
    output_dir: Path,
) -> None:
    """Analyze mismatches in derived fields to identify root causes.

    Args:
        matched_df: DataFrame from inner join on response_id
        field_summary_df: DataFrame with per-field match statistics
        output_dir: Directory to write analysis reports
    """
    logger.info("%s", "\n" + "=" * 80)
    logger.info("DERIVED FIELD ROOT CAUSE ANALYSIS")
    logger.info("%s", "=" * 80)

    # Get field dependencies from pipeline modules
    try:
        field_deps = get_field_dependencies()
        logger.info("Loaded dependencies for %s derived fields", len(field_deps))
    except Exception:
        logger.exception("Failed to load field dependencies")
        return

    # Find derived fields with mismatches
    derived_mismatches = field_summary_df.filter(
        (pl.col("field_name").is_in(list(field_deps.keys()))) & (pl.col("match_rate") < MATCH_RATE)
    )

    if len(derived_mismatches) == 0:
        logger.info("[PASS] All derived fields match 100%%")
        return

    logger.info("\nAnalyzing %s derived fields with mismatches...", len(derived_mismatches))

    analysis_results = []

    for row in derived_mismatches.iter_rows(named=True):
        derived_field = row["field_name"]
        input_fields = field_deps.get(derived_field, [])
        result = _analyze_single_derived_field(
            derived_field, row, input_fields, matched_df, output_dir
        )
        if result:
            analysis_results.append(result)

    # Write analysis summary
    if analysis_results:
        analysis_df = pl.DataFrame(analysis_results)
        analysis_path = output_dir / "derived_field_analysis.csv"
        analysis_df.write_csv(analysis_path)
        logger.info("\n[SAVED] Derived field analysis written to: %s", analysis_path)
