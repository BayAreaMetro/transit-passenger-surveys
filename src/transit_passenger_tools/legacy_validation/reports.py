"""Report generation for validation comparison."""

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

MATCH_RATE = 100.0


def compare_field_by_field(matched_df: pl.DataFrame, output_dir: Path) -> pl.DataFrame:
    """Compare all common fields on matched records, requiring 100% exact match.

    Args:
        matched_df: DataFrame from inner join on response_id
        output_dir: Directory to write reports

    Returns:
        DataFrame with per-field comparison statistics
    """
    logger.info("%s", "\n" + "=" * 80)
    logger.info("FIELD-BY-FIELD COMPARISON")
    logger.info("%s", "=" * 80)

    # Find common fields (excluding _new suffix fields)
    all_cols = set(matched_df.columns)
    legacy_cols = {c for c in all_cols if not c.endswith("_new")}
    new_cols = {c.replace("_new", "") for c in all_cols if c.endswith("_new")}
    common_fields = sorted(legacy_cols & new_cols - {"response_id"})

    logger.info("\nComparing %s common fields...", len(common_fields))

    field_results = []
    all_mismatches = []

    for field in common_fields:
        legacy_col = field
        new_col = f"{field}_new"

        # Skip if either column doesn't exist
        if legacy_col not in matched_df.columns or new_col not in matched_df.columns:
            continue

        # Get dtypes for type checking
        legacy_dtype = matched_df[legacy_col].dtype
        new_dtype = matched_df[new_col].dtype

        # Check if types are incompatible and need casting
        is_legacy_numeric = legacy_dtype in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.Float32,
            pl.Float64,
        ]
        is_new_numeric = new_dtype in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.Float32,
            pl.Float64,
        ]

        # If one is numeric and one is string, cast both to string for comparison
        if is_legacy_numeric != is_new_numeric:
            # Apply casting to the DataFrame columns
            matched_df = matched_df.with_columns(
                [
                    pl.col(legacy_col).cast(pl.Utf8).alias(legacy_col),
                    pl.col(new_col).cast(pl.Utf8).alias(new_col),
                ]
            )

        # Count exact matches (null == null is a match)
        match_expr = (
            pl.when(pl.col(legacy_col).is_null() & pl.col(new_col).is_null())
            .then(pl.lit(value=True))
            .when(pl.col(legacy_col) == pl.col(new_col))
            .then(pl.lit(value=True))
            .otherwise(pl.lit(value=False))
        )

        matches_df = matched_df.with_columns([match_expr.alias("_match")])
        match_count = matches_df.filter(pl.col("_match")).height
        mismatch_count = matches_df.filter(~pl.col("_match")).height
        match_rate = 100 * match_count / len(matched_df) if len(matched_df) > 0 else 0

        # Count null mismatches specifically
        null_mismatch_count = matches_df.filter(
            (pl.col(legacy_col).is_null() & pl.col(new_col).is_not_null())
            | (pl.col(legacy_col).is_not_null() & pl.col(new_col).is_null())
        ).height

        field_results.append(
            {
                "field_name": field,
                "total_records": len(matched_df),
                "matches": match_count,
                "mismatches": mismatch_count,
                "match_rate": round(match_rate, 4),
                "null_mismatches": null_mismatch_count,
            }
        )

        # Log per-field result
        if match_rate == MATCH_RATE:
            logger.info("  [PASS] %s: 100%% match", field)
        else:
            logger.warning(
                "  [FAIL] %s: %.2f%% match (%s mismatches)",
                field,
                match_rate,
                f"{mismatch_count:,}",
            )

        # Collect mismatched records for detailed report
        if mismatch_count > 0:
            mismatches = matches_df.filter(~pl.col("_match")).select(
                [
                    "response_id",
                    pl.lit(field).alias("field_name"),
                    pl.col(legacy_col).cast(pl.Utf8).alias("legacy_value"),
                    pl.col(new_col).cast(pl.Utf8).alias("new_value"),
                ]
            )
            all_mismatches.append(mismatches)

    # Write field summary
    field_summary_df = pl.DataFrame(field_results)
    field_summary_path = output_dir / "field_summary.csv"
    field_summary_df.write_csv(field_summary_path)
    logger.info("\n[SAVED] Field summary written to: %s", field_summary_path)

    # Write field mismatches (limit to 10,000 rows to avoid huge files)
    if all_mismatches:
        mismatches_df = pl.concat(all_mismatches)
        mismatches_path = output_dir / "field_mismatches.csv"
        mismatches_df.head(10000).write_csv(mismatches_path)
        logger.info(
            "[SAVED] Field mismatches written to: %s (%s total, showing first 10,000)",
            mismatches_path,
            f"{len(mismatches_df):,}",
        )

    return field_summary_df


def _write_record_counts(
    legacy_df: pl.DataFrame,
    new_df: pl.DataFrame,
    matched_df: pl.DataFrame | None,
) -> str:
    """Generate record count section."""
    lines = [
        "RECORD COUNTS\n",
        "-" * 80 + "\n",
        f"  Legacy database:     {len(legacy_df):,}\n",
        f"  New pipeline:        {len(new_df):,}\n",
        f"  Difference:          {len(new_df) - len(legacy_df):+,}\n",
    ]

    if matched_df is not None:
        missing_in_new = len(legacy_df) - len(matched_df)
        missing_in_legacy = len(new_df) - len(matched_df)
        lines.extend(
            [
                f"  Matched records:     {len(matched_df):,}\n",
                f"  Missing in new:      {missing_in_new:,}\n",
                f"  Missing in legacy:   {missing_in_legacy:,}\n",
            ]
        )
    else:
        lines.append("  Matched records:     0 (CRITICAL FAILURE)\n")

    return "".join(lines)


def _write_weight_totals(legacy_df: pl.DataFrame, new_df: pl.DataFrame) -> str:
    """Generate weight total section."""
    lines = ["\nWEIGHT TOTALS\n", "-" * 80 + "\n"]

    if "weight" in legacy_df.columns and "weight" in new_df.columns:
        legacy_weight = legacy_df.select(pl.col("weight").sum()).item()
        new_weight = new_df.select(pl.col("weight").sum()).item()
        diff = new_weight - legacy_weight
        diff_pct = 100 * diff / legacy_weight if legacy_weight != 0 else 0

        lines.extend(
            [
                f"  Legacy:         {legacy_weight:,.2f}\n",
                f"  New:            {new_weight:,.2f}\n",
                f"  Difference:     {diff:+,.2f} ({diff_pct:+.4f}%)\n",
            ]
        )

        if abs(diff_pct) < 0.01:  # noqa: PLR2004
            lines.append("  Status:         [PASS] (within 0.01% tolerance)\n")
        else:
            lines.append("  Status:         [WARN] Difference exceeds 0.01% tolerance\n")
    else:
        lines.append("  [N/A] Weight column not found in one or both datasets\n")

    return "".join(lines)


def _write_field_comparison(field_summary_df: pl.DataFrame | None) -> str:
    """Generate field comparison section."""
    lines = ["\nFIELD COMPARISON RESULTS\n", "-" * 80 + "\n"]

    if field_summary_df is None:
        lines.append("  [N/A] No field comparison performed (no matched records)\n")
        return "".join(lines)

    perfect_match = field_summary_df.filter(pl.col("match_rate") == MATCH_RATE)
    imperfect_match = field_summary_df.filter(pl.col("match_rate") < MATCH_RATE)

    lines.extend(
        [
            f"  Perfect matches (100%):  {len(perfect_match)} fields\n",
            f"  Imperfect matches:       {len(imperfect_match)} fields\n",
        ]
    )

    if len(imperfect_match) > 0:
        lines.append("\n  TOP 10 PROBLEMATIC FIELDS:\n")
        top_problems = imperfect_match.sort("match_rate").head(10)
        for i, row in enumerate(top_problems.iter_rows(named=True), 1):
            lines.append(
                f"    {i:2d}. {row['field_name']:30s} {row['match_rate']:6.2f}% "
                f"({row['mismatches']:,} mismatches)\n"
            )

    return "".join(lines)


def _write_actionable_steps(
    matched_df: pl.DataFrame | None,
    field_summary_df: pl.DataFrame | None,
) -> str:
    """Generate actionable next steps section."""
    lines = ["\nACTIONABLE NEXT STEPS\n", "-" * 80 + "\n"]

    if matched_df is None:
        lines.extend(
            [
                "  1. CRITICAL: Fix response_id generation - no records matched!\n",
                "  2. Review missing_in_*.csv files to understand discrepancy\n",
            ]
        )
    elif (
        field_summary_df is not None
        and len(field_summary_df.filter(pl.col("match_rate") < MATCH_RATE)) > 0
    ):
        lines.extend(
            [
                "  1. Review field_mismatches.csv for specific value differences\n",
                "  2. Review derived_field_analysis.csv to distinguish:\n",
                "     - Data issues (input fields differ) -> Fix preprocessing\n",
                "     - Logic changes (inputs match, output differs) -> Expected pipeline evolution\n",  # noqa: E501
                "  3. For fields with 'Missing' strings, update preprocessing to convert to null\n",
                "  4. For enum violations, standardize values in preprocessing\n",
            ]
        )
    else:
        lines.append(
            "  [SUCCESS] All validations passed! New pipeline output matches legacy data.\n"
        )

    return "".join(lines)


def generate_validation_summary(
    legacy_df: pl.DataFrame,
    new_df: pl.DataFrame,
    matched_df: pl.DataFrame | None,
    field_summary_df: pl.DataFrame | None,
    output_dir: Path,
    operator: str,
    year: int,
) -> None:
    """Generate comprehensive validation summary report.

    Args:
        legacy_df: Legacy database records
        new_df: New pipeline records
        matched_df: Matched records (None if no matches)
        field_summary_df: Field comparison stats (None if no comparison)
        output_dir: Directory to write report
        operator: Canonical operator name
        year: Survey year
    """
    logger.info("%s", "\n" + "=" * 80)
    logger.info("GENERATING VALIDATION SUMMARY")
    logger.info("%s", "=" * 80)

    summary_path = output_dir / "validation_summary.txt"

    # Build complete summary
    summary = "".join(
        [
            "=" * 80 + "\n",
            f"{operator} {year} PIPELINE VALIDATION SUMMARY\n",
            "=" * 80 + "\n\n",
            _write_record_counts(legacy_df, new_df, matched_df),
            _write_weight_totals(legacy_df, new_df),
            _write_field_comparison(field_summary_df),
            _write_actionable_steps(matched_df, field_summary_df),
            "\n" + "=" * 80 + "\n",
        ]
    )

    # Write to file
    summary_path.write_text(summary)
    logger.info("[SAVED] Validation summary written to: %s", summary_path)

    # Also log summary to console
    logger.info("%s", "\n" + summary)
