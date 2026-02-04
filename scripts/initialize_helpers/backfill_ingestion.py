"""Database ingestion functions for backfill operations.

This module contains functions for writing survey data, metadata,
and weights to the Hive warehouse.
"""

import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools import database
from transit_passenger_tools.schemas.models import SurveyMetadata, SurveyWeight

from .backfill_constants import MAX_SAMPLE_LOCATIONS
from .backfill_validation import validate_batch_collect_errors

logger = logging.getLogger(__name__)


def _validate_batches_collect_errors(
    groups: Iterable[tuple[tuple[int, str], pl.DataFrame]]
) -> tuple[int, int, dict]:
    """Validate batches and collect all errors without writing data.

    Returns:
        Tuple of (total_valid_records, num_valid_batches, error_summary)
    """
    total_records = 0
    num_batches = 0
    error_summary = defaultdict(lambda: defaultdict(list))
    batches_with_errors = []

    for (survey_year, canonical_operator), group_df in groups:
        logger.info(
            "Processing: %s %s (%s records)", canonical_operator, survey_year, len(group_df)
        )

        batch_errors = validate_batch_collect_errors(
            group_df, survey_year, canonical_operator
        )

        if batch_errors:
            batches_with_errors.append((canonical_operator, survey_year, len(batch_errors)))
            for field, values_dict in batch_errors.items():
                for value, occurrences in values_dict.items():
                    error_summary[field][value].extend(occurrences)
            logger.warning("  Found %s field(s) with validation errors", len(batch_errors))
        else:
            logger.info("  Validation passed (would write %s records)", len(group_df))
            total_records += len(group_df)
            num_batches += 1

    if error_summary:
        logger.info(
            "\n%s\nVALIDATION ERROR SUMMARY\n%s\n\nBatches with errors: %s",
            "="*80,
            "="*80,
            len(batches_with_errors)
        )
        for operator, year, num_fields in batches_with_errors:
            logger.info("  - %s %s: %s fields with errors", operator, year, num_fields)

        logger.info("\nTotal fields with errors: %s", len(error_summary))
        for field in sorted(error_summary.keys()):
            values_dict = error_summary[field]
            logger.info("\n%s: %s invalid value(s)", field, len(values_dict))
            for value in sorted(
                values_dict.keys(), key=lambda v: len(values_dict[v]), reverse=True
            )[:20]:
                occurrences = values_dict[value]
                sample_locs = [
                    f"{op} {yr} row {idx}"
                    for op, yr, idx in occurrences[:MAX_SAMPLE_LOCATIONS]
                ]
                more_text = (
                    f" ... and {len(occurrences) - MAX_SAMPLE_LOCATIONS} more"
                    if len(occurrences) > MAX_SAMPLE_LOCATIONS else ""
                )
                logger.info(
                    "  '%s': %s occurrence(s) - %s%s",
                    value,
                    len(occurrences),
                    ", ".join(sample_locs),
                    more_text
                )

    return total_records, num_batches, dict(error_summary)


def ingest_survey_batches(
    df: pl.DataFrame, collect_errors: bool = False
) -> tuple[int, int, dict, dict]:
    """Ingest survey data to the Hive warehouse, partitioned by operator and year.

    Args:
        df: DataFrame to ingest
        collect_errors: If True, collect validation errors instead of raising

    Returns:
        Tuple of (total_records, num_batches, error_summary, version_info).
        version_info maps (operator, year) -> (version, commit)
        error_summary is a dict mapping field names to dicts of
        {invalid_value: [(operator, year, row_idx), ...]}
    """
    logger.info("Grouping data by (survey_year, canonical_operator)...")
    groups = df.group_by(["survey_year", "canonical_operator"])

    if collect_errors:
        total_records, num_batches, error_summary = _validate_batches_collect_errors(groups)
        logger.info("\n%s", "="*80)
        logger.info(
            "Validated %s batches (%s records passed) - Skipped batches with errors",
            num_batches,
            total_records
        )
        return total_records, num_batches, error_summary, {}

    # Write mode: ingest all batches
    total_records = 0
    num_batches = 0
    version_info = {}

    for (survey_year, canonical_operator), group_df in groups:
        logger.info(
            "Processing: %s %s (%s records)", canonical_operator, survey_year, len(group_df)
        )

        _output_path, version, commit, data_hash = database.ingest_survey_batch(
            df=group_df,
            survey_year=survey_year,
            canonical_operator=canonical_operator,
            validate=True,
            refresh_cache=False,
            require_clean_git=False,
        )
        version_info[(canonical_operator, survey_year)] = (version, commit, data_hash)
        total_records += len(group_df)
        num_batches += 1

    logger.info("\n%s", "="*80)
    logger.info("Wrote %s batches totaling %s records", num_batches, total_records)
    return total_records, num_batches, {}, version_info


def extract_and_ingest_metadata(
    df: pl.DataFrame, version_info: dict, collect_errors: bool = False
) -> int:
    """Extract survey metadata and write to Hive warehouse.

    Args:
        df: Survey data DataFrame
        version_info: Dict mapping (operator, year) -> (version, commit)
        collect_errors: If True, collect and report validation errors instead of failing

    Returns:
        Number of unique survey metadata records written.
    """
    logger.info("Extracting survey metadata...")

    # Get unique combinations of metadata fields
    metadata_df = (
        df.select([
            "survey_year",
            "canonical_operator",
            "survey_name",
            "field_start",
            "field_end",
        ])
        .unique()
        .with_columns([
            pl.lit("standardized_csv").alias("source"),
            pl.lit(None).alias("inflation_year"),
        ])
    )

    # Add version tracking fields from version_info
    rows = []
    for row in metadata_df.iter_rows(named=True):
        operator = row["canonical_operator"]
        year = row["survey_year"]
        version, commit, data_hash = version_info.get(
            (operator, year), (1, "unknown", "unknown")
        )
        row["survey_id"] = f"{operator}_{year}"
        row["data_version"] = version
        row["data_commit"] = commit
        row["data_hash"] = data_hash
        row["ingestion_timestamp"] = datetime.now(tz=UTC)
        row["processing_notes"] = "Initial backfill from standardized_* CSV"
        rows.append(row)

    metadata_df = pl.DataFrame(rows)

    # Reorder columns to match SurveyMetadata model field order
    metadata_df = metadata_df.select([
        "survey_id",
        "survey_year",
        "canonical_operator",
        "survey_name",
        "source",
        "field_start",
        "field_end",
        "inflation_year",
        "data_version",
        "data_commit",
        "data_hash",
        "ingestion_timestamp",
        "processing_notes",
    ])

    logger.info("Found %d unique survey combinations", len(metadata_df))

    if collect_errors:
        # Validate and collect errors
        errors = []
        valid_rows = []

        for i, row_dict in enumerate(metadata_df.iter_rows(named=True)):
            try:
                SurveyMetadata(**row_dict)
                valid_rows.append(i)
            except ValidationError as e:
                operator = row_dict["canonical_operator"]
                year = row_dict["survey_year"]
                errors.append({
                    "operator": operator,
                    "year": year,
                    "error": str(e)
                })

        if errors:
            logger.warning("\n%s", "="*80)
            logger.warning("METADATA VALIDATION ERRORS")
            logger.warning("%s", "="*80)
            logger.warning("\nFound %s metadata record(s) with validation errors:", len(errors))

            for err in errors:
                logger.warning("\n%s %s:", err["operator"], err["year"])
                # Extract just the field names from the error
                error_lines = err["error"].split("\n")
                for line in error_lines:
                    if "field_start" in line or "field_end" in line or "Input should be" in line:
                        logger.warning("  %s", line)

            logger.warning("\n%s", "="*80)
            logger.warning("Validated %s metadata records successfully", len(valid_rows))
            logger.warning("Skipped %s metadata records due to validation errors", len(errors))
            logger.warning("%s", "="*80)
            return len(valid_rows)
        logger.info("All %s metadata records validated successfully", len(metadata_df))
        return len(metadata_df)
    # Write metadata to Hive warehouse (will fail on first error)
    database.ingest_survey_metadata(metadata_df, validate=True, refresh_cache=False)
    return len(metadata_df)


def extract_and_ingest_weights(
    df: pl.DataFrame, collect_errors: bool = False
) -> int:
    """Extract survey weights and write to Hive warehouse.

    Args:
        df: Survey data DataFrame with weight and trip_weight columns
        collect_errors: If True, collect and report validation errors instead of failing

    Returns:
        Number of weight records written.
    """
    logger.info("Extracting survey weights...")

    # Select weight columns and add constants
    weights_df = df.select([
        pl.col("response_id"),
        pl.col("weight").alias("boarding_weight"),
        "trip_weight",
    ]).with_columns([
        pl.lit("baseline").alias("weight_scheme"),
        pl.lit("Baseline weights").alias("description"),
    ])

    logger.info("Found %d weight records", len(weights_df))

    if collect_errors:
        # Validate and collect errors
        errors = []
        valid_rows = []

        for i, row_dict in enumerate(weights_df.iter_rows(named=True)):
            try:
                SurveyWeight(**row_dict)
                valid_rows.append(i)
            except ValidationError as e:
                response_id = row_dict["response_id"]
                errors.append({
                    "response_id": response_id,
                    "error": str(e)
                })

        if errors:
            logger.warning("\n%s", "="*80)
            logger.warning("WEIGHT VALIDATION ERRORS")
            logger.warning("%s", "="*80)
            logger.warning("\nFound %s weight record(s) with validation errors:", len(errors))

            max_errors_to_show = 10
            for err in errors[:max_errors_to_show]:
                logger.warning("\n%s:", err["response_id"])
                error_lines = err["error"].split("\n")
                for line in error_lines:
                    if (
                        "boarding_weight" in line
                        or "trip_weight" in line
                        or "Input should be" in line
                    ):
                        logger.warning("  %s", line)

            if len(errors) > max_errors_to_show:
                logger.warning("\n... and %s more errors", len(errors) - max_errors_to_show)

            logger.warning("\n%s", "="*80)
            logger.warning("Validated %s weight records successfully", len(valid_rows))
            logger.warning("Skipped %s weight records due to validation errors", len(errors))
            logger.warning("%s", "="*80)
            return len(valid_rows)
        logger.info("All %s weight records validated successfully", len(weights_df))
        return len(weights_df)
    # Write weights to Hive warehouse (will fail on first error)
    database.ingest_survey_weights(weights_df, validate=True, refresh_cache=False)
    return len(weights_df)
