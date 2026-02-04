"""Schema validation functions for backfill operations.

This module contains functions for validating survey data against
Pydantic schemas and collecting validation errors.
"""

import logging
from collections import defaultdict

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools.schemas.models import SurveyResponse

logger = logging.getLogger(__name__)


def validate_schema(df: pl.DataFrame, sample_size: int = 100) -> None:
    """Validate a sample of records against the Pydantic schema.

    Note: Only validates fields that exist in both the DataFrame and the schema.
    The CSV may have additional columns (geography, derived fields) that we want to preserve.
    """
    logger.info("Validating %s sample records against schema...", sample_size)
    sample = df.head(sample_size)

    # Get schema fields
    schema_fields = set(SurveyResponse.model_fields.keys())
    df_columns = set(df.columns)

    # Only validate columns that exist in both
    common_fields = schema_fields & df_columns
    logger.info(
        "Validating %s common fields (DataFrame has %s columns total)",
        len(common_fields),
        len(df_columns)
    )

    validation_errors = []
    error_details = {}  # Group errors by field

    for i, row_dict in enumerate(sample.iter_rows(named=True)):
        # Filter to only fields in schema
        filtered_row = {k: v for k, v in row_dict.items() if k in schema_fields}
        try:
            SurveyResponse(**filtered_row)
        except ValidationError as e:
            validation_errors.append((i, e))
            # Group errors by field
            for error in e.errors():
                field = error["loc"][0] if error["loc"] else "unknown"
                if field not in error_details:
                    error_details[field] = {
                        "count": 0,
                        "unique_values": set(),
                        "rows": [],
                        "error_types": set(),
                    }
                error_details[field]["count"] += 1
                error_details[field]["rows"].append(i)
                error_details[field]["error_types"].add(error["type"])
                # Store actual field value, not entire record
                if "input" in error:
                    # For missing fields, input is the whole dict - just note it's missing
                    if error["type"] == "missing" or isinstance(error["input"], dict):
                        error_details[field]["unique_values"].add("<missing>")
                    else:
                        error_details[field]["unique_values"].add(str(error["input"]))

    if validation_errors:
        logger.error(
            "Validation failed on %s of %s sample records:",
            len(validation_errors),
            len(sample)
        )
        logger.error("\nErrors grouped by field:")
        for field, details in sorted(error_details.items()):
            logger.error("\n  Field: %s", field)
            logger.error("    Error types: %s", ", ".join(sorted(details["error_types"])))
            logger.error(
                "    Failed in %s records (rows: %s...)",
                details["count"], details["rows"][:5]
            )
            if details["unique_values"]:
                logger.error(
                    "    Unique problematic values (%s total): %s",
                    len(details["unique_values"]),
                    sorted(details["unique_values"])[:10]
                )
        msg = "Schema validation failed. Fix data or schema before importing."
        raise ValueError(msg)

    logger.info("Schema validation passed")


def validate_batch_collect_errors(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str
) -> dict:
    """Validate a batch and collect all validation errors.

    Returns:
        Dict mapping field names to dicts of {invalid_value: [(operator, year, row_idx), ...]}
    """
    errors = defaultdict(lambda: defaultdict(list))

    for row_idx, row in enumerate(df.iter_rows(named=True)):
        try:
            SurveyResponse(**row)
        except ValidationError as e:
            # Parse validation error and collect field/value info
            for error in e.errors():
                field = error["loc"][0] if error["loc"] else "unknown"
                # Get the actual value from the row
                if field in row:
                    invalid_value = str(row[field])
                    errors[field][invalid_value].append(
                        (canonical_operator, survey_year, row_idx)
                    )

    return dict(errors)
