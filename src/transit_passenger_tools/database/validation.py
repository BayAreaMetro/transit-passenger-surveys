"""Schema validation and type enforcement for survey data."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import get_args, get_origin

import polars as pl

from transit_passenger_tools.schemas.models import SurveyResponse

from .helpers import DATA_LAKE_ROOT, connect

logger = logging.getLogger(__name__)

# Python type to Polars type mapping
python_to_polars = {
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    str: pl.Utf8,
}


def enforce_dataframe_types(df: pl.DataFrame) -> pl.DataFrame:
    """Cast DataFrame columns to match Pydantic schema types.

    This ensures that Parquet files have correct data types even if CSV
    inference gets them wrong (e.g., lat/lon as VARCHAR instead of Float64).
    """
    type_map = {}

    for field_name, field_info in SurveyResponse.model_fields.items():
        if field_name not in df.columns:
            continue

        # Get the annotation (type hint)
        annotation = field_info.annotation

        # Handle Optional/Union types using typing.get_origin and get_args
        origin = get_origin(annotation)
        if origin is not None:
            # This is a generic type (Union, Optional, etc.)
            args = get_args(annotation)
            # Filter out None to get the actual type
            non_none_types = [t for t in args if t is not type(None)]
            if non_none_types:
                annotation = non_none_types[0]

        # Map Python type to Polars type
        if annotation in python_to_polars:
            type_map[field_name] = python_to_polars[annotation]
        else:
            # Skip types we don't handle (date, datetime, enums, etc.)
            # These are handled automatically by Polars or are already correct
            pass

    # Cast columns that need type correction
    cast_exprs = []
    for col in df.columns:
        if col in type_map and df[col].dtype != type_map[col]:
            logger.info("Casting %s from %s to %s", col, df[col].dtype, type_map[col])
            cast_exprs.append(pl.col(col).cast(type_map[col], strict=False).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    return df.select(cast_exprs) if cast_exprs else df


def validate_dataframe_schema(df: pl.DataFrame, strict: bool = True) -> None:
    """Validate DataFrame schema matches expected Pydantic schema types.

    Args:
        df: DataFrame to validate
        strict: If True, raise error on type mismatches. If False, just warn.

    Raises:
        ValueError: If strict=True and schema validation fails
    """
    mismatches = []

    for field_name, field_info in SurveyResponse.model_fields.items():
        if field_name not in df.columns:
            continue

        annotation = field_info.annotation

        # Handle Optional/Union types
        origin = get_origin(annotation)
        if origin is not None:
            args = get_args(annotation)
            non_none_types = [t for t in args if t is not type(None)]
            if non_none_types:
                annotation = non_none_types[0]

        # Check if we can map this type
        if annotation not in python_to_polars:
            # Skip types we don't validate (date, datetime, enums)
            continue

        # Check type matches
        expected_type = python_to_polars[annotation]
        actual_type = df[field_name].dtype

        if actual_type != expected_type:
            mismatches.append(
                f"  {field_name}: expected {expected_type}, got {actual_type}"
            )

    if mismatches:
        msg = "Schema validation failed. Type mismatches:\n" + "\n".join(mismatches)
        if strict:
            raise ValueError(msg)
        logger.warning(msg)


def validate_referential_integrity() -> dict[str, int]:
    """Validate foreign key relationships in the data lake.

    Checks:
    1. All SurveyWeight.response_id values reference existing SurveyResponse.response_id
    2. All SurveyResponse.survey_id values reference existing SurveyMetadata.survey_id

    Returns:
        Dict with validation results: {
            'orphaned_weights': count,
            'orphaned_responses': count
        }

    Raises:
        ValueError: If referential integrity violations are found
    """
    conn = connect(read_only=True)
    try:
        results = {}
        errors = []

        # Check 1: Weights must reference existing responses
        orphaned_weights = conn.execute(
            """
            SELECT COUNT(*)
            FROM survey_weights w
            LEFT JOIN survey_responses r ON w.response_id = r.response_id
            WHERE r.response_id IS NULL
        """
        ).fetchone()[0]  # type: ignore[index]

        results["orphaned_weights"] = orphaned_weights

        if orphaned_weights > 0:
            sample = conn.execute(
                """
                SELECT w.response_id
                FROM survey_weights w
                LEFT JOIN survey_responses r ON w.response_id = r.response_id
                WHERE r.response_id IS NULL
                LIMIT 5
            """
            ).fetchall()
            sample_ids = [row[0] for row in sample]
            errors.append(
                f"{orphaned_weights} weight records reference non-existent responses. "
                f"Sample IDs: {', '.join(sample_ids)}"
            )

        # Check 2: Responses must have metadata (via survey_id foreign key)
        orphaned_responses = conn.execute(
            """
            SELECT COUNT(*)
            FROM survey_responses r
            LEFT JOIN survey_metadata m ON r.survey_id = m.survey_id
            WHERE m.survey_id IS NULL
        """
        ).fetchone()[0]  # type: ignore[index]

        results["orphaned_responses"] = orphaned_responses

        if orphaned_responses > 0:
            sample = conn.execute(
                """
                SELECT DISTINCT r.survey_id
                FROM survey_responses r
                LEFT JOIN survey_metadata m ON r.survey_id = m.survey_id
                WHERE m.survey_id IS NULL
                LIMIT 5
            """
            ).fetchall()
            sample_ids = [row[0] for row in sample]
            errors.append(
                f"{orphaned_responses} response records lack metadata. "
                f"Missing survey_id: {', '.join(sample_ids)}"
            )

        if errors:
            msg = "Referential integrity violations found:\n  - " + "\n  - ".join(errors)
            raise ValueError(msg)

        logger.info("✓ Referential integrity validation passed")
        return results
    finally:
        conn.close()


def _check_schema_compatibility(
    canonical_operator: str, survey_year: int, new_schema: dict, schema_version: int
) -> None:
    """Check if new data schema is compatible with existing data."""
    partition_dir = (
        DATA_LAKE_ROOT
        / "survey_responses"
        / f"operator={canonical_operator}"
        / f"year={survey_year}"
    )

    if not partition_dir.exists():
        return

    existing_files = list(partition_dir.glob("*.parquet"))
    if not existing_files:
        return

    existing_schema = pl.read_parquet_schema(existing_files[0])
    schema_issues = []

    # Check for column renames
    if "hispanic" in existing_schema and "is_hispanic" in new_schema:
        schema_issues.append(
            "Column rename detected: 'hispanic' → 'is_hispanic'. "
            "Run migration script: scripts/migrate_schema_v1_to_v2.py"
        )

    # Check for type changes
    common_cols = set(existing_schema.keys()) & set(new_schema.keys())
    schema_issues.extend(
        f"Type mismatch for '{col}': existing={existing_schema[col]}, new={new_schema[col]}"
        for col in common_cols
        if existing_schema[col] != new_schema[col]
    )

    if schema_issues:
        error_msg = (
            f"\n\nSCHEMA VERSION MISMATCH for {canonical_operator}/{survey_year}:\n"
            + "\n".join(f"  - {issue}" for issue in schema_issues)
            + f"\n\nCurrent schema version: {schema_version}\n"
            + "\nMigration required. See docs/schema_migration.md\n"
        )
        raise ValueError(error_msg)


def _check_duplicate_data(
    canonical_operator: str,
    survey_year: int,
    data_hash: str,
    get_latest_metadata: Callable[[str, int], dict[str, str | int] | None],
    data_lake_root: Path,
) -> tuple[Path, int, str, str] | None:
    """Check if identical data already exists. Returns existing version info if found."""
    existing_metadata = get_latest_metadata(canonical_operator, survey_year)
    if not existing_metadata or existing_metadata["hash"] != data_hash:
        return None

    existing_version = int(existing_metadata["version"])
    existing_commit = str(existing_metadata["commit"])
    partition_dir = (
        data_lake_root
        / "survey_responses"
        / f"operator={canonical_operator}"
        / f"year={survey_year}"
    )
    existing_path = partition_dir / f"data-{existing_version}-{existing_commit}.parquet"

    logger.info(
        "Data unchanged (hash %s...), reusing version %d for %s/%d",
        data_hash[:8],
        existing_version,
        canonical_operator,
        survey_year,
    )
    return existing_path, existing_version, existing_commit, data_hash
