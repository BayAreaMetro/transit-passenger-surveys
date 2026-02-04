"""CSV loading and data preparation for backfill operations.

This module contains functions for finding and loading standardized CSV files,
applying all transformations, normalizations, and schema preparation.
"""

import logging
from pathlib import Path

import polars as pl

from transit_passenger_tools.schemas.models import SurveyResponse

from .backfill_constants import SHORT_DATE_LENGTH, STANDARDIZED_DATA_PATH
from .backfill_normalization import apply_normalization
from .backfill_transformations import (
    add_optional_fields,
    clean_numeric_fields,
    convert_binary_fields,
    convert_hispanic_to_bool,
    convert_word_numbers,
    handle_pums_placeholders,
)
from .backfill_typo_fixes import apply_typo_fixes, global_enum_cleanup

logger = logging.getLogger(__name__)


def find_latest_standardized_dir(base_path: Path | None = None) -> Path:
    """Find the most recent standardized data directory.

    Args:
        base_path: Optional override for STANDARDIZED_DATA_PATH
    """
    search_path = base_path or STANDARDIZED_DATA_PATH
    dirs = [d for d in search_path.iterdir()
            if d.is_dir() and d.name.startswith("standardized_")]
    if not dirs:
        msg = f"No standardized directories found in {search_path}"
        raise FileNotFoundError(msg)

    latest = sorted(dirs, key=lambda x: x.name)[-1]
    logger.info("Found latest standardized directory: %s", latest.name)
    return latest


def load_survey_data(csv_path: Path) -> pl.DataFrame:
    """Load and prepare survey data from CSV file.

    This function orchestrates all data loading, cleaning, normalization,
    and type casting operations needed to prepare the data for ingestion.

    Args:
        csv_path: Path to survey_combined.csv file

    Returns:
        Prepared DataFrame ready for validation and ingestion
    """
    logger.info("Reading CSV: %s", csv_path)

    df = pl.read_csv(
        str(csv_path),
        null_values=["NA", "N/A", "", "Inf"],
        infer_schema_length=10000
    )
    logger.info("Loaded %s rows with %s columns", len(df), len(df.columns))

    # Calculate household_income from bounds if hh_income_nominal_continuous is missing/invalid
    df = df.with_columns([
        pl.col("hh_income_nominal_continuous").cast(
            pl.Float64, strict=False
        ).alias("hh_income_nominal_continuous")
    ])

    # For null values, calculate midpoint from bounds
    df = df.with_columns([
        pl.when(pl.col("hh_income_nominal_continuous").is_null())
        .then(
            (pl.col("income_lower_bound") + pl.col("income_upper_bound")) / 2.0
        )
        .otherwise(pl.col("hh_income_nominal_continuous"))
        .alias("hh_income_nominal_continuous"),
    ])
    logger.info("Calculated household_income from bounds for rows with missing/invalid values")

    # Rename columns to match schema
    df = df.rename({
        "unique_ID": "response_id",
        "ID": "original_id",
        "household_income": "household_income_category",
        "hh_income_nominal_continuous": "household_income",
        "survey_tech": "vehicle_tech"
    })

    # Strip whitespace from all string columns
    string_cols = [
        col for col, dtype in zip(df.columns, df.dtypes, strict=False)
        if dtype == pl.Utf8
    ]
    df = df.with_columns([pl.col(col).str.strip_chars() for col in string_cols])

    # Fill NULL canonical_operator with "REGIONAL - PUMS"
    df = df.with_columns(
        pl.col("canonical_operator").fill_null("REGIONAL - PUMS")
    )

    # Handle PUMS placeholders (dates, IDs, survey_name)
    df = handle_pums_placeholders(df)

    # Fix date formats
    df = df.with_columns([
        pl.when(pl.col("field_start").str.len_chars() == SHORT_DATE_LENGTH)
        .then(pl.col("field_start").str.strptime(pl.Date, format="%m-%d-%y", strict=False))
        .otherwise(pl.col("field_start").str.to_date(format="%Y-%m-%d", strict=False))
        .alias("field_start"),

        pl.when(pl.col("field_end").str.len_chars() == SHORT_DATE_LENGTH)
        .then(pl.col("field_end").str.strptime(pl.Date, format="%m-%d-%y", strict=False))
        .otherwise(pl.col("field_end").str.to_date(format="%Y-%m-%d", strict=False))
        .alias("field_end"),
    ])

    # Data type conversions
    df = convert_hispanic_to_bool(df)
    df = clean_numeric_fields(df)
    df = convert_word_numbers(df)

    # Standardize field names
    rename_map = {col: col.replace(".", "_").replace("_GEOID20", "_GEOID")
                  for col in df.columns if "." in col or "_GEOID20" in col}
    if rename_map:
        logger.info("Standardizing %s column names...", len(rename_map))
        df = df.rename(rename_map)

    # Handle placeholder values
    placeholder_fields = {
        "approximate_age": "approximate_age",
        "transfer_from": "None",
        "transfer_to": "None",
        "survey_time": "None",
        "day_part": "None",
        "vehicle_numeric_cat": "None",
        "worker_numeric_cat": "None",
        "tour_purp_case": "None",
        "transfers_surveyed": "None",
    }

    df = df.with_columns([
        pl.when(pl.col(col) == placeholder_fields[col]).then(None).otherwise(pl.col(col)).alias(col)
        if col in placeholder_fields else pl.col(col)
        for col in df.columns
    ])

    # Convert binary fields
    df = convert_binary_fields(df)

    # Add optional fields
    df = add_optional_fields(df)

    # Cast numeric fields to proper types
    logger.info("Casting numeric fields to proper types...")

    float_fields = [
        "orig_lat", "orig_lon", "dest_lat", "dest_lon",
        "home_lat", "home_lon", "workplace_lat", "workplace_lon",
        "school_lat", "school_lon", "first_board_lat", "first_board_lon",
        "last_alight_lat", "last_alight_lon", "survey_board_lat", "survey_board_lon",
        "survey_alight_lat", "survey_alight_lon",
        "distance_orig_dest", "distance_board_alight", "distance_orig_first_board",
        "distance_orig_survey_board", "distance_survey_alight_dest", "distance_last_alight_dest",
        "household_income", "income_lower_bound", "income_upper_bound",
    ]

    int_fields = [
        "depart_hour", "return_hour", "approximate_age", "boardings",
        "persons", "workers", "vehicles",
        "orig_maz", "dest_maz", "home_maz", "workplace_maz", "school_maz",
        "orig_taz", "dest_taz", "home_taz", "workplace_taz", "school_taz",
        "first_board_tap", "last_alight_tap",
        "first_board_tm1_taz", "last_alight_tm1_taz", "survey_board_tm1_taz",
        "survey_alight_tm1_taz", "orig_tm2_taz", "dest_tm2_taz", "home_tm2_taz",
        "workplace_tm2_taz", "school_tm2_taz", "first_board_tm2_taz", "last_alight_tm2_taz",
        "survey_board_tm2_taz", "survey_alight_tm2_taz", "orig_tm2_maz", "dest_tm2_maz",
        "home_tm2_maz", "workplace_tm2_maz", "school_tm2_maz",
        "first_board_tm2_maz", "last_alight_tm2_maz", "survey_board_tm2_maz",
        "survey_alight_tm2_maz",
    ]

    string_fields = [
        "first_route_before_survey_board", "second_route_before_survey_board",
        "third_route_before_survey_board", "first_route_after_survey_alight",
        "second_route_after_survey_alight", "third_route_after_survey_alight",
        "race_other_string", "home_county", "workplace_county", "school_county",
    ]

    cast_exprs = []
    for col in df.columns:
        if col in float_fields:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
        elif col in int_fields:
            cast_exprs.append(pl.col(col).cast(pl.Int64, strict=False).alias(col))
        elif col in string_fields:
            cast_exprs.append(pl.col(col).cast(pl.Utf8, strict=False).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    df = df.select(cast_exprs)
    logger.info("Type casting complete")

    # Apply text normalization
    df = apply_normalization(df)

    # Apply typo fixes and value mappings
    df = apply_typo_fixes(df)

    # Global enum cleanup
    df = global_enum_cleanup(df)

    # Add survey_id (computed from canonical_operator and survey_year)
    df = df.with_columns(
        (
            pl.col("canonical_operator") + "_" + pl.col("survey_year").cast(pl.Utf8)
        ).alias("survey_id")
    )

    logger.info("Data loading and cleaning complete")
    return df


def reorder_columns_to_match_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Reorder DataFrame columns to match Pydantic model field order.

    This ensures Parquet files have columns in the same logical order as the
    data model definition, with primary keys first.

    Args:
        df: DataFrame with survey response data

    Returns:
        DataFrame with columns reordered to match SurveyResponse model
    """
    # Get field order from Pydantic model
    schema_order = list(SurveyResponse.model_fields.keys())

    # Select only fields that exist in both schema and DataFrame
    existing_cols = [col for col in schema_order if col in df.columns]

    # Preserve extra columns (geography, derived fields) at the end
    extra_cols = [col for col in df.columns if col not in schema_order]

    logger.info(
        "Reordering %s columns to match schema (%s extra columns preserved)",
        len(existing_cols),
        len(extra_cols)
    )

    return df.select(existing_cols + extra_cols)
