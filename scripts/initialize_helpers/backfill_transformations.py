"""Data transformation functions for backfill operations.

This module contains functions for converting data types, handling
PUMS placeholders, and adding missing optional fields.
"""

import logging

import polars as pl

from .backfill_constants import WORD_TO_NUMBER

logger = logging.getLogger(__name__)


def convert_hispanic_to_bool(df: pl.DataFrame) -> pl.DataFrame:
    """Convert hispanic enum strings to boolean is_hispanic field."""
    if "hispanic" not in df.columns:
        return df

    return df.with_columns([
        pl.when(pl.col("hispanic").str.to_uppercase() == "HISPANIC/LATINO OR OF SPANISH ORIGIN")
          .then(pl.lit(1).cast(pl.Boolean))
          .when(pl.col("hispanic").str.to_uppercase() == "NOT HISPANIC/LATINO OR OF SPANISH ORIGIN")
          .then(pl.lit(0).cast(pl.Boolean))
          .otherwise(None)
          .alias("is_hispanic")
    ]).drop("hispanic")


def clean_numeric_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Convert 'missing' strings and field name literals to NULL for numeric fields."""
    numeric_fields = ["approximate_age", "persons", "workers", "boardings"]
    return df.with_columns([
        pl.when(pl.col(field).cast(pl.Utf8).str.to_lowercase() == "missing").then(None)
        .when(pl.col(field).cast(pl.Utf8).str.to_lowercase() == field.lower()).then(None)
        .otherwise(pl.col(field))
        .alias(field)
        for field in numeric_fields
    ])


def convert_word_numbers(df: pl.DataFrame) -> pl.DataFrame:
    """Convert word numbers to integers for numeric fields (persons, workers, vehicles)."""
    return df.with_columns([
        pl.col(field)
        .replace_strict(WORD_TO_NUMBER, default=pl.col(field))
        .cast(pl.Int32, strict=False)
        .alias(field)
        for field in ["persons", "workers", "vehicles"]
    ])


def convert_binary_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Convert TRUE/FALSE strings to 0/1 for binary flag fields."""
    binary_fields = [
        "commuter_rail_present", "heavy_rail_present", "express_bus_present",
        "ferry_present", "light_rail_present"
    ]
    return df.with_columns([
        pl.when(pl.col(field).str.to_uppercase() == "TRUE").then(1)
        .when(pl.col(field).str.to_uppercase() == "FALSE").then(0)
        .otherwise(pl.col(field).cast(pl.Int64, strict=False))
        .alias(field)
        for field in binary_fields
    ])


def add_optional_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Add missing Tier 3 (Optional) fields with NULL if they don't exist in CSV."""
    optional_fields_tier3 = [
        # Transfer routes
        "immediate_access_mode", "immediate_egress_mode",
        "first_route_before_survey_board", "second_route_before_survey_board",
        "third_route_before_survey_board", "first_route_after_survey_alight",
        "second_route_after_survey_alight", "third_route_after_survey_alight",
        # Transfer operators
        "first_before_operator", "first_before_operator_detail", "first_before_technology",
        "second_before_operator", "second_before_operator_detail", "second_before_technology",
        "third_before_operator", "third_before_operator_detail", "third_before_technology",
        "first_after_operator", "first_after_operator_detail", "first_after_technology",
        "second_after_operator", "second_after_operator_detail", "second_after_technology",
        "third_after_operator", "third_after_operator_detail", "third_after_technology",
        # Optional county/geography fields
        "home_county", "workplace_county", "school_county",
        # Optional MAZ/TAZ fields
        "orig_maz", "dest_maz", "home_maz", "workplace_maz", "school_maz",
        "orig_taz", "dest_taz", "home_taz", "workplace_taz", "school_taz",
        "first_board_tap", "last_alight_tap",
        # Optional sparse response fields
        "race_other_string", "auto_to_workers_ratio",
    ]
    for field in optional_fields_tier3:
        if field not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field))
    return df


def handle_pums_placeholders(df: pl.DataFrame) -> pl.DataFrame:
    """Handle PUMS synthetic data placeholders in a single operation.

    PUMS records lack survey metadata, so we fill required fields with placeholders:
    - field_start/field_end: Use Jan 1 - Dec 31 of survey_year
    - response_id, original_id, survey_name: Use "REGIONAL - PUMS"
    """
    return df.with_columns([
        # PUMS dates: January 1st - December 31st of survey year
        pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
          .then(
              pl.col("survey_year").cast(pl.Utf8) + "-01-01"
          )
          .otherwise(pl.col("field_start"))
          .alias("field_start"),
        pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
          .then(
              pl.col("survey_year").cast(pl.Utf8) + "-12-31"
          )
          .otherwise(pl.col("field_end"))
          .alias("field_end"),

        # PUMS identifiers
        pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
          .then(pl.lit("REGIONAL - PUMS"))
          .otherwise(pl.col("response_id"))
          .alias("response_id"),
        pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
          .then(pl.lit("REGIONAL - PUMS"))
          .otherwise(pl.col("original_id"))
          .alias("original_id"),
        pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
          .then(pl.lit("REGIONAL - PUMS"))
          .otherwise(pl.col("survey_name"))
          .alias("survey_name"),
    ])
