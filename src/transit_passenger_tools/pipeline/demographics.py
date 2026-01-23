"""Demographics processing module.

Processes race, age, language, and income fields.
"""
import logging

import polars as pl

logger = logging.getLogger(__name__)


def process_race(df: pl.DataFrame) -> pl.DataFrame:
    """Process race fields into standardized race and hispanic categories.

    Implements 2023+ logic:
    - If 2+ races selected -> "OTHER" (except white + middle_eastern -> "WHITE")
    - If 1 race selected -> Use that race
    - If 0 races but "other" text -> "OTHER"
    - If 0 races and no "other" text -> "MISSING"

    Args:
        df: DataFrame with race dummy columns (race_dmy_*)

    Returns:
        DataFrame with added 'race' and 'hispanic' columns
    """
    logger.info("Processing race/ethnicity")

    # Count number of races selected (excluding hispanic)
    race_dummies = [
        "race_dmy_asn",
        "race_dmy_blk",
        "race_dmy_ind",
        "race_dmy_hwi",
        "race_dmy_wht",
        "race_dmy_mdl_estn"
    ]

    # Check which columns exist
    existing_race_cols = [col for col in race_dummies if col in df.columns]

    if not existing_race_cols:
        logger.warning("No race dummy columns found, setting race to MISSING")
        return df.with_columns([
            pl.lit("MISSING").alias("race"),
            pl.lit(None).cast(pl.Int64).alias("hispanic")
        ])

    # Sum race selections
    race_sum_expr = sum(pl.col(col).fill_null(0) for col in existing_race_cols)

    result_df = df.with_columns([
        race_sum_expr.alias("_race_count")
    ])

    # Apply race logic
    race_expr = (
        pl.when(pl.col("_race_count") > 1)
        .then(
            # Multiple races - check for white + middle eastern exception
            pl.when(
                (pl.col("race_dmy_wht").fill_null(0) == 1) &
                (pl.col("race_dmy_mdl_estn").fill_null(0) == 1) &
                (pl.col("_race_count") == 2)  # noqa: PLR2004
            )
            .then(pl.lit("WHITE"))
            .otherwise(pl.lit("OTHER"))
        )
        .when(pl.col("_race_count") == 1)
        .then(
            # Single race - determine which one
            pl.when(pl.col("race_dmy_asn").fill_null(0) == 1)
            .then(pl.lit("ASIAN"))
            .when(pl.col("race_dmy_blk").fill_null(0) == 1)
            .then(pl.lit("BLACK"))
            .when(pl.col("race_dmy_ind").fill_null(0) == 1)
            .then(pl.lit("NATIVE AMERICAN"))
            .when(pl.col("race_dmy_hwi").fill_null(0) == 1)
            .then(pl.lit("PACIFIC ISLANDER"))
            .when(pl.col("race_dmy_wht").fill_null(0) == 1)
            .then(pl.lit("WHITE"))
            .when(pl.col("race_dmy_mdl_estn").fill_null(0) == 1)
            .then(pl.lit("MIDDLE EASTERN"))
            .otherwise(pl.lit("OTHER"))
        )
        .when(
            (pl.col("_race_count") == 0) &
            (pl.col("race_other_string").is_not_null())
        )
        .then(pl.lit("OTHER"))
        .otherwise(pl.lit("MISSING"))
    )

    result_df = result_df.with_columns([
        race_expr.alias("race")
    ])

    # Process hispanic (separate from race)
    if "hispanic" in df.columns:
        # Already exists, just ensure it's binary
        result_df = result_df.with_columns([
            pl.when(pl.col("hispanic") == 1)
            .then(pl.lit(1))
            .when(pl.col("hispanic") == 0)
            .then(pl.lit(0))
            .otherwise(pl.lit(None).cast(pl.Int64))
            .alias("hispanic")
        ])
    else:
        logger.warning("No hispanic column found")
        result_df = result_df.with_columns([
            pl.lit(None).cast(pl.Int64).alias("hispanic")
        ])

    # Drop temporary column
    result_df = result_df.drop("_race_count")

    # Log distribution
    race_counts = result_df.group_by("race").agg(pl.count()).sort("race")
    logger.info("Race distribution:\n%s", race_counts)

    return result_df


def calculate_age(df: pl.DataFrame, survey_year: int) -> pl.DataFrame:
    """Calculate approximate age from year_born.

    Args:
        df: DataFrame with year_born column
        survey_year: Year of survey for age calculation

    Returns:
        DataFrame with added 'approximate_age' column
    """
    logger.info("Calculating approximate age")

    if "year_born" not in df.columns and "year_born_four_digit" not in df.columns:
        logger.warning("No year_born column found")
        return df.with_columns([
            pl.lit(None).cast(pl.Int64).alias("approximate_age")
        ])

    year_born_col = "year_born_four_digit" if "year_born_four_digit" in df.columns else "year_born"

    result_df = df.with_columns([
        (pl.lit(survey_year) - pl.col(year_born_col)).alias("approximate_age")
    ])

    # Log age distribution
    age_stats = result_df.select([
        pl.col("approximate_age").min().alias("min"),
        pl.col("approximate_age").max().alias("max"),
        pl.col("approximate_age").mean().alias("mean"),
        pl.col("approximate_age").median().alias("median"),
    ])
    logger.info("Age statistics: %s", age_stats)

    return result_df


def normalize_language(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize language fields.

    Creates standardized language_at_home_binary and language_at_home_detail.

    Args:
        df: DataFrame with language columns

    Returns:
        DataFrame with normalized language fields
    """
    logger.info("Normalizing language")

    # If language_at_home_binary doesn't exist, create it
    if "language_at_home_binary" not in df.columns:
        logger.warning("No language_at_home_binary column found")
        result_df = df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("language_at_home_binary")
        ])
    else:
        # Normalize to uppercase
        result_df = df.with_columns([
            pl.col("language_at_home_binary").str.to_uppercase()
        ])

    # Normalize detail field if it exists
    if "language_at_home_detail" in df.columns:
        result_df = result_df.with_columns([
            pl.col("language_at_home_detail").str.to_uppercase()
        ])

    return result_df


def process_income(df: pl.DataFrame) -> pl.DataFrame:
    """Process income fields.

    For now, just validates household_income is in expected categories.
    Future: Could add continuous income imputation.

    Args:
        df: DataFrame with household_income column

    Returns:
        DataFrame (unchanged for now)
    """
    logger.info("Processing income")

    if "household_income" in df.columns:
        # Log distribution
        income_counts = df.group_by("household_income").agg(pl.count()).sort("household_income")
        logger.info("Income distribution:\n%s", income_counts)
    else:
        logger.warning("No household_income column found")

    return df


def process_demographics(
    df: pl.DataFrame,
    survey_year: int
) -> pl.DataFrame:
    """Process all demographic fields.

    Args:
        df: Input DataFrame
        survey_year: Year of survey

    Returns:
        DataFrame with processed demographic fields
    """
    logger.info("Processing demographics")

    result_df = df
    result_df = process_race(result_df)
    result_df = calculate_age(result_df, survey_year)
    result_df = normalize_language(result_df)
    result_df = process_income(result_df)

    logger.info("Demographics processing complete")
    return result_df
