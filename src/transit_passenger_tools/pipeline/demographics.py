"""Transform demographic variables.

This module standardizes demographic fields including:
- Race categorization with multi-racial logic (2023+ and pre-2023)
- Language at home consolidation and uppercase conversion
- Fare medium standardization with Clipper detail integration
"""

import polars as pl

from transit_passenger_tools.codebook import Language, Race

# Required columns for race processing (at least one group must exist)
RACE_DMY_COLUMNS = [
    "race_dmy_ind",
    "race_dmy_asn",
    "race_dmy_blk",
    "race_dmy_hwi",
    "race_dmy_wht",
    "race_dmy_mdl_estn",
]


def process_race(df: pl.DataFrame) -> pl.DataFrame:
    """Process race fields into standardized race category.

    Implements current race categorization logic:
    - Multi-racial (>=2): OTHER (except white + middle_eastern → WHITE)
    - Single race: ASIAN, BLACK, WHITE (includes middle_eastern)
    - Native American or Pacific Islander (any combo): OTHER
    - race_dmy_oth only: OTHER  
    - No race: MISSING

    Args:
        df: DataFrame with race dummy columns

    Returns:
        DataFrame with standardized 'race' column, intermediate columns dropped
        
    Raises:
        ValueError: If neither race_dmy columns nor race_cat column exists
    """
    # Validate that at least one set of race columns exists
    has_race_dmy = any(col in df.columns for col in RACE_DMY_COLUMNS)
    has_race_cat = "race_cat" in df.columns

    if not has_race_dmy and not has_race_cat:
        raise ValueError(
            f"demographics transform requires either race_cat column or "
            f"at least one of: {RACE_DMY_COLUMNS}"
        )

    # Fill missing values in race dummy columns with 0
    race_cols = RACE_DMY_COLUMNS

    # Convert race columns to numeric and fill nulls with 0
    for col in race_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int32, strict=False).fill_null(0))
        else:
            # Add missing race_dmy columns as 0
            df = df.with_columns(pl.lit(0).alias(col))

    # Create race_other_string and race_cat columns if missing (leave as null, don't use "NA" sentinel)
    if "race_other_string" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("race_other_string"))
    if "race_cat" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("race_cat"))

    # Calculate race_dmy_oth based on race_other_string length (>2 chars, ignoring nulls)
    df = df.with_columns(
        pl.when(pl.col("race_other_string").is_not_null() & (pl.col("race_other_string").str.len_chars() > 2))
        .then(1)
        .otherwise(0)
        .alias("race_dmy_oth")
    )

    # Calculate race_categories based on race_cat length (>2 chars, ignoring nulls)
    df = df.with_columns(
        pl.when(pl.col("race_cat").is_not_null() & (pl.col("race_cat").str.len_chars() > 2))
        .then(1)
        .otherwise(0)
        .alias("race_categories")
    )

    # Calculate race_dmy_sum_limited (primary indicator for categorization)
    df = df.with_columns(
        (
            pl.col("race_dmy_ind")
            + pl.col("race_dmy_asn")
            + pl.col("race_dmy_blk")
            + pl.col("race_dmy_hwi")
            + pl.col("race_dmy_wht")
            + pl.col("race_dmy_mdl_estn")
        ).alias("race_dmy_sum_limited")
    )

    # Initialize race as MISSING
    df = df.with_columns(pl.lit(Race.MISSING.value).alias("race"))

    # Apply race/ethnicity categorization
    # NOTE: Order matters! White+MiddleEastern exception must come before general multi-racial check
    df = df.with_columns(
        pl.when(
            # White + Middle Eastern exception (before multi-racial check!)
            (pl.col("race_dmy_sum_limited") == 2)
            & (pl.col("race_dmy_wht") == 1)
            & (pl.col("race_dmy_mdl_estn") == 1)
        )
        .then(pl.lit(Race.WHITE.value))  # White + Middle Eastern → WHITE
        .when(pl.col("race_dmy_sum_limited") >= 2)
        .then(pl.lit(Race.OTHER.value))  # Multi-racial
        .when((pl.col("race_dmy_sum_limited") == 0) & (pl.col("race_dmy_oth") == 1))
        .then(pl.lit(Race.OTHER.value))  # Other race only
        .when((pl.col("race_dmy_sum_limited") == 1) & (pl.col("race_dmy_asn") == 1))
        .then(pl.lit(Race.ASIAN.value))
        .when((pl.col("race_dmy_sum_limited") == 1) & (pl.col("race_dmy_blk") == 1))
        .then(pl.lit(Race.BLACK.value))
        .when((pl.col("race_dmy_sum_limited") == 1) & (pl.col("race_dmy_wht") == 1))
        .then(pl.lit(Race.WHITE.value))
        .when((pl.col("race_dmy_sum_limited") == 1) & (pl.col("race_dmy_mdl_estn") == 1))
        .then(pl.lit(Race.WHITE.value))  # Middle Eastern → WHITE
        .when(pl.col("race_dmy_ind") == 1)
        .then(pl.lit(Race.OTHER.value))  # Any Native American
        .when(pl.col("race_dmy_hwi") == 1)
        .then(pl.lit(Race.OTHER.value))  # Any Pacific Islander
        .otherwise(pl.col("race"))  # Keep existing value (MISSING)
        .alias("race")
    )

    # Drop intermediate race columns
    cols_to_drop = [
        "race_dmy_ind",
        "race_dmy_asn",
        "race_dmy_blk",
        "race_dmy_hwi",
        "race_dmy_wht",
        "race_dmy_mdl_estn",
        "race_dmy_oth",
        "race_dmy_sum_limited",
        "race_cat",
        "race_categories",
        "race_other_string",
    ]
    df = df.drop([col for col in cols_to_drop if col in df.columns])

    return df


def process_language(df: pl.DataFrame) -> pl.DataFrame:
    """Consolidate language at home fields and convert to uppercase.

    Matches Build_Standard_Database.R Lines 1637-1645:
    - If language_at_home_binary == "OTHER", use language_at_home_detail
    - If language_at_home == "other", use language_at_home_detail_other
    - Convert to uppercase
    - Drop source columns

    Args:
        df: DataFrame with language_at_home_binary and related columns

    Returns:
        DataFrame with consolidated language_at_home field (uppercase)
    """
    if "language_at_home_binary" not in df.columns:
        return df

    # Consolidate: binary → detail if OTHER (case-insensitive), then detail_other if "other" (case-insensitive)
    df = df.with_columns(
        pl.when(pl.col("language_at_home_binary").str.to_titlecase() == Language.OTHER)
        .then(pl.col("language_at_home_detail"))
        .otherwise(pl.col("language_at_home_binary"))
        .alias("language_at_home")
    )

    df = df.with_columns(
        pl.when(pl.col("language_at_home").str.to_titlecase() == Language.OTHER)
        .then(pl.col("language_at_home_detail_other"))
        .otherwise(pl.col("language_at_home"))
        .alias("language_at_home")
    )

    # Convert to uppercase
    df = df.with_columns(pl.col("language_at_home").str.to_uppercase())

    # Drop source columns
    df = df.drop(
        [
            col
            for col in [
                "language_at_home_binary",
                "language_at_home_detail",
                "language_at_home_detail_other",
            ]
            if col in df.columns
        ]
    )

    return df


def process_fare_medium(df: pl.DataFrame) -> pl.DataFrame:
    """Consolidate fare medium with Clipper detail and convert to lowercase.

    Matches Build_Standard_Database.R Lines 1651-1656:
    - If clipper_detail is not null, use it instead of fare_medium
    - Convert to lowercase
    - Drop clipper_detail

    Args:
        df: DataFrame with fare_medium and clipper_detail columns

    Returns:
        DataFrame with consolidated fare_medium field (lowercase)
    """
    if "fare_medium" not in df.columns:
        return df

    # Replace fare_medium with clipper_detail when available
    if "clipper_detail" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("clipper_detail").is_not_null())
            .then(pl.col("clipper_detail"))
            .otherwise(pl.col("fare_medium"))
            .alias("fare_medium")
        )

    # Convert to lowercase
    df = df.with_columns(pl.col("fare_medium").str.to_lowercase())

    # Drop clipper_detail
    if "clipper_detail" in df.columns:
        df = df.drop("clipper_detail")

    return df


def derive_demographics(df: pl.DataFrame) -> pl.DataFrame:
    """Transform all demographic variables.

    Applies race, language, fare medium standardization, and derives year_born.

    Args:
        df: Input DataFrame with demographic fields

    Returns:
        DataFrame with transformed demographic fields including year_born_four_digit
    """
    # Process race only if race columns exist
    has_race_dmy = any(col in df.columns for col in RACE_DMY_COLUMNS)
    has_race_cat = "race_cat" in df.columns

    if has_race_dmy or has_race_cat:
        df = process_race(df)

    df = process_language(df)
    df = process_fare_medium(df)

    # Derive year_born_four_digit from approximate_age if available
    if "approximate_age" in df.columns and "survey_year" in df.columns:
        df = df.with_columns([
            pl.when(pl.col("approximate_age").is_not_null())
            .then(pl.col("survey_year") - pl.col("approximate_age"))
            .alias("year_born_four_digit")
        ])
    elif "approximate_age" in df.columns:
        # approximate_age exists but no survey_year - leave as null
        df = df.with_columns([
            pl.lit(None).cast(pl.Int32).alias("year_born_four_digit")
        ])

    return df
