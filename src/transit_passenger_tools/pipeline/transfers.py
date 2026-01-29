"""Transform transfer and technology fields.

This module:
- Assigns technology to 6 transfer legs via canonical route lookups
- Calculates 6 technology presence flags
- Calculates total boardings (1 + transfer count)
- Raises ValueError for unmapped routes
"""

import polars as pl

from transit_passenger_tools.codebook import TechnologyType
from transit_passenger_tools.utils.reference_data import ReferenceData

# Transfer leg columns (before and after surveyed vehicle)
TRANSFER_LEGS = [
    "first_before",
    "second_before",
    "third_before",
    "first_after",
    "second_after",
    "third_after"
]

# Technology types for presence flags (derived from enum)
TECHNOLOGIES = [tech.value for tech in TechnologyType]


def derive_transfer_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Transform transfer-related fields.

    Implements R Lines 1200-1478 logic:
    1. Assigns technology to 6 transfer legs via canonical route lookups
    2. Fills null technologies with "Missing"
    3. Calculates 6 technology presence flags
    4. Calculates total boardings (1 + transfer count)
    5. Raises ValueError for unmapped critical routes

    Args:
        df: Input DataFrame with survey_tech and transfer route/operator columns

    Returns:
        DataFrame with technology columns, presence flags, and boardings

    Raises:
        ValueError: If required columns missing or critical routes cannot be mapped
    """
    # Validate required columns
    required = ["vehicle_tech"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"transfers transform requires columns: {missing}")

    # Validate at least one transfer leg column set exists (route + operator)
    # Transfer columns are optional, but if present they should be complete
    has_transfer_data = any(
        f"{leg}_route_before_survey_board" in df.columns if "before" in leg
        else f"{leg}_route_after_survey_alight" in df.columns
        for leg in TRANSFER_LEGS
    )

    reference = ReferenceData()

    # Get survey metadata from first row (assume uniform within batch)
    survey_name = df["survey_name"][0]
    survey_year = df["survey_year"][0]

    result_df = df

    # For each of 6 transfer legs, assign technology
    for leg in TRANSFER_LEGS:
        route_col = f"{leg}_route_before_survey_board" if "before" in leg else f"{leg}_route_after_survey_alight"
        operator_col = f"{leg}_operator"
        tech_col = f"{leg}_technology"

        # Skip if route column doesn't exist
        if route_col not in df.columns:
            continue

        # Get unique non-null routes
        unique_routes = df.filter(pl.col(route_col).is_not_null()).select(route_col).unique().to_series().to_list()

        if not unique_routes:
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(tech_col))
            continue

        # Build technology lookup dictionary
        tech_lookup = {}
        unmapped_routes = []
        for route in unique_routes:
            try:
                tech = reference.get_route_technology(route)
                tech_lookup[route] = tech
            except ValueError:
                unmapped_routes.append(route)

        # Raise error if critical routes unmapped
        if unmapped_routes:
            msg = f"Cannot map routes to technology for {leg}: {unmapped_routes}"
            raise ValueError(msg)

        # Apply mapping
        result_df = result_df.with_columns(
            pl.col(route_col).replace(tech_lookup, default=None).alias(tech_col)
        )

    # Technology columns remain NULL when no transfer exists (not "Missing" string)

    # Calculate technology presence flags (6 technologies)
    tech_cols = ["vehicle_tech"]
    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        if tech_col in result_df.columns:
            tech_cols.append(tech_col)

    for tech in TechnologyType:
        if tech == TechnologyType.MISSING:
            continue  # Skip MISSING
        flag_col = f"{tech.to_column_name()}_present"
        tech_value = tech.value
        conditions = [pl.col(col) == tech_value for col in tech_cols if col in result_df.columns]

        if conditions:
            tech_present_expr = conditions[0]
            for condition in conditions[1:]:
                tech_present_expr = tech_present_expr | condition
            # fill_null(False) because NULL == "tech" returns NULL, not False
            result_df = result_df.with_columns(tech_present_expr.fill_null(False).alias(flag_col))
        else:
            result_df = result_df.with_columns(pl.lit(False).alias(flag_col))

    # Calculate boardings: 1 (surveyed vehicle) + count of non-null transfer technologies
    boardings_expr = pl.lit(1)
    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        if tech_col in result_df.columns:
            boardings_expr = boardings_expr + pl.col(tech_col).is_not_null().cast(pl.Int64)

    result_df = result_df.with_columns(boardings_expr.alias("boardings"))

    # Calculate transfer_from (last before operator) and transfer_to (first after operator)
    if "third_before_operator" in result_df.columns:
        result_df = result_df.with_columns(
            pl.coalesce(
                ["third_before_operator", "second_before_operator", "first_before_operator"]
            ).alias("transfer_from")
        )

    if "first_after_operator" in result_df.columns:
        result_df = result_df.with_columns(pl.col("first_after_operator").alias("transfer_to"))

    # Calculate first_board_tech (earliest technology) and last_alight_tech (latest technology)
    # Only if we have transfer columns; otherwise use vehicle_tech directly
    # coalesce naturally handles NULL values - picks first non-null
    # TODO: Why is vehicle_tech not required? I think it should be.
    if "vehicle_tech" in result_df.columns:
        # Build list of columns that exist for first_board_tech coalesce
        first_board_cols = []
        if "first_before_technology" in result_df.columns:
            first_board_cols.append("first_before_technology")
        first_board_cols.append("vehicle_tech")

        # Build list of columns that exist for last_alight_tech coalesce
        last_alight_cols = []
        for col in ["third_after_technology", "second_after_technology", "first_after_technology"]:
            if col in result_df.columns:
                last_alight_cols.append(col)
        last_alight_cols.append("vehicle_tech")

        result_df = result_df.with_columns(
            pl.coalesce(first_board_cols).alias("first_board_tech"),
            pl.coalesce(last_alight_cols).alias("last_alight_tech"),
        )

    return result_df
