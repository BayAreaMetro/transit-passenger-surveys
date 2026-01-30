"""Transform transfer and technology fields.

This module:
- Assigns technology to 6 transfer legs via canonical route lookups
- Calculates 6 technology presence flags
- Calculates total boardings (1 + transfer count)
- Raises ValueError for unmapped routes
"""

import polars as pl

from transit_passenger_tools.geocoding.reference import ReferenceData
from transit_passenger_tools.schemas.codebook import TechnologyType

# Transfer leg columns (before and after surveyed vehicle)
TRANSFER_LEGS = [
    "first_before",
    "second_before",
    "third_before",
    "first_after",
    "second_after",
    "third_after",
]

# Technology types for presence flags (derived from enum)
TECHNOLOGIES = [tech.value for tech in TechnologyType]


def _assign_transfer_technologies(
    df: pl.DataFrame, reference: ReferenceData
) -> pl.DataFrame:
    """Assign technology to each transfer leg via canonical route lookups.

    Args:
        df: DataFrame with transfer route columns
        reference: Reference data for route technology lookups

    Returns:
        DataFrame with technology columns added for each transfer leg

    Raises:
        ValueError: If critical routes cannot be mapped to technology
    """
    result_df = df

    for leg in TRANSFER_LEGS:
        route_col = (
            f"{leg}_route_before_survey_board"
            if "before" in leg
            else f"{leg}_route_after_survey_alight"
        )
        operator_col = f"{leg}_operator"
        tech_col = f"{leg}_technology"

        # Skip if route column doesn't exist
        if route_col not in df.columns:
            continue

        # Skip if operator column doesn't exist
        if operator_col not in df.columns:
            continue

        # Get unique non-null route-operator pairs
        unique_pairs = (
            df.filter(pl.col(route_col).is_not_null() & pl.col(operator_col).is_not_null())
            .select([operator_col, route_col])
            .unique()
        )

        if unique_pairs.height == 0:
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(tech_col))
            continue

        # Build technology lookup dictionary
        tech_lookup = {}
        unmapped_routes = []
        for row in unique_pairs.iter_rows(named=True):
            operator = row[operator_col]
            route = row[route_col]
            try:
                tech_info = reference.get_route_technology(operator, route)
                tech_lookup[route] = tech_info["technology"]
            except ValueError:
                unmapped_routes.append(f"{operator}/{route}")

        # Raise error if critical routes unmapped
        if unmapped_routes:
            msg = f"Cannot map routes to technology for {leg}: {unmapped_routes}"
            raise ValueError(msg)

        # Apply mapping
        result_df = result_df.with_columns(
            pl.col(route_col).replace_strict(tech_lookup, default=None).alias(tech_col)
        )

    return result_df


def _calculate_technology_presence_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate presence flags for each technology type.

    Args:
        df: DataFrame with vehicle_tech and transfer technology columns

    Returns:
        DataFrame with boolean presence flag for each technology type
    """
    # Collect all technology columns
    tech_cols = ["vehicle_tech"]
    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        if tech_col in df.columns:
            tech_cols.append(tech_col)

    result_df = df
    false_value = pl.lit(0).cast(pl.Boolean)

    for tech in TechnologyType:
        flag_col = f"{tech.to_column_name()}_present"
        tech_value = tech.value

        # Build OR condition across all technology columns
        conditions = [
            pl.col(col) == tech_value for col in tech_cols if col in df.columns
        ]

        if conditions:
            tech_present_expr = conditions[0]
            for condition in conditions[1:]:
                tech_present_expr = tech_present_expr | condition
            # fill_null(False) because NULL == "tech" returns NULL, not False
            result_df = result_df.with_columns(
                tech_present_expr.fill_null(false_value).alias(flag_col)
            )
        else:
            result_df = result_df.with_columns(false_value.alias(flag_col))

    return result_df


def _calculate_boardings(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate total boardings: 1 (surveyed vehicle) + transfer count.

    Args:
        df: DataFrame with transfer technology columns

    Returns:
        DataFrame with boardings column added
    """
    boardings_expr = pl.lit(1)
    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        if tech_col in df.columns:
            boardings_expr = boardings_expr + pl.col(tech_col).is_not_null().cast(pl.Int64)

    return df.with_columns(boardings_expr.alias("boardings"))


def _calculate_transfer_operators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate transfer_from (last before) and transfer_to (first after) operators.

    Args:
        df: DataFrame with transfer operator columns

    Returns:
        DataFrame with transfer_from and transfer_to columns added
    """
    result_df = df

    # Transfer from: last before operator (coalesce from third -> second -> first)
    if "third_before_operator" in df.columns:
        result_df = result_df.with_columns(
            pl.coalesce([
                "third_before_operator",
                "second_before_operator",
                "first_before_operator",
            ]).alias("transfer_from")
        )

    # Transfer to: first after operator
    if "first_after_operator" in df.columns:
        result_df = result_df.with_columns(
            pl.col("first_after_operator").alias("transfer_to")
        )

    return result_df


def _calculate_first_last_technologies(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate first_board_tech and last_alight_tech across all legs.

    Args:
        df: DataFrame with vehicle_tech and transfer technology columns

    Returns:
        DataFrame with first_board_tech and last_alight_tech columns added
    """
    if "vehicle_tech" not in df.columns:
        return df

    # First board: earliest technology (first before, then vehicle)
    first_board_cols = []
    if "first_before_technology" in df.columns:
        first_board_cols.append("first_before_technology")
    first_board_cols.append("vehicle_tech")

    # Last alight: latest technology (third/second/first after, then vehicle)
    last_alight_cols = [
        col for col in [
            "third_after_technology",
            "second_after_technology",
            "first_after_technology",
        ]
        if col in df.columns
    ]
    last_alight_cols.append("vehicle_tech")

    return df.with_columns(
        pl.coalesce(first_board_cols).alias("first_board_tech"),
        pl.coalesce(last_alight_cols).alias("last_alight_tech"),
    )


def derive_transfer_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Transform transfer-related fields.

    Implements R Lines 1200-1478 logic:
    1. Assigns technology to 6 transfer legs via canonical route lookups
    2. Calculates 6 technology presence flags
    3. Calculates total boardings (1 + transfer count)
    4. Calculates transfer_from and transfer_to operators
    5. Calculates first_board_tech and last_alight_tech

    Args:
        df: Input DataFrame with vehicle_tech and transfer route/operator columns

    Returns:
        DataFrame with technology columns, presence flags, and boardings

    Raises:
        ValueError: If required columns missing or critical routes cannot be mapped
    """
    # Validate required columns
    required = ["vehicle_tech"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = f"transfers transform requires columns: {missing}"
        raise ValueError(msg)

    reference = ReferenceData()

    # Process in logical steps
    result_df = df
    result_df = _assign_transfer_technologies(result_df, reference)
    result_df = _calculate_technology_presence_flags(result_df)
    result_df = _calculate_boardings(result_df)
    result_df = _calculate_transfer_operators(result_df)
    result_df = _calculate_first_last_technologies(result_df)

    return result_df
