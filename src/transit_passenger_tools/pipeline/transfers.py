"""Transform transfer and technology fields.

This module:
- Assigns technology to 6 transfer legs via canonical route lookups
- Calculates 6 technology presence flags
- Calculates total boardings (1 + transfer count)
- Raises ValueError for unmapped routes
"""

import logging

import polars as pl

from transit_passenger_tools.geocoding.reference import ReferenceData
from transit_passenger_tools.schemas import FieldDependencies
from transit_passenger_tools.schemas.codebook import TechnologyType

logger = logging.getLogger(__name__)

# Field dependencies
FIELD_DEPENDENCIES = FieldDependencies(
    inputs=[
        "vehicle_tech",
        "transfer_from",
        "transfer_to",
        "first_before_operator",
        "second_before_operator",
        "third_before_operator",
        "first_after_operator",
        "second_after_operator",
        "third_after_operator",
        "first_before_route_before_survey_board",
        "second_before_route_before_survey_board",
        "third_before_route_before_survey_board",
        "first_after_route_after_survey_alight",
        "second_after_route_after_survey_alight",
        "third_after_route_after_survey_alight",
    ],
    outputs=[
        "first_before_technology",
        "second_before_technology",
        "third_before_technology",
        "first_after_technology",
        "second_after_technology",
        "third_after_technology",
        "commuter_rail_present",
        "heavy_rail_present",
        "express_bus_present",
        "ferry_present",
        "light_rail_present",
        "local_bus_present",
        "boardings",
    ],
)

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


def _assign_transfer_technologies(df: pl.DataFrame, reference: ReferenceData) -> pl.DataFrame:
    """Assign technology to each transfer leg via canonical route lookups.

    Skips legs where technology columns already exist (from preprocessing).

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

        # Skip if technology already assigned (from preprocessing)
        if tech_col in df.columns:
            continue

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

        # Raise error if routes unmapped
        if unmapped_routes:
            msg = (
                f"Could not map {len(unmapped_routes)} routes to technology "
                f"for {leg}: {unmapped_routes[:10]}"
            )
            raise ValueError(msg)

        # Apply mapping
        result_df = result_df.with_columns(
            pl.col(route_col).replace_strict(tech_lookup, default=None).alias(tech_col)
        )

    return result_df


def _create_null_transfer_technologies(df: pl.DataFrame) -> pl.DataFrame:
    """Create null technology columns for all transfer legs when skipping lookups.

    Args:
        df: DataFrame with transfer route columns

    Returns:
        DataFrame with null technology columns added for each transfer leg
    """
    result_df = df

    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        result_df = result_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(tech_col))

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
        conditions = [pl.col(col) == tech_value for col in tech_cols if col in df.columns]

        if conditions:
            tech_present_expr = conditions[0]
            for condition in conditions[1:]:
                tech_present_expr = tech_present_expr | condition
            # fill_null(False) because NULL == "tech" returns NULL, not False
            # Cast to Int8 to match legacy database (0/1 instead of False/True)
            result_df = result_df.with_columns(
                tech_present_expr.fill_null(false_value).cast(pl.Int8).alias(flag_col)
            )
        else:
            result_df = result_df.with_columns(pl.lit(0).cast(pl.Int8).alias(flag_col))

    return result_df


def _calculate_boardings(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate total boardings: 1 (surveyed vehicle) + transfer count.

    Uses technology columns if available, otherwise falls back to operator columns.

    Args:
        df: DataFrame with transfer technology or operator columns

    Returns:
        DataFrame with boardings column added
    """
    boardings_expr = pl.lit(1)

    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        operator_col = f"{leg}_operator"

        # Prefer technology column if it exists and has non-null values
        if tech_col in df.columns:
            boardings_expr = boardings_expr + pl.col(tech_col).is_not_null().cast(pl.Int64)
        elif operator_col in df.columns:
            # Fall back to operator column when technology unavailable
            boardings_expr = boardings_expr + pl.col(operator_col).is_not_null().cast(pl.Int64)

    return df.with_columns(boardings_expr.alias("boardings"))


def _titlecase_operator(operator_name: str | None) -> str | None:
    """Convert uppercase operator name to legacy title case format.

    Matches legacy R script's operator display formatting:
    - "AC TRANSIT" → "AC Transit"
    - "MUNI" → "Muni"
    - "SAMTRANS" → "SamTrans"
    - "WESTCAT" → "WestCat"
    - "AIRTRAIN" → "AirTrain"

    Args:
        operator_name: Uppercase operator name

    Returns:
        Title cased operator name matching legacy format
    """
    if operator_name is None:
        return None

    # Special case mappings extracted from legacy BART 2015 database
    # - Compound words: SAMTRANS→SamTrans, WESTCAT→WestCat
    # - Acronyms: VTA, LAVTA (identity mappings needed because .capitalize() would break them)
    # - Multi-word: AC TRANSIT→AC Transit
    special_cases = {
        "AC TRANSIT": "AC Transit",
        "EMERYVILLE MTA": "Emeryville MTA",
        "LAVTA": "LAVTA",  # Needed: would become "Lavta" otherwise
        "SAMTRANS": "SamTrans",
        "SOLTRANS": "SolTrans",
        "TRI-DELTA": "Tri-Delta",
        "VTA": "VTA",  # Needed: would become "Vta" otherwise
        "WESTCAT": "WestCat",
    }

    if operator_name in special_cases:
        return special_cases[operator_name]

    # Default: title case each word and strip any whitespace
    return " ".join(word.capitalize() for word in operator_name.split()).strip()


def _calculate_transfer_operators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate transfer_from (last before) and transfer_to (first after) operators.

    Args:
        df: DataFrame with transfer operator columns

    Returns:
        DataFrame with transfer_from and transfer_to columns added
    """
    result_df = df

    # Transfer from: last before operator (coalesce from third -> second -> first)
    # Apply title case to match legacy display format
    if "third_before_operator" in df.columns:
        result_df = result_df.with_columns(
            pl.coalesce(
                [
                    "third_before_operator",
                    "second_before_operator",
                    "first_before_operator",
                ]
            )
            .map_elements(_titlecase_operator, return_dtype=pl.Utf8)
            .alias("transfer_from")
        )

    # Transfer to: first after operator
    # Apply title case to match legacy display format
    if "first_after_operator" in df.columns:
        result_df = result_df.with_columns(
            pl.col("first_after_operator")
            .map_elements(_titlecase_operator, return_dtype=pl.Utf8)
            .alias("transfer_to")
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
        col
        for col in [
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


def derive_transfer_fields(df: pl.DataFrame, skip_technology_lookup: bool = False) -> pl.DataFrame:
    """Transform transfer-related fields.

    Implements R Lines 1200-1478 logic:
    1. Assigns technology to 6 transfer legs via canonical route lookups (unless skipped)
    2. Calculates 6 technology presence flags
    3. Calculates total boardings (1 + transfer count)
    4. Calculates transfer_from and transfer_to operators
    5. Calculates first_board_tech and last_alight_tech

    Args:
        df: Input DataFrame with vehicle_tech and transfer route/operator columns
        skip_technology_lookup: If True, skip technology lookups and create null columns
            (useful for validation against legacy databases without technology fields)

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

    # Process in logical steps
    result_df = df

    if skip_technology_lookup:
        # Create null technology columns without lookups
        result_df = _create_null_transfer_technologies(result_df)
    else:
        # Perform full technology lookups (strict - will raise on unmapped routes)
        reference = ReferenceData()
        result_df = _assign_transfer_technologies(result_df, reference)

    result_df = _calculate_technology_presence_flags(result_df)
    result_df = _calculate_boardings(result_df)
    result_df = _calculate_transfer_operators(result_df)
    result_df = _calculate_first_last_technologies(result_df)

    return result_df
