"""Transfer and technology processing module.

Assigns technology to transfer legs and calculates technology presence flags.
"""
import logging

import polars as pl

from transit_passenger_tools.reference import ReferenceData

logger = logging.getLogger(__name__)

# Transfer leg columns (before and after surveyed vehicle)
TRANSFER_LEGS = [
    "first_before",
    "second_before",
    "third_before",
    "first_after",
    "second_after",
    "third_after"
]

# Technology types
TECHNOLOGIES = [
    "local bus",
    "express bus",
    "light rail",
    "heavy rail",
    "commuter rail",
    "ferry"
]


def assign_transfer_technologies(
    df: pl.DataFrame,
    survey_name: str,
    survey_year: int,
    reference: ReferenceData | None = None
) -> pl.DataFrame:
    """Assign technology to each transfer leg based on route names.

    Uses canonical route crosswalk to map route names to technologies.

    Args:
        df: DataFrame with transfer route columns
        survey_name: Name of survey
        survey_year: Year of survey
        reference: ReferenceData instance (creates new if None)

    Returns:
        DataFrame with added technology columns for each transfer leg
    """
    logger.info("Assigning transfer technologies")

    if reference is None:
        reference = ReferenceData()

    result_df = df.clone()

    # For each transfer leg, assign technology based on route
    for leg in TRANSFER_LEGS:
        route_col = f"{leg}_route"
        operator_col = f"{leg}_operator"
        tech_col = f"{leg}_technology"

        # Check if route column exists
        if route_col not in df.columns:
            logger.debug(f"Column {route_col} not found, skipping")
            continue

        # Get unique routes to look up
        unique_routes = df.select(route_col).unique().to_series().to_list()
        unique_routes = [r for r in unique_routes if r is not None]

        if not unique_routes:
            logger.debug(f"No routes found in {route_col}")
            result_df = result_df.with_columns([
                pl.lit(None).cast(pl.Utf8).alias(tech_col)
            ])
            continue

        # Build lookup dictionary from crosswalk
        tech_lookup = {}
        for route in unique_routes:
            tech = reference.get_route_technology(survey_name, survey_year, route)
            if tech:
                tech_lookup[route] = tech

        logger.debug(f"Found technology for {len(tech_lookup)}/{len(unique_routes)} routes in {route_col}")

        # Create technology column via mapping
        result_df = result_df.with_columns([
            pl.col(route_col).replace(tech_lookup, default=None).alias(tech_col)
        ])

        # Log any unmapped routes
        unmapped = result_df.filter(
            pl.col(route_col).is_not_null() & pl.col(tech_col).is_null()
        ).select(route_col).unique()

        if unmapped.height > 0:
            logger.warning(f"Unmapped routes in {route_col}: {unmapped.to_series().to_list()}")

    return result_df


def calculate_technology_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate boolean flags for presence of each technology type in the trip.

    Checks survey_tech and all 6 transfer technology columns.

    Args:
        df: DataFrame with survey_tech and transfer technology columns

    Returns:
        DataFrame with added technology presence flags
    """
    logger.info("Calculating technology presence flags")

    result_df = df.clone()

    # Build list of technology columns to check
    tech_cols = ["survey_tech"]
    for leg in TRANSFER_LEGS:
        tech_col = f"{leg}_technology"
        if tech_col in df.columns:
            tech_cols.append(tech_col)

    # For each technology type, create a presence flag
    for tech in TECHNOLOGIES:
        flag_col = f"{tech.replace(' ', '_')}_present"

        # Check if technology appears in any of the columns
        conditions = [pl.col(col) == tech for col in tech_cols if col in result_df.columns]

        if conditions:
            # Any of the conditions being true means technology is present
            tech_present = conditions[0]
            for condition in conditions[1:]:
                tech_present = tech_present | condition

            result_df = result_df.with_columns([
                tech_present.alias(flag_col)
            ])
        else:
            # No columns to check, set to False
            result_df = result_df.with_columns([
                pl.lit(False).alias(flag_col)
            ])

    # Log technology distribution
    for tech in TECHNOLOGIES:
        flag_col = f"{tech.replace(' ', '_')}_present"
        if flag_col in result_df.columns:
            count = result_df.filter(pl.col(flag_col) == True).height
            pct = count / result_df.height * 100
            logger.info(f"{tech}: {count:,} trips ({pct:.1f}%)")

    return result_df


def calculate_boardings(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate total number of boardings on the trip.

    Uses transfer counts if available, otherwise sums technology columns.

    Args:
        df: DataFrame with transfer information

    Returns:
        DataFrame with added boardings column
    """
    logger.info("Calculating boardings")

    # Check if we have number_transfers columns
    has_orig_transfers = "number_transfers_orig_board" in df.columns
    has_dest_transfers = "number_transfers_alight_dest" in df.columns

    if has_orig_transfers and has_dest_transfers:
        # Calculate boardings from transfer counts
        # Total boardings = 1 (surveyed vehicle) + transfers before + transfers after
        result_df = df.with_columns([
            (
                pl.lit(1) +
                pl.col("number_transfers_orig_board").fill_null(0) +
                pl.col("number_transfers_alight_dest").fill_null(0)
            ).alias("boardings")
        ])
    else:
        # Calculate from technology columns
        # Count non-null transfer technologies + 1 for surveyed vehicle
        tech_cols = [f"{leg}_technology" for leg in TRANSFER_LEGS]
        existing_cols = [col for col in tech_cols if col in df.columns]

        if existing_cols:
            # Sum non-null technologies + 1
            boardings_expr = pl.lit(1)
            for col in existing_cols:
                boardings_expr = boardings_expr + pl.col(col).is_not_null().cast(pl.Int64)

            result_df = df.with_columns([
                boardings_expr.alias("boardings")
            ])
        else:
            # No transfer info, assume single boarding
            result_df = df.with_columns([
                pl.lit(1).alias("boardings")
            ])

    # Log boardings distribution
    boardings_dist = result_df.group_by("boardings").agg(pl.count()).sort("boardings")
    logger.info(f"Boardings distribution:\n{boardings_dist}")

    return result_df


def generate_transfer_validation(
    df: pl.DataFrame,
    survey_name: str,
    survey_year: int
) -> pl.DataFrame:
    """Generate validation dataframe for transfer assignments.

    Identifies records with transfer routes that couldn't be matched to
    operators or technologies (equivalent to R's check_transfers.csv).

    Args:
        df: Processed DataFrame
        survey_name: Name of survey
        survey_year: Year of survey

    Returns:
        DataFrame with problematic transfer records
    """
    logger.info("Generating transfer validation report")

    problems = []

    # Check each transfer leg for missing technology
    for leg in TRANSFER_LEGS:
        route_col = f"{leg}_route"
        tech_col = f"{leg}_technology"

        if route_col not in df.columns or tech_col not in df.columns:
            continue

        # Find records with route but no technology
        missing_tech = df.filter(
            pl.col(route_col).is_not_null() & pl.col(tech_col).is_null()
        ).select([
            pl.lit(survey_name).alias("survey_name"),
            pl.lit(survey_year).alias("survey_year"),
            pl.lit(leg).alias("transfer_leg"),
            pl.col("ID"),
            pl.col(route_col).alias("route"),
            pl.col(tech_col).alias("technology"),
        ])

        if missing_tech.height > 0:
            problems.append(missing_tech)

    if problems:
        validation_df = pl.concat(problems)
        logger.warning(f"Found {validation_df.height} transfer assignment problems")
        return validation_df
    logger.info("No transfer assignment problems found")
    return pl.DataFrame()


def process_transfers(
    df: pl.DataFrame,
    survey_name: str,
    survey_year: int,
    reference: ReferenceData | None = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Process all transfer-related fields.

    Args:
        df: Input DataFrame
        survey_name: Name of survey
        survey_year: Year of survey
        reference: ReferenceData instance (creates new if None)

    Returns:
        Tuple of (processed DataFrame, validation DataFrame)
    """
    logger.info("Processing transfers")

    result_df = df
    result_df = assign_transfer_technologies(result_df, survey_name, survey_year, reference)
    result_df = calculate_technology_flags(result_df)
    result_df = calculate_boardings(result_df)

    validation_df = generate_transfer_validation(result_df, survey_name, survey_year)

    logger.info("Transfer processing complete")
    return result_df, validation_df
