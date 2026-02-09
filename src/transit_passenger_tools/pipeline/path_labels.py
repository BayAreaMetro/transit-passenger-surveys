"""Path labels transformation module.

Derives path-related summary fields from technology columns:
- Route generation (operator + stations for rail surveys)
- Technology short codes (LB, EB, LR, HR, CR, FR)
- Technology usage flags (usedLB, usedEB, etc.)
- BEST_MODE (highest in hierarchy)
- TRANSFER_TYPE combinations
- Period codes (EA, AM, MD, PM, EV)
"""

import polars as pl

from transit_passenger_tools.schemas import FieldDependencies
from transit_passenger_tools.schemas.codebook import (
    DayPart,
    TechnologyType,
)

# Field dependencies
FIELD_DEPENDENCIES = FieldDependencies(
    inputs=[
        "canonical_operator",
        "onoff_enter_station",
        "onoff_exit_station",
        "route",
        "vehicle_tech",
        "first_board_tech",
        "last_alight_tech",
        "day_part",
        "first_before_technology",
        "second_before_technology",
        "third_before_technology",
        "first_after_technology",
        "second_after_technology",
        "third_after_technology",
        "access_mode",
        "egress_mode",
    ],
    outputs=[
        "route",
        "survey_mode",
        "first_board_mode",
        "last_alight_mode",
        "usedLB",
        "usedEB",
        "usedLR",
        "usedHR",
        "usedCR",
        "usedFR",
        "BEST_MODE",
        "TRANSFER_TYPE",
        "period",
        "path_access",
        "path_egress",
        "path_line_haul",
        "path_label",
    ],
)

# Delimiters for route generation (matching legacy R code)
OPERATOR_DELIMITER = "___"
ROUTE_DELIMITER = "&&&"

# Technology short codes derived from enum
TECH_SHORT_CODES = {
    tech_type.value: tech_type.to_short_code()
    for tech_type in TechnologyType
    if tech_type.to_short_code() is not None
}

# Technology hierarchy (lowest to highest priority for BEST_MODE)
TECH_HIERARCHY = [tech.to_short_code() for tech in TechnologyType.hierarchy()]

# Day part to period code mapping derived from enum
DAY_PART_TO_PERIOD = {
    day_part.value: day_part.to_period_code()
    for day_part in DayPart
    if day_part.to_period_code() is not None
}


def derive_path_labels(df: pl.DataFrame) -> pl.DataFrame:  # noqa: C901
    """Transform path-related fields.

    Creates:
    - route: Generated from operator + stations (if not already provided)
    - survey_mode: Short code for vehicle_tech
    - first_board_mode: Short code for first_board_tech
    - last_alight_mode: Short code for last_alight_tech
    - usedLB, usedEB, usedLR, usedHR, usedCR, usedFR: Usage flags
    - BEST_MODE: Highest technology in hierarchy
    - TRANSFER_TYPE: Mode combination for transfers
    - period: Time period code

    Args:
        df: Input DataFrame with technology and day_part columns

    Returns:
        DataFrame with added path label columns

    Raises:
        ValueError: If required technology columns are missing
    """
    # Validate required columns
    required = ["vehicle_tech", "first_board_tech", "last_alight_tech"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = f"path_labels transform requires columns: {missing}"
        raise ValueError(msg)

    result_df = df

    # Generate route field if not already present or if NULL
    # Pattern: {OPERATOR}___{enter_station}&&&{exit_station}
    if "route" not in result_df.columns or result_df["route"].is_null().all():
        route_cols_exist = all(
            col in result_df.columns
            for col in ["canonical_operator", "onoff_enter_station", "onoff_exit_station"]
        )

        if route_cols_exist:
            result_df = result_df.with_columns(
                pl.concat_str(
                    [
                        pl.col("canonical_operator"),
                        pl.lit(OPERATOR_DELIMITER),
                        pl.col("onoff_enter_station"),
                        pl.lit(ROUTE_DELIMITER),
                        pl.col("onoff_exit_station"),
                    ]
                ).alias("route")
            )
        elif "route" not in result_df.columns:
            # Add NULL route column if we can't generate it
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("route"))

    # Map technology names to short codes using when/then chains for null safety
    def map_tech_to_short(col_name: str) -> pl.Expr:
        """Map technology long name to short code, preserving nulls."""
        expr = pl.lit(None).cast(pl.Utf8)
        for long_name, short_code in TECH_SHORT_CODES.items():
            expr = pl.when(pl.col(col_name) == long_name).then(pl.lit(short_code)).otherwise(expr)
        return expr

    result_df = result_df.with_columns(
        map_tech_to_short("vehicle_tech").alias("survey_mode"),
        map_tech_to_short("first_board_tech").alias("first_board_mode"),
        map_tech_to_short("last_alight_tech").alias("last_alight_mode"),
    )

    # Map transfer_from/to operators to tech (if transfer_from_tech doesn't exist)
    # This would require an operator→tech crosswalk which we'll handle separately

    # Calculate technology usage flags
    # A technology is "used" if it appears in any of: first_board, survey, last_alight
    mode_cols = ["first_board_mode", "survey_mode", "last_alight_mode"]

    for tech_code in TECH_SHORT_CODES.values():
        flag_col = f"used{tech_code}"
        # Check if tech appears in any mode column
        conditions = [pl.col(c) == tech_code for c in mode_cols]
        combined = conditions[0]
        for cond in conditions[1:]:
            combined = combined | cond
        false_value = pl.lit(0).cast(pl.Boolean)
        result_df = result_df.with_columns(
            combined.fill_null(false_value).cast(pl.Int8).alias(flag_col)
        )

    # Calculate total technologies used
    used_cols = [f"used{code}" for code in TECH_SHORT_CODES.values()]
    result_df = result_df.with_columns(pl.sum_horizontal(used_cols).alias("usedTotal"))

    # Calculate BEST_MODE using hierarchy (CR > HR > LR > FR > EB > LB)
    # Start with LB as default, override with higher priority modes
    best_mode_expr = pl.lit("LB")
    for tech_code in TECH_HIERARCHY[1:]:  # Skip LB (default)
        flag_col = f"used{tech_code}"
        best_mode_expr = (
            pl.when(pl.col(flag_col) == 1).then(pl.lit(tech_code)).otherwise(best_mode_expr)
        )
    result_df = result_df.with_columns(best_mode_expr.alias("BEST_MODE"))

    # Calculate number of transfers (boardings - 1)
    if "boardings" in result_df.columns:
        result_df = result_df.with_columns((pl.col("boardings") - 1).alias("nTransfers"))
    else:
        result_df = result_df.with_columns(pl.lit(0).alias("nTransfers"))

    # Calculate TRANSFER_TYPE
    # NO_TRANSFERS if nTransfers == 0
    # Otherwise combination of technologies used (e.g., LB_HR, EB_CR)
    result_df = _calculate_transfer_type(result_df)

    # Map day_part to period code using when/then for null safety
    if "day_part" in result_df.columns:
        period_expr = pl.lit(None).cast(pl.Utf8)
        for day_part, period in DAY_PART_TO_PERIOD.items():
            period_expr = (
                pl.when(pl.col("day_part") == day_part).then(pl.lit(period)).otherwise(period_expr)
            )
        result_df = result_df.with_columns(period_expr.alias("period"))

    return result_df


def _calculate_transfer_type(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate TRANSFER_TYPE field based on technologies used.

    Logic:
    - NO_TRANSFERS if nTransfers == 0
    - XX_YY where XX and YY are the two most prominent technologies used
    - Uses hierarchy to determine naming order
    """
    # Start with OTHER as default
    transfer_type = pl.lit("OTHER")

    # NO_TRANSFERS case
    transfer_type = (
        pl.when(pl.col("nTransfers") == 0).then(pl.lit("NO_TRANSFERS")).otherwise(transfer_type)
    )

    # Build transfer type combinations
    # Order matters - lower tech listed first (e.g., LB_HR not HR_LB)
    transfer_combos = [
        ("LB", "EB"),
        ("LB", "FB"),
        ("LB", "LR"),
        ("LB", "HR"),
        ("LB", "CR"),
        ("EB", "FB"),
        ("EB", "LR"),
        ("EB", "HR"),
        ("EB", "CR"),
        ("FB", "LR"),
        ("FB", "HR"),
        ("FB", "CR"),
        ("LR", "HR"),
        ("LR", "CR"),
        ("HR", "CR"),
    ]

    for tech1, tech2 in transfer_combos:
        combo_name = f"{tech1}_{tech2}"
        flag1 = f"used{tech1}"
        flag2 = f"used{tech2}"

        transfer_type = (
            pl.when((pl.col("nTransfers") > 0) & (pl.col(flag1) == 1) & (pl.col(flag2) == 1))
            .then(pl.lit(combo_name))
            .otherwise(transfer_type)
        )

    # Same-mode transfers (e.g., LB_LB when only LB used but has transfers)
    for tech_code in TECH_SHORT_CODES.values():
        same_combo = f"{tech_code}_{tech_code}"
        flag_col = f"used{tech_code}"

        transfer_type = (
            pl.when(
                (pl.col("nTransfers") > 0) & (pl.col(flag_col) == 1) & (pl.col("usedTotal") == 1)
            )
            .then(pl.lit(same_combo))
            .otherwise(transfer_type)
        )

    return df.with_columns(transfer_type.alias("TRANSFER_TYPE"))
