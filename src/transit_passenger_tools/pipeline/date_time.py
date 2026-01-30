"""Date and time transformation module.

Derives temporal categorizations from Core fields:
- weekpart: WEEKDAY or WEEKEND (from day_of_the_week)
- day_part: Time period (EARLY AM, AM PEAK, MIDDAY, PM PEAK, EVENING) (from survey_time)
"""

import polars as pl

from transit_passenger_tools.schemas.codebook import (
    DayOfWeek,
    DayPart,
    Weekpart,
)

# Weekend days
WEEKEND_DAYS = {DayOfWeek.SATURDAY.value, DayOfWeek.SUNDAY.value}


def derive_temporal_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Transform date and time fields.

    Derives weekpart from day_of_the_week and day_part from survey_time.

    Args:
        df: Input DataFrame with day_of_the_week and survey_time columns

    Returns:
        DataFrame with added columns: weekpart, day_part

    Raises:
        ValueError: If required columns missing or time formats cannot be parsed
    """
    # Validate required columns
    required = ["day_of_the_week"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = f"date_time transform requires columns: {missing}"
        raise ValueError(msg)

    # Derive weekpart from day_of_the_week
    df = df.with_columns(
        [
            pl.when(pl.col("day_of_the_week").is_null())
            .then(pl.lit(None).cast(pl.Utf8))
            .when(pl.col("day_of_the_week").is_in(list(WEEKEND_DAYS)))
            .then(pl.lit(Weekpart.WEEKEND.value))
            .otherwise(pl.lit(Weekpart.WEEKDAY.value))
            .alias("weekpart")
        ]
    )

    # Parse survey_time to extract hour for day_part calculation
    if "survey_time" in df.columns:
        # Try to parse time string to extract hour
        df = df.with_columns(
            [
                pl.when(pl.col("survey_time").is_not_null())
                .then(
                    pl.coalesce(
                        [
                            # Try HH:MM:SS format
                            pl.col("survey_time").str.strptime(pl.Time, "%H:%M:%S", strict=False),
                            # Try HH:MM format
                            pl.col("survey_time").str.strptime(pl.Time, "%H:%M", strict=False),
                            # Try h:MM:SS AM/PM format
                            pl.col("survey_time").str.strptime(
                                pl.Time, "%I:%M:%S %p", strict=False
                            ),
                            # Try h:MM AM/PM format
                            pl.col("survey_time").str.strptime(pl.Time, "%I:%M %p", strict=False),
                        ]
                    )
                )
                .alias("_parsed_time")
            ]
        )

        # Extract hour from parsed time
        df = df.with_columns(
            [
                pl.when(pl.col("_parsed_time").is_not_null())
                .then(pl.col("_parsed_time").dt.hour())
                .alias("_survey_hour")
            ]
        )
    else:
        # No survey_time column - day_part will be null
        df = df.with_columns([pl.lit(None).cast(pl.Int32).alias("_survey_hour")])

    # Derive day_part from hour using configured time ranges
    day_part_expr = pl.lit(None).cast(pl.Utf8)

    # Build conditional expression for each configured day part
    for day_part in DayPart:
        time_range = day_part.time_range()
        if time_range is None:
            continue
        start_hour, end_hour = time_range
        day_part_expr = (
            pl.when(pl.col("_survey_hour").is_between(start_hour, end_hour, closed="both"))
            .then(pl.lit(day_part.value))
            .otherwise(day_part_expr)
        )

    # Default to EVENING for any hour not in configured ranges
    day_part_expr = (
        pl.when(pl.col("_survey_hour").is_not_null())
        .then(pl.coalesce([day_part_expr, pl.lit(DayPart.EVENING.value)]))
        .otherwise(pl.lit(None).cast(pl.Utf8))
    )

    df = df.with_columns([day_part_expr.alias("day_part")])

    # Drop temporary columns
    temp_cols = ["_parsed_time", "_survey_hour"]
    df = df.drop([c for c in temp_cols if c in df.columns])

    return df
