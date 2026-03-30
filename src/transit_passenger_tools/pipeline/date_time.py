"""Date and time transformation module.

Standardizes canonical temporal fields:
- day_of_week: Day of week supplied by preprocessing
- time_period: Time period (EARLY AM, AM PEAK, MIDDAY, PM PEAK, EVENING)
  preserved from source when available, otherwise derived from survey_time
"""

import polars as pl

from transit_passenger_tools.codebook import DayPart
from transit_passenger_tools.models import FieldDependencies

# Field dependencies
FIELD_DEPENDENCIES = FieldDependencies(
    inputs=["day_of_week", "survey_time"],
    outputs=["time_period"],
    optional_inputs=["day_of_the_week", "day_part", "time_period"],
)


def derive_temporal_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Transform date and time fields.

    Standardizes day_of_week and time_period.

    Args:
        df: Input DataFrame with day_of_week/day_of_the_week and survey_time columns

    Returns:
        DataFrame with standardized day_of_week and time_period columns

    Raises:
        ValueError: If required columns missing or time formats cannot be parsed
    """
    if "day_of_week" not in df.columns and "day_of_the_week" in df.columns:
        df = df.rename({"day_of_the_week": "day_of_week"})

    if "day_of_week" not in df.columns:
        msg = "date_time transform requires columns: ['day_of_week']"
        raise ValueError(msg)

    if "day_part" in df.columns:
        if "time_period" in df.columns:
            df = df.with_columns(
                pl.coalesce([pl.col("time_period"), pl.col("day_part")]).alias("time_period")
            )
        else:
            df = df.with_columns(pl.col("day_part").alias("time_period"))

    # Parse survey_time to extract hour for time_period calculation.
    if "survey_time" in df.columns:
        dtype = df.schema["survey_time"]

        if isinstance(dtype, pl.Datetime):
            # Already a proper Datetime — extract hour directly
            df = df.with_columns(
                pl.col("survey_time").dt.hour().alias("_survey_hour")
            )
        elif dtype == pl.Time:
            df = df.with_columns(
                pl.col("survey_time").dt.hour().alias("_survey_hour")
            )
        else:
            # String — try common time formats
            df = df.with_columns(
                [
                    pl.when(pl.col("survey_time").is_not_null())
                    .then(
                        pl.coalesce(
                            [
                                pl.col("survey_time").str.strptime(
                                    pl.Time, "%H:%M:%S", strict=False
                                ),
                                pl.col("survey_time").str.strptime(
                                    pl.Time, "%H:%M", strict=False
                                ),
                                pl.col("survey_time").str.strptime(
                                    pl.Time, "%I:%M:%S %p", strict=False
                                ),
                                pl.col("survey_time").str.strptime(
                                    pl.Time, "%I:%M %p", strict=False
                                ),
                            ]
                        )
                    )
                    .alias("_parsed_time")
                ]
            )
            df = df.with_columns(
                pl.col("_parsed_time").dt.hour().alias("_survey_hour")
            )
    else:
        # No survey_time column - derived time_period will be null.
        df = df.with_columns([pl.lit(None).cast(pl.Int32).alias("_survey_hour")])

    # Derive time_period from hour using configured time ranges.
    time_period_expr = pl.lit(None).cast(pl.Utf8)

    # Build conditional expression for each configured time period.
    for day_part in DayPart:
        time_range = day_part.time_range()
        if time_range is None:
            continue
        start_hour, end_hour = time_range
        time_period_expr = (
            pl.when(pl.col("_survey_hour").is_between(start_hour, end_hour, closed="both"))
            .then(pl.lit(day_part.value))
            .otherwise(time_period_expr)
        )

    # Default to EVENING for any hour not in configured ranges.
    time_period_expr = (
        pl.when(pl.col("_survey_hour").is_not_null())
        .then(pl.coalesce([time_period_expr, pl.lit(DayPart.EVENING.value)]))
        .otherwise(pl.lit(None).cast(pl.Utf8))
    )

    df = df.with_columns(
        pl.coalesce([pl.col("time_period"), time_period_expr]).alias("time_period")
        if "time_period" in df.columns
        else time_period_expr.alias("time_period")
    )

    # Drop temporary columns
    temp_cols = ["_parsed_time", "_survey_hour"]
    df = df.drop([c for c in temp_cols if c in df.columns])

    return df
