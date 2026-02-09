"""Auto sufficiency transformation module.

Calculates household auto sufficiency by comparing number of vehicles
to number of workers.
"""

import polars as pl

from transit_passenger_tools.schemas import FieldDependencies
from transit_passenger_tools.schemas.codebook import AutoRatioCategory

# Field dependencies
FIELD_DEPENDENCIES = FieldDependencies(
    inputs=["vehicles", "workers"],
    outputs=["vehicle_numeric", "worker_numeric", "auto_to_workers_ratio"],
)

# Mapping from text to numeric values
VEHICLE_WORKER_MAPPING = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "four or more": 4,
}


def _map_to_numeric(df: pl.DataFrame, col: str, output_col: str) -> pl.DataFrame:
    """Map vehicle/worker text to numeric, handling both string and numeric input.

    Args:
        df: Input DataFrame
        col: Source column name
        output_col: Output column name

    Returns:
        DataFrame with mapped numeric column added
    """
    if df.schema.get(col) == pl.Utf8:
        return df.with_columns(
            pl.col(col)
            .str.to_lowercase()
            .replace_strict(VEHICLE_WORKER_MAPPING, default=None)
            .alias(output_col)
        )
    return df.with_columns(pl.col(col).cast(pl.Int32, strict=False).alias(output_col))


def _check_unmapped_values(df: pl.DataFrame, source_col: str, mapped_col: str, label: str) -> None:
    """Check for unmapped values and raise error with samples if found.

    Args:
        df: DataFrame to check
        source_col: Original column name
        mapped_col: Mapped numeric column name
        label: Label for error message (e.g., 'vehicle', 'worker')

    Raises:
        ValueError: If unmapped values found
    """
    unmapped = df.filter(pl.col(source_col).is_not_null() & pl.col(mapped_col).is_null())
    if unmapped.height > 0:
        sample_values = unmapped.select(source_col).head(5)[source_col].to_list()
        msg = (
            f"Unrecognized {label} values found ({unmapped.height} records). "
            f"Sample values: {sample_values}"
        )
        raise ValueError(msg)


def derive_auto_sufficiency(df: pl.DataFrame) -> pl.DataFrame:
    """Transform vehicles and workers to calculate vehicle-to-worker ratio category.

    Maps text values ("one", "two", etc.) to numeric, then calculates
    auto_to_workers_ratio category:
    - "Zero autos" if vehicles = 0
    - "Autos < workers" if workers > vehicles > 0
    - "Autos >= workers" if 0 < workers <= vehicles

    Args:
        df: Input DataFrame with vehicles and workers columns

    Returns:
        DataFrame with added columns: vehicle_numeric, worker_numeric, auto_to_workers_ratio

    Raises:
        ValueError: If vehicles or workers columns are missing
    """
    # Validate required columns
    required = ["vehicles", "workers"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = f"auto_sufficiency transform requires columns: {missing}"
        raise ValueError(msg)

    # Map vehicles and workers to numeric
    df = _map_to_numeric(df, "vehicles", "vehicle_numeric")
    df = _map_to_numeric(df, "workers", "worker_numeric")

    # Check for unmapped values
    _check_unmapped_values(df, "vehicles", "vehicle_numeric", "vehicle")
    _check_unmapped_values(df, "workers", "worker_numeric", "worker")

    # Calculate auto_to_workers_ratio category
    df = df.with_columns(
        [
            pl.when(pl.col("vehicle_numeric") == 0)
            .then(pl.lit(AutoRatioCategory.ZERO_AUTOS.value))
            .when(
                (pl.col("worker_numeric") > pl.col("vehicle_numeric"))
                & (pl.col("vehicle_numeric") > 0)
            )
            .then(pl.lit(AutoRatioCategory.WORKERS_GT_AUTOS.value))
            .when(
                (pl.col("worker_numeric") <= pl.col("vehicle_numeric"))
                & (pl.col("worker_numeric") > 0)
            )
            .then(pl.lit(AutoRatioCategory.WORKERS_LE_AUTOS.value))
            .alias("auto_to_workers_ratio")
        ]
    )

    return df
