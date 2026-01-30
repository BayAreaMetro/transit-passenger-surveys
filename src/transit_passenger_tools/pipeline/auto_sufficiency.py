"""Auto sufficiency transformation module.

Calculates household auto sufficiency by comparing number of vehicles
to number of workers.
"""

import polars as pl

from transit_passenger_tools.codebook import AutoRatioCategory

# Mapping from text to numeric values
VEHICLE_WORKER_MAPPING = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "four or more": 4,
}


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

    # Map vehicles - handle both string and numeric input
    if df.schema.get("vehicles") == pl.Utf8:
        df = df.with_columns(
            pl.col("vehicles")
            .str.to_lowercase()
            .replace_strict(VEHICLE_WORKER_MAPPING, default=None)
            .alias("vehicle_numeric")
        )
    else:
        df = df.with_columns(
            pl.col("vehicles").cast(pl.Int32, strict=False).alias("vehicle_numeric")
        )

    # Map workers - handle both string and numeric input
    if df.schema.get("workers") == pl.Utf8:
        df = df.with_columns(
            pl.col("workers")
            .str.to_lowercase()
            .replace_strict(VEHICLE_WORKER_MAPPING, default=None)
            .alias("worker_numeric")
        )
    else:
        df = df.with_columns(pl.col("workers").cast(pl.Int32, strict=False).alias("worker_numeric"))

    # Check for unmapped values
    unmapped_vehicles = df.filter(
        pl.col("vehicles").is_not_null() & pl.col("vehicle_numeric").is_null()
    )
    if unmapped_vehicles.height > 0:
        sample_values = unmapped_vehicles.select("vehicles").head(5)["vehicles"].to_list()
        msg = (
            f"Unrecognized vehicle values found ({unmapped_vehicles.height} records). "
            f"Sample values: {sample_values}"
        )
        raise ValueError(msg)

    unmapped_workers = df.filter(
        pl.col("workers").is_not_null() & pl.col("worker_numeric").is_null()
    )
    if unmapped_workers.height > 0:
        sample_values = unmapped_workers.select("workers").head(5)["workers"].to_list()
        msg = (
            f"Unrecognized worker values found ({unmapped_workers.height} records). "
            f"Sample values: {sample_values}"
        )
        raise ValueError(msg)

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
