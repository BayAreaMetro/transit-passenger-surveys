"""Survey processing pipeline orchestrator.

This module coordinates the execution of all transformation modules
to process raw survey data into standardized format.

ARCHITECTURE:
- Preprocessing scripts (one-off, vendor-specific) transform data → CoreSurveyResponse format
- Pipeline validates core fields exist, then runs derivation transforms
- Each transform expects required columns and fails fast if missing
- Output includes all CoreSurveyResponse + DerivedSurveyResponse fields
"""

import logging
from pathlib import Path

import polars as pl

from transit_passenger_tools.pipeline import (
    auto_sufficiency,
    date_time,
    demographics,
    geocoding,
    path_labels,
    tour_purpose,
    transfers,
    validate,
)

logger = logging.getLogger(__name__)

# Pipeline execution order (dependencies must come before dependents)
# Each tuple: (module_name, transform_function, kwargs_needed)
PIPELINE_MODULES = [
    ("date_time", date_time.derive_temporal_fields, False),
    ("auto_sufficiency", auto_sufficiency.derive_auto_sufficiency, False),
    ("demographics", demographics.derive_demographics, False),
    ("transfers", transfers.derive_transfer_fields, False),
    ("path_labels", path_labels.derive_path_labels, False),
    ("tour_purpose", tour_purpose.derive_tour_purpose, False),
    ("geocoding", geocoding.assign_zones_and_distances, True),
]


def process_survey(
    df: pl.DataFrame,
    survey_name: str | None = None,
    skip_geocoding: bool = False,
    skip_validation: bool = False,
    shapefiles_dir: Path | None = None,
) -> pl.DataFrame:
    """Process a preprocessed survey DataFrame through the derivation pipeline.

    Validates input meets CoreSurveyResponse contract, then executes
    transformation modules in the correct order to derive DerivedSurveyResponse fields.

    Pipeline order:
    1. date_time: Derive weekpart (from day_of_the_week), day_part (from survey_time)
    2. income: Derive income bounds from household_income enum
    3. demographics_derived: Derive year_born_four_digit from age + survey_year
    4. auto_sufficiency: Normalize vehicle/worker counts, derive autos_vs_workers
    5. demographics: Process race/ethnicity fields
    6. transfers: Calculate boardings, technology assignments
    7. path_labels: Derive mode codes, usage flags, BEST_MODE, TRANSFER_TYPE
    8. tour_purpose: Derive tour purposes from origin/destination activities
    9. geocoding: Calculate distances (optional, requires coordinates)

    Args:
        df: Input DataFrame with preprocessed survey data (CoreSurveyResponse format)
        survey_name: Optional survey identifier for logging/filtering
        skip_geocoding: If True, skip the geocoding module (useful when
            coordinate columns are not present)
        skip_validation: If True, skip input validation (not recommended)
        shapefiles_dir: Directory containing zone shapefiles for geocoding (optional)

    Returns:
        DataFrame with all derived columns added

    Raises:
        ValueError: If validation fails (required columns missing, invalid enum values)

    Example:
        >>> preprocessed_df = pl.read_parquet("surveys/bart_2024_preprocessed.parquet")
        >>> processed = process_survey(preprocessed_df, survey_name="BART 2024")
    """
    # Log processing start
    if survey_name:
        logger.info("Processing survey: %s", survey_name)

    # Build list of transforms to skip for validation
    skip_transforms = []
    if skip_geocoding:
        skip_transforms.append("geocoding")

    # Validate input meets CoreSurveyResponse contract
    if not skip_validation:
        validate.validate_preprocessed_input(df, skip_transforms=skip_transforms)

    result_df = df

    # Execute pipeline modules in order
    for module_name, transform_func, needs_shapefiles in PIPELINE_MODULES:
        # Skip geocoding if requested
        if module_name == "geocoding" and skip_geocoding:
            continue

        # Call the transform function with appropriate arguments
        kwargs = {"shapefiles_dir": shapefiles_dir} if needs_shapefiles else {}
        result_df = transform_func(result_df, **kwargs)

    return result_df


def process_survey_file(
    input_path: str | Path,
    output_path: str | Path | None = None,
    survey_name: str | None = None,
    skip_geocoding: bool = False,
    skip_validation: bool = False,
    shapefiles_dir: Path | None = None,
) -> pl.DataFrame:
    """Process a survey file through the standardization pipeline.

    Reads a CSV or Parquet file, validates it meets CoreSurveyResponse contract,
    processes it through all transformation modules, and optionally writes the result.

    Args:
        input_path: Path to input CSV or Parquet file
        output_path: Optional path to write output. If None, result is
            only returned (not written). Format determined by extension.
        survey_name: Optional survey identifier
        skip_geocoding: If True, skip the geocoding module
        skip_validation: If True, skip input validation (not recommended)
        shapefiles_dir: Directory containing zone shapefiles for geocoding (optional)

    Returns:
        Processed DataFrame

    Raises:
        ValueError: If input file format is not supported or validation fails

    Example:
        >>> result = process_survey_file(
        ...     "data/preprocessed/bart_2024.parquet",
        ...     "data/processed/bart_2024.parquet"
        ... )
    """
    input_path = Path(input_path)

    # Read input file
    if input_path.suffix.lower() == ".csv":
        df = pl.read_csv(input_path)
    elif input_path.suffix.lower() in (".parquet", ".pq"):
        df = pl.read_parquet(input_path)
    else:
        msg = f"Unsupported input format: {input_path.suffix}"
        raise ValueError(msg)

    # Process through pipeline
    result_df = process_survey(
        df,
        survey_name=survey_name,
        skip_geocoding=skip_geocoding,
        skip_validation=skip_validation,
        shapefiles_dir=shapefiles_dir,
    )

    # Write output if path provided
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix.lower() == ".csv":
            result_df.write_csv(output_path)
        elif output_path.suffix.lower() in (".parquet", ".pq"):
            result_df.write_parquet(output_path)
        else:
            msg = f"Unsupported output format: {output_path.suffix}"
            raise ValueError(msg)

    return result_df
