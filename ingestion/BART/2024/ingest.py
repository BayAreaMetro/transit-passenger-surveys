"""Ingest BART 2024 survey data into the data lake."""

import polars as pl
import preprocessing
from pydantic import ValidationError

from transit_passenger_tools import database
from transit_passenger_tools.pipeline import process_survey
from transit_passenger_tools.schemas.models import SurveyResponse

# ============================================================================
# PROJECT-SPECIFIC CONFIGURATION
# ============================================================================

# Paths
SURVEY_PATH = (
    r"M:/Data/OnBoard/Data and Reports/BART/"
    r"2024_StationProfileV1_NewWeights_ReducedVariables.xlsx"
)

STATION_GEOJSON = (
    r"M:/Data/OnBoard/Data and Reports/Geography Files/"
    r"cdot_ca_transit_stops_4312132402745178866.geojson"
)

OUTPUT_DIR = r"M:/Data/OnBoard/Data and Reports/BART"

# Replace letter drives with UNC paths for compatibility
net_paths = {
    "M:": r"\\models.ad.mtc.ca.gov\data\models"
}

for drive, unc_path in net_paths.items():
    SURVEY_PATH = SURVEY_PATH.replace(drive, unc_path)
    STATION_GEOJSON = STATION_GEOJSON.replace(drive, unc_path)
    OUTPUT_DIR = OUTPUT_DIR.replace(drive, unc_path)

# Constants
CANONICAL_OPERATOR = "BART"
VEHICLE_TECH = "Heavy Rail"  # Must match TechnologyType enum
SURVEY_YEAR = 2024
FUZZY_MATCH_THRESHOLD = 80

# Station geocoding configuration
OPERATOR_NAMES = ["BART", "Bay Area Rapid Transit"]


def validate(survey_df: pl.DataFrame, sample_size: int = 10) -> None:
    """Quick validation of sample records."""
    print(f"\nValidating sample of {sample_size} records...")
    errors = []
    for i, row_dict in enumerate(survey_df.head(sample_size).iter_rows(named=True)):
        try:
            SurveyResponse(**row_dict)
        except ValidationError as e:
            errors.append((i, str(e)))

    if errors:
        print(f"\nValidation errors found in {len(errors)} records:")
        for i, err in errors[:3]:
            print(f"  Row {i}: {err}")
        msg = f"Validation failed: {len(errors)} errors"
        raise ValueError(msg)
    print("Validation passed!")


def main() -> None:
    """Main ingestion pipeline."""
    print("=" * 80)
    print("BART 2024 INGESTION")
    print("=" * 80)

    # Preprocess
    print("\n1. Preprocessing...")
    survey_df = preprocessing.preprocess(
        survey_path=SURVEY_PATH,
        station_geojson=STATION_GEOJSON,
        canonical_operator=CANONICAL_OPERATOR,
        vehicle_tech=VEHICLE_TECH,
        survey_year=SURVEY_YEAR,
        fuzzy_match_threshold=FUZZY_MATCH_THRESHOLD,
        operator_names=OPERATOR_NAMES,
    )
    print(f"   {len(survey_df):,} records")

    # Apply pipeline
    print("\n2. Applying canonical pipeline...")
    survey_df = process_survey.process_survey(
        survey_df,
        survey_name="BART 2024",
        skip_geocoding=True,
        skip_validation=True
    )

    # Create response_id
    print("\n3. Creating response_id...")
    survey_df = survey_df.with_columns([
        (
            pl.col("canonical_operator") + "_" +
            pl.col("survey_year").cast(pl.Utf8) + "_" +
            pl.col("ID")
        ).alias("response_id")
    ])

    # Validate
    print("\n4. Validating...")
    validate(survey_df, sample_size=10)

    # Filter to standard schema columns only (remove vendor-specific raw columns)
    print("\n5. Filtering to standard schema columns...")
    standard_columns = list(SurveyResponse.model_fields.keys())
    # Keep only columns that exist in both the schema and the dataframe
    keep_columns = [col for col in standard_columns if col in survey_df.columns]
    missing_columns = [col for col in standard_columns if col not in survey_df.columns]
    extra_columns = [col for col in survey_df.columns if col not in standard_columns]

    print(f"   Standard columns present: {len(keep_columns)}")
    print(f"   Missing from data: {len(missing_columns)}")
    print(f"   Extra vendor columns dropped: {len(extra_columns)}")

    survey_df_clean = survey_df.select(keep_columns)

    # Ingest surveys
    print("\n6. Ingesting survey responses...")
    _output_path, version, commit_id, _data_hash = database.ingest_survey_batch(
        survey_df_clean,
        survey_year=SURVEY_YEAR,
        canonical_operator=CANONICAL_OPERATOR,
        validate=False
    )
    print(f"   {len(survey_df_clean):,} responses ingested")
    print(f"   Version: {version}, Commit: {commit_id[:8]}")

    # Ingest weights
    print("\n7. Ingesting weights...")
    weights_df = survey_df.select([
        pl.col("response_id"),
        pl.col("weight").alias("boarding_weight"),
        "trip_weight",
    ]).with_columns([
        pl.lit("baseline").alias("weight_scheme"),
        pl.lit("Baseline weights from BART 2024 survey").alias("description"),
    ])
    database.ingest_survey_weights(weights_df, validate=False)
    print(f"   {len(weights_df):,} weight records ingested")

    print("\n" + "=" * 80)
    print("COMPLETE")
    print("Data location: E:\\_data_lake\\survey_responses\\operator=BART\\year=2024\\")
    print("=" * 80)


if __name__ == "__main__":
    main()
