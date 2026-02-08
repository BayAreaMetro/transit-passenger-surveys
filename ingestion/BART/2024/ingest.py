"""Ingest BART 2024 survey data into the Hive warehouse."""

import logging

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools import database
from transit_passenger_tools.pipeline import process_survey
from transit_passenger_tools.schemas.models import SurveyResponse

from . import preprocessing

logger = logging.getLogger(__name__)

# ============================================================================
# PROJECT-SPECIFIC CONFIGURATION
# ============================================================================

# Paths (replace M: with UNC for compatibility)
UNC_PREFIX = r"\\models.ad.mtc.ca.gov\data\models"
SURVEY_PATH = (
    f"{UNC_PREFIX}/Data/OnBoard/Data and Reports/BART/"
    "2024_StationProfileV1_NewWeights_ReducedVariables.xlsx"
)
STATION_GEOJSON = (
    f"{UNC_PREFIX}/Data/OnBoard/Data and Reports/Geography Files/"
    "cdot_ca_transit_stops_4312132402745178866.geojson"
)

# Constants
CANONICAL_OPERATOR = "BART"
VEHICLE_TECH = "Heavy Rail"
SURVEY_YEAR = 2024
FUZZY_MATCH_THRESHOLD = 80
OPERATOR_NAMES = ["BART", "Bay Area Rapid Transit"]


def validate(survey_df: pl.DataFrame, sample_size: int = 10) -> None:
    """Validate sample records against schema."""
    errors = []
    for i, row_dict in enumerate(survey_df.head(sample_size).iter_rows(named=True)):
        try:
            SurveyResponse(**row_dict)
        except ValidationError as e:
            errors.append((i, str(e)))

    if errors:
        for i, err in errors[:3]:
            logger.error("Validation error in row %d: %s", i, err)
        msg = f"Validation failed: {len(errors)} errors in {sample_size} records"
        raise ValueError(msg)

    logger.info("Validation passed for %d sample records", sample_size)


def main() -> None:
    """Main ingestion pipeline."""
    logger.info("=" * 80)
    logger.info("BART 2024 INGESTION")
    logger.info("=" * 80)

    # Preprocess
    logger.info("Preprocessing survey data...")
    survey_df = preprocessing.preprocess(
        survey_path=SURVEY_PATH,
        station_geojson=STATION_GEOJSON,
        canonical_operator=CANONICAL_OPERATOR,
        vehicle_tech=VEHICLE_TECH,
        survey_year=SURVEY_YEAR,
        fuzzy_match_threshold=FUZZY_MATCH_THRESHOLD,
        operator_names=OPERATOR_NAMES,
    )
    logger.info("Loaded %s records", f"{len(survey_df):,}")

    # Apply pipeline and create response_id
    logger.info("Applying canonical pipeline...")
    survey_df = process_survey.process_survey(
        survey_df, survey_name="BART 2024", skip_geocoding=True, skip_validation=True
    ).with_columns(
        [
            (
                pl.col("canonical_operator")
                + "_"
                + pl.col("survey_year").cast(pl.Utf8)
                + "_"
                + pl.col("ID")
            ).alias("response_id")
        ]
    )

    # Validate
    logger.info("Validating sample...")
    validate(survey_df, sample_size=10)

    # Filter to standard schema columns
    logger.info("Filtering to standard schema columns...")
    standard_columns = list(SurveyResponse.model_fields.keys())
    keep_columns = [col for col in standard_columns if col in survey_df.columns]
    logger.info(
        "Keeping %d standard columns (dropped %d vendor-specific)",
        len(keep_columns),
        len(survey_df.columns) - len(keep_columns),
    )
    survey_df_clean = survey_df.select(keep_columns)

    # Ingest surveys
    logger.info("Ingesting survey responses...")
    _output_path, version, commit_id, _data_hash = database.ingest_survey_batch(
        survey_df_clean,
        survey_year=SURVEY_YEAR,
        canonical_operator=CANONICAL_OPERATOR,
        validate=False,
    )
    logger.info(
        "Ingested %s responses (version: %s, commit: %s)",
        f"{len(survey_df_clean):,}",
        version,
        commit_id[:8],
    )

    # Ingest weights
    logger.info("Ingesting weights...")
    weights_df = survey_df.select(
        ["response_id", pl.col("weight").alias("boarding_weight"), "trip_weight"]
    ).with_columns(
        [
            pl.lit("baseline").alias("weight_scheme"),
            pl.lit("Baseline weights from BART 2024 survey").alias("description"),
        ]
    )
    database.ingest_survey_weights(weights_df, validate=False)
    logger.info("Ingested %s weight records", f"{len(weights_df):,}")

    logger.info("=" * 80)
    logger.info(
        "COMPLETE - Data: E:\\_transit_data_hive\\survey_responses\\operator=BART\\year=2024\\"
    )
    logger.info("=" * 80)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
