"""Prepare BART 2024 survey data for ingestion into the Parquet data warehouse."""

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

try:
    from . import preprocessing
except ImportError:
    import preprocessing  # type: ignore[no-redef]  # fallback for direct execution

from transit_passenger_tools.database import _validate_rows, validate_referential_integrity
from transit_passenger_tools.models import SurveyResponse
from transit_passenger_tools.pipeline import process_survey

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


def main() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Prepare BART 2024 data and return (responses, weights, metadata).

    Returns:
        A 3-tuple of ``(responses_df, weights_df, metadata_df)`` ready for
        :func:`database.ingest`.
    """
    logger.info("=" * 80)
    logger.info("BART 2024 INGESTION")
    logger.info("=" * 80)

    # ===========================
    # PREPROCESSING
    # ===========================
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

    # ==========================
    # IDENTIFIERS (before pipeline validation)
    # ==========================
    survey_id = f"{CANONICAL_OPERATOR}_{SURVEY_YEAR}"
    survey_df = survey_df.with_columns(
        [
            (
                pl.col("canonical_operator")
                + "_"
                + pl.col("survey_year").cast(pl.Utf8)
                + "_"
                + pl.col("ID")
            ).alias("response_id"),
            pl.lit(survey_id).alias("survey_id"),
        ]
    )

    # ==========================
    # CANONICAL PIPELINE
    # ==========================
    logger.info("Applying canonical pipeline...")
    survey_df = process_survey.process_survey(
        survey_df,
        survey_name="BART 2024",
        skip_geocoding=False,
        skip_validation=False,
        shapefiles_dir=Path("."),  # Enable spatial joins (paths come from config)
    )

    # ==========================
    # VALIDATION
    # ==========================
    logger.info("Validating sample...")
    _validate_rows(survey_df, SurveyResponse, sample_size=10)

    # Filter to standard schema columns is handled centrally by
    # database.ingest_survey_responses(), so just pass the full DataFrame.
    responses_df = survey_df

    # ==========================
    # WEIGHTS TABLE
    # ==========================
    logger.info("Building weights table...")
    weights_df = survey_df.select(
        ["response_id", pl.col("weight").alias("boarding_weight"), "trip_weight"]
    ).with_columns(
        [
            pl.lit("baseline").alias("weight_scheme"),
            pl.lit("Baseline weights from BART 2024 survey").alias("description"),
        ]
    )

    # ==========================
    # METADATA TABLE
    # ==========================
    survey_id = f"{CANONICAL_OPERATOR}_{SURVEY_YEAR}"
    metadata_df = pl.DataFrame({
        "survey_id": [survey_id],
        "survey_year": [SURVEY_YEAR],
        "canonical_operator": [CANONICAL_OPERATOR],
        "survey_name": ["BART 2024 Station Profile Study"],
        "source": ["BART 2024 Station Profile Survey v1 (new weights)"],
        "field_start": [date(2024, 1, 1)],
        "field_end": [date(2024, 12, 31)],
        "inflation_year": [None],
        "ingestion_timestamp": [datetime.now(UTC)],
        "processing_notes": ["Preprocessed from Excel with fuzzy station matching"],
    })

    logger.info(
        "Prepared %s responses, %s weights, 1 metadata record",
        f"{len(responses_df):,}",
        f"{len(weights_df):,}",
    )
    return responses_df, weights_df, metadata_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    from transit_passenger_tools.database import _validate_rows, enforce_dataframe_types
    from transit_passenger_tools.models import SurveyMetadata, SurveyWeight

    responses, weights, metadata = main()
    responses = enforce_dataframe_types(responses)
    _validate_rows(metadata, SurveyMetadata)
    _validate_rows(weights, SurveyWeight)
    _validate_rows(responses, SurveyResponse)

    # Validate relational integrity between the three tables
    validate_referential_integrity(responses, weights, metadata)

    logger.info("All validation passed.")


    # from transit_passenger_tools.database import DATA_ROOT, ingest
    # paths = ingest(responses, weights, metadata, archive=False)
    # logger.info("COMPLETE — Data: %s", DATA_ROOT)
    # for table, path in paths.items():
    #     logger.info("  %s: %s", table, path)
