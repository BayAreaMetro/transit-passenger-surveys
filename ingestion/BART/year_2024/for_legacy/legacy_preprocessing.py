"""Preprocess BART 2024 survey data for standardization pipeline.

Reads raw BART 2024 Excel file, geocodes locations, processes demographics,
and outputs CSV in format expected by Build_Standard_Database.R

Uses Polars for efficient data processing.
"""

import logging
import sys
from pathlib import Path

import geopandas as gpd
import polars as pl

from transit_passenger_tools.geocoding.stations import (
    geocode_stops_from_names,
)

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

net_paths = {"M:": r"\\models.ad.mtc.ca.gov\data\models"}

# Replace letter drives with UNC paths for compatibility
for drive, unc_path in net_paths.items():
    SURVEY_PATH = SURVEY_PATH.replace(drive, unc_path)
    STATION_GEOJSON = STATION_GEOJSON.replace(drive, unc_path)
    OUTPUT_DIR = OUTPUT_DIR.replace(drive, unc_path)


# Constants
CANONICAL_OPERATOR = "BART"
SURVEY_TECH = "Heavy Rail"  # Must match TechnologyType enum
SURVEY_YEAR = 2024
FUZZY_MATCH_THRESHOLD = 80

# Station geocoding configuration
OPERATOR_NAMES = ["BART", "Bay Area Rapid Transit"]
STOP_NAME_FIELD = "stop_name"
AGENCY_FIELD = "agency"

# Survey field names (configurable)
ENTRY_STATION_FIELD = "ENTRY_STATION_FINAL"
EXIT_STATION_FIELD = "EXIT_STATION_FINAL"
TRIP_WEIGHT_FIELD = "combined_OD_weight_NEW"  # Trip weight
BOARDING_WEIGHT_FIELD = "combined_entry_weight_NEW"  # Boarding weight

# Home coordinate column names (already in survey)
HOME_ADDRESS_LAT = "HOME_ADDRESS_LAT"
HOME_ADDRESS_LONG = "HOME_ADDRESS_LONG"


# Setup logging
def setup_logging() -> None:
    """Setup logging to both console and file."""
    # Check if OUTPUT_DIR is accessible, otherwise use local directory
    if Path(OUTPUT_DIR).exists() or Path(OUTPUT_DIR).drive == "":
        log_dir = Path(OUTPUT_DIR)
    else:
        # Fallback to local output directory
        log_dir = Path(__file__).parent.parent / "output"
        logger_temp = logging.getLogger(__name__)
        logger_temp.warning(
            "OUTPUT_DIR %s not accessible, using local directory: %s", OUTPUT_DIR, log_dir
        )

    # Create directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "preprocessing_BART_2024.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        handlers=[logging.FileHandler(log_file, mode="w"), logging.StreamHandler(sys.stdout)],
    )


logger = logging.getLogger(__name__)


def read_survey_excel(
    path: str | Path,
    data_sheet: str = "data",
    codebook_sheet: str = "codebook",
    codebook_columns: list[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read survey data and codebook from Excel file.

    Args:
        path: Path to Excel file
        data_sheet: Name of sheet containing survey data
        codebook_sheet: Name of sheet containing codebook
        codebook_columns: Column names for codebook. Defaults to
            ["field", "description", "value", "value_description"]

    Returns:
        Tuple of (survey_df, codebook_df)

    Note:
        Codebook is expected to have merged cells for field/description columns.
        This function forward-fills those columns to handle merged cells.
    """
    if codebook_columns is None:
        codebook_columns = ["field", "description", "value", "value_description"]

    path = Path(path)
    logger.info("Reading survey data from %s", path)

    # Read data sheet
    survey_df = pl.read_excel(path, sheet_name=data_sheet, infer_schema_length=10000)
    logger.info("Read %s records from data sheet", f"{len(survey_df):,}")
    logger.info("Columns: %d", len(survey_df.columns))

    # Read codebook sheet
    codebook_df = pl.read_excel(path, sheet_name=codebook_sheet, has_header=False)
    codebook_df.columns = codebook_columns

    # Forward fill field and description columns to handle merged cells
    # (Excel merged cells only have value in first row)
    codebook_df = codebook_df.with_columns(
        [
            pl.col("field").fill_null(strategy="forward"),
            pl.col("description").fill_null(strategy="forward"),
        ]
    )

    logger.info("Read %s codebook entries", f"{len(codebook_df):,}")

    return survey_df, codebook_df


def create_codebook_lookup(codebook_df: pl.DataFrame, field_name: str) -> dict:
    """Create a lookup dictionary from codebook for a specific field.

    Args:
        codebook_df: Codebook DataFrame with columns 'field', 'value', 'value_description'
        field_name: Name of field to create lookup for

    Returns:
        Dictionary mapping value -> value_description
    """
    field_codes = codebook_df.filter(pl.col("field") == field_name)

    if field_codes.height == 0:
        return {}

    # Create lookup: value -> value_description
    lookup = dict(
        zip(
            field_codes.select("value").to_series().to_list(),
            field_codes.select("value_description").to_series().to_list(),
            strict=True,
        )
    )

    return lookup


def process_access_egress(bart_df: pl.DataFrame, codebook_df: pl.DataFrame) -> pl.DataFrame:
    """Process and recode access and egress modes using codebook."""
    logger.info("Processing access/egress modes")

    # Standard recoding for typical access/egress values
    # This is a fallback - actual codes should come from codebook
    standard_recode = {
        1: "walk",
        2: "bike",
        3: "pnr",  # drive alone/park and ride
        4: "knr",  # dropped off
        5: "local bus",
        6: "express bus",
        7: "light rail",
        8: "heavy rail",
        9: "commuter rail",
        10: "ferry",
        11: "taxi",
        12: "tnc",  # uber/lyft
        13: "other",
    }

    # Find access/egress columns
    access_cols = [col for col in bart_df.columns if "access" in col.lower()]
    egress_cols = [col for col in bart_df.columns if "egress" in col.lower()]

    logger.info("Found access columns: %s", access_cols)
    logger.info("Found egress columns: %s", egress_cols)

    # Process access columns
    for col in access_cols:
        # Try to get codebook mapping
        codebook_lookup = create_codebook_lookup(codebook_df, col)

        if codebook_lookup:
            logger.info("Using codebook for %s", col)
            bart_df = bart_df.with_columns(
                [pl.col(col).replace(codebook_lookup, default=None).alias(f"{col}_mode")]
            )
        else:
            logger.info("Using standard recode for %s", col)
            bart_df = bart_df.with_columns(
                [pl.col(col).replace(standard_recode, default=None).alias(f"{col}_mode")]
            )

    # Process egress columns
    for col in egress_cols:
        codebook_lookup = create_codebook_lookup(codebook_df, col)

        if codebook_lookup:
            logger.info("Using codebook for %s", col)
            bart_df = bart_df.with_columns(
                [pl.col(col).replace(codebook_lookup, default=None).alias(f"{col}_mode")]
            )
        else:
            logger.info("Using standard recode for %s", col)
            bart_df = bart_df.with_columns(
                [pl.col(col).replace(standard_recode, default=None).alias(f"{col}_mode")]
            )

    # Add standard access_mode and egress_mode fields expected by R script
    # Use the first processed access/egress column if available, otherwise null
    logger.info("Adding access_mode and egress_mode fields")
    if access_cols:
        primary_access_col = f"{access_cols[0]}_mode"
        bart_df = bart_df.with_columns([pl.col(primary_access_col).alias("access_mode")])
    else:
        bart_df = bart_df.with_columns([pl.lit(None).cast(pl.Utf8).alias("access_mode")])

    if egress_cols:
        primary_egress_col = f"{egress_cols[0]}_mode"
        bart_df = bart_df.with_columns([pl.col(primary_egress_col).alias("egress_mode")])
    else:
        bart_df = bart_df.with_columns([pl.lit(None).cast(pl.Utf8).alias("egress_mode")])

    return bart_df


def process_demographics(  # noqa: PLR0912, PLR0915
    survey_df: pl.DataFrame, codebook_df: pl.DataFrame
) -> pl.DataFrame:
    """Process demographic fields: race, age, language, gender using codebook."""
    logger.info("Processing demographics")

    # Race - look for race columns and decode using codebook
    race_cols = [col for col in survey_df.columns if "race" in col.lower()]
    logger.info("Found race columns: %s", race_cols)
    for col in race_cols:
        race_lookup = create_codebook_lookup(codebook_df, col)
        if race_lookup:
            survey_df = survey_df.with_columns(
                [pl.col(col).replace_strict(race_lookup, default=None, return_dtype=pl.Utf8)]
            )

    # Hispanic - look for hispanic/latino column and decode
    hispanic_cols = [
        col for col in survey_df.columns if "hisp" in col.lower() or "latin" in col.lower()
    ]
    if hispanic_cols:
        logger.info("Found hispanic column: %s", hispanic_cols[0])
        hispanic_lookup = create_codebook_lookup(codebook_df, hispanic_cols[0])
        if hispanic_lookup:
            # Decode first
            survey_df = survey_df.with_columns(
                [
                    pl.col(hispanic_cols[0])
                    .replace_strict(hispanic_lookup, default=None, return_dtype=pl.Utf8)
                    .alias("hispanic_decoded")
                ]
            )
            # Then create binary: any non-null value means Hispanic
            survey_df = survey_df.with_columns(
                [
                    pl.when(pl.col("hispanic_decoded").is_not_null())
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("hispanic")
                ]
            )
            survey_df = survey_df.drop("hispanic_decoded")
        else:
            # No codebook, assume it's already binary
            survey_df = survey_df.with_columns(
                [
                    pl.when(pl.col(hispanic_cols[0]).is_not_null())
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("hispanic")
                ]
            )

    # Age - look for age or birth year columns and decode
    age_cols = [
        col
        for col in survey_df.columns
        if "age" in col.lower() or "birth" in col.lower() or "born" in col.lower()
    ]
    logger.info("Found age columns: %s", age_cols)

    if age_cols:
        # If we have birth year, use it directly
        birth_year_cols = [
            col for col in age_cols if "birth" in col.lower() or "born" in col.lower()
        ]
        if birth_year_cols:
            logger.info("Using birth year column: %s", birth_year_cols[0])
            survey_df = survey_df.with_columns(
                [pl.col(birth_year_cols[0]).alias("year_born_four_digit")]
            )
        else:
            # If we have age categories, map to approximate birth year
            logger.info("Converting age categories to year_born")
            # This mapping needs to be customized based on actual categories
            age_to_birth_year = {
                1: SURVEY_YEAR - 20,  # Approximate midpoint
                2: SURVEY_YEAR - 30,
                3: SURVEY_YEAR - 40,
                4: SURVEY_YEAR - 50,
                5: SURVEY_YEAR - 60,
                6: SURVEY_YEAR - 70,
            }
            survey_df = survey_df.with_columns(
                [
                    pl.col(age_cols[0])
                    .replace_strict(age_to_birth_year, default=None, return_dtype=pl.Int64)
                    .alias("year_born_four_digit")
                ]
            )

    # Language - decode using codebook, then create binary
    lang_cols = [
        col for col in survey_df.columns if "language" in col.lower() or "lang" in col.lower()
    ]
    # Filter out survey_language columns
    lang_cols = [col for col in lang_cols if "survey" not in col.lower()]
    logger.info("Found language columns: %s", lang_cols)

    # Decode PRIMARY_LANGUAGE1_FINAL using codebook for language_at_home_detail
    lang_lookup = create_codebook_lookup(codebook_df, "PRIMARY_LANGUAGE1_FINAL")
    if lang_lookup:
        survey_df = survey_df.with_columns(
            [
                pl.col("PRIMARY_LANGUAGE1_FINAL")
                .replace_strict(lang_lookup, default=None, return_dtype=pl.Utf8)
                .alias("language_at_home_detail")
            ]
        )
    else:
        survey_df = survey_df.with_columns(
            [pl.lit(None).cast(pl.Utf8).alias("language_at_home_detail")]
        )

    # Create language_at_home_binary (English only vs Other)
    survey_df = survey_df.with_columns(
        [
            pl.when(pl.col("language_at_home_detail").str.to_lowercase().str.contains("english"))
            .then(pl.lit("ENGLISH ONLY"))
            .when(pl.col("language_at_home_detail").is_not_null())
            .then(pl.lit("OTHER"))
            .otherwise(None)
            .alias("language_at_home_binary"),
            pl.lit(None).cast(pl.Utf8).alias("language_at_home_detail_other"),
        ]
    )

    # Gender - decode using codebook
    gender_cols = [
        col for col in survey_df.columns if "gender" in col.lower() or "sex" in col.lower()
    ]
    if gender_cols:
        logger.info("Found gender column: %s", gender_cols[0])
        gender_lookup = create_codebook_lookup(codebook_df, gender_cols[0])
        if gender_lookup:
            survey_df = survey_df.with_columns(
                [
                    pl.col(gender_cols[0])
                    .replace_strict(gender_lookup, default=None, return_dtype=pl.Utf8)
                    .alias("gender")
                ]
            )
        else:
            # No codebook, use as-is
            survey_df = survey_df.with_columns([pl.col(gender_cols[0]).alias("gender")])

    # Add race dummy variable (1 if any race field is populated)
    logger.info("Adding race_dmy_ind field")
    race_cols = [col for col in survey_df.columns if "RACE_" in col]
    survey_df = survey_df.with_columns(
        [
            pl.when(pl.any_horizontal([pl.col(c).is_not_null() for c in race_cols]))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("race_dmy_ind")
        ]
    )

    # Add individual race dummy variables (1 if that specific race is marked)
    logger.info("Adding individual race dummy variables")

    # Map SingleRaceCode to race_cat (SingleRaceCode is already text)
    race_cat_mapping = {
        "American Indian, non-Hispanic": "NATIVE AMERICAN",
        "Asian or Pac Islander, non-Hispanic": "ASIAN",
        "African American, non-Hispanic": "BLACK",
        "White or MENA, non-Hispanic": "WHITE",
        "Hispanic, any race": "HISPANIC",
        "Multi-racial, non-Hispanic": "OTHER",
        "No response": None,
        "Other, non-Hispanic": "OTHER",
    }

    survey_df = survey_df.with_columns(
        [
            pl.col("RACE_1_AmIndian").fill_null(0).cast(pl.Int8).alias("race_dmy_amind"),
            pl.col("RACE_2_Asian").fill_null(0).cast(pl.Int8).alias("race_dmy_asn"),
            pl.col("RACE_3_AfrAm").fill_null(0).cast(pl.Int8).alias("race_dmy_blk"),
            pl.col("RACE_4_PI").fill_null(0).cast(pl.Int8).alias("race_dmy_pacisl"),
            pl.col("RACE_5_White").fill_null(0).cast(pl.Int8).alias("race_dmy_wht"),
            pl.col("RACE_6_Hispanic").fill_null(0).cast(pl.Int8).alias("race_dmy_hisp"),
            pl.col("RACE_98_Other").fill_null(0).cast(pl.Int8).alias("race_dmy_othr"),
            pl.lit(0).cast(pl.Int8).alias("race_dmy_hwi"),  # Hawaiian not in BART 2024
            pl.lit(0).cast(pl.Int8).alias("race_dmy_mdl_estn"),  # Middle Eastern not in BART 2024
            pl.col("SingleRaceCode").replace(race_cat_mapping, default=None).alias("race_cat"),
            pl.col("RACE_OTHER").alias("race_other_string"),
        ]
    )

    return survey_df


def process_trip_characteristics(bart_df: pl.DataFrame, _codebook_df: pl.DataFrame) -> pl.DataFrame:
    """Process trip characteristics: origin/destination purposes, transfers, etc."""
    logger.info("Processing trip characteristics")

    # BART 2024 doesn't have meaningful purpose data, add null fields
    logger.info(
        "Adding orig_purp, dest_purp, trip_purp, and related work/school tour fields as null"
    )
    bart_df = bart_df.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("orig_purp"),
            pl.lit(None).cast(pl.Utf8).alias("dest_purp"),
            pl.lit(None).cast(pl.Utf8).alias("trip_purp"),
            pl.lit(None).cast(pl.Utf8).alias("at_work_prior_to_orig_purp"),
            pl.lit(None).cast(pl.Utf8).alias("at_school_prior_to_orig_purp"),
            pl.lit(None).cast(pl.Utf8).alias("at_work_after_dest_purp"),
            pl.lit(None).cast(pl.Utf8).alias("at_school_after_dest_purp"),
        ]
    )

    # Add workplace and school location fields (BART 2024 has these)
    logger.info("Adding workplace and school location fields")
    bart_df = bart_df.with_columns(
        [
            pl.col("WORK_ADDRESS_LAT").alias("workplace_lat"),
            pl.col("WORK_ADDRESS_LONG").alias("workplace_lon"),
            pl.col("SCHOOL_ADDRESS_LAT").alias("school_lat"),
            pl.col("SCHOOL_ADDRESS_LONG").alias("school_lon"),
        ]
    )

    # Rename household/demographics fields to standard names (these exist in survey)
    logger.info(
        "Renaming EMPLOYMENT_STATUS_EDITED -> work_status, STUDENT_STATUS_EDITED -> student_status"
    )
    bart_df = bart_df.rename(
        {
            "EMPLOYMENT_STATUS_EDITED": "work_status",
            "STUDENT_STATUS_EDITED": "student_status",
            "HH_SIZE": "persons",
            "COUNT_VH_HH": "vehicles",
            "EMPLOYED_IN_HH_EDITED": "workers",
            "HHI_EDITED": "household_income",
            "ENGLISH_ABILITY": "eng_proficient",
        }
    )

    # Add the _other fields as null (used when value is 6+ or "other")
    logger.info("Adding persons_other, vehicles_other, workers_other as null")
    bart_df = bart_df.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("persons_other"),
            pl.lit(None).cast(pl.Utf8).alias("vehicles_other"),
            pl.lit(None).cast(pl.Utf8).alias("workers_other"),
        ]
    )

    # Add transfer route fields (BART 2024 doesn't have detailed transfer routing)
    logger.info("Adding transfer route fields as null")
    bart_df = bart_df.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("first_route_before_survey_board"),
            pl.lit(None).cast(pl.Utf8).alias("first_route_after_survey_alight"),
            pl.lit(None).cast(pl.Utf8).alias("second_route_before_survey_board"),
            pl.lit(None).cast(pl.Utf8).alias("second_route_after_survey_alight"),
            pl.lit(None).cast(pl.Utf8).alias("third_route_before_survey_board"),
            pl.lit(None).cast(pl.Utf8).alias("third_route_after_survey_alight"),
            pl.lit(None).cast(pl.Utf8).alias("fourth_route_before_survey_board"),
            pl.lit(None).cast(pl.Utf8).alias("fourth_route_after_survey_alight"),
        ]
    )

    # Use the actual PREV_TRANSFERS and NEXT_TRANSFERS fields
    logger.info("Mapping PREV_TRANSFERS and NEXT_TRANSFERS to number_transfers fields")
    bart_df = bart_df.with_columns(
        [
            pl.col("PREV_TRANSFERS").alias("number_transfers_orig_board"),
            pl.col("NEXT_TRANSFERS").alias("number_transfers_alight_dest"),
        ]
    )

    # Transfers
    transfer_cols = [col for col in bart_df.columns if "transfer" in col.lower()]
    logger.info("Found transfer columns: %s", transfer_cols)

    # Create date_string from DATE_COMPLETED (modeling) or DATE_STARTED_SAS (SAS)
    # Format as MM/DD/YYYY for R script (which will derive day_of_the_week)
    logger.info("Creating date_string from DATE_COMPLETED or DATE_STARTED_SAS")
    bart_df = bart_df.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("DATE_COMPLETED").cast(pl.Utf8),
                    pl.col("DATE_STARTED_SAS").dt.strftime("%m/%d/%Y"),
                ]
            ).alias("date_string")
        ]
    )

    # Create time_string from SURVEY_END_TIME or SURVEY_START_TIME
    logger.info("Creating time_string from SURVEY_END_TIME or SURVEY_START_TIME")
    bart_df = bart_df.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("SURVEY_END_TIME").cast(pl.Utf8),
                    pl.col("DATE_STARTED_SAS").dt.strftime("%H:%M:%S"),
                ]
            ).alias("time_string")
        ]
    )

    # Add time_period from TIME_ON_fnl (survey strata)
    logger.info("Mapping TIME_ON_fnl to time_period")
    bart_df = bart_df.with_columns([pl.col("TIME_ON_fnl").cast(pl.Utf8).alias("time_period")])

    # We don't have a depart_time field, so set as null
    logger.info("Adding depart_time, return_time, depart_hour, return_hour fields as null")
    bart_df = bart_df.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("depart_time"),
            pl.lit(None).cast(pl.Utf8).alias("return_time"),
            pl.lit(None).cast(pl.Utf8).alias("depart_hour"),
            pl.lit(None).cast(pl.Utf8).alias("return_hour"),
        ]
    )

    # Add origin/destination coordinates (BART 2024 doesn't collect trip O/D, only home/work/school)
    logger.info("Adding orig_lat, orig_lon, dest_lat, dest_lon as null")
    bart_df = bart_df.with_columns(
        [
            pl.lit(None).cast(pl.Float64).alias("orig_lat"),
            pl.lit(None).cast(pl.Float64).alias("orig_lon"),
            pl.lit(None).cast(pl.Float64).alias("dest_lat"),
            pl.lit(None).cast(pl.Float64).alias("dest_lon"),
        ]
    )

    # Add metadata fields
    logger.info("Adding interview_language and survey_type fields")
    bart_df = bart_df.with_columns(
        [
            pl.lit("ENGLISH").alias("interview_language"),  # Default to English
            pl.when(pl.col("DATE_COMPLETED").is_not_null())
            .then(pl.lit("online - modeling platform"))
            .when(pl.col("DATE_STARTED_SAS").is_not_null())
            .then(pl.lit("online - SAS platform"))
            .otherwise(pl.lit("online"))
            .alias("survey_type"),
        ]
    )

    # Add fare fields (household_income and eng_proficient already renamed above)
    logger.info("Adding fare_category and fare_medium fields")
    bart_df = bart_df.with_columns(
        [
            pl.col("TYPE_OF_FARE").alias("fare_category"),
            pl.lit(None).cast(pl.Utf8).alias("fare_medium"),
            pl.lit(None).cast(pl.Utf8).alias("clipper_detail"),
            pl.lit(None).cast(pl.Utf8).alias("fare_medium_other"),
            pl.lit(None).cast(pl.Utf8).alias("fare_category_other"),
        ]
    )

    # Add weight fields
    logger.info("Adding weight field")
    bart_df = bart_df.with_columns(
        [
            pl.col(TRIP_WEIGHT_FIELD).alias("trip_weight"),
            pl.col(BOARDING_WEIGHT_FIELD).alias("weight"),
            # R expects these for some annoying reason
            pl.lit(None, dtype=pl.Float64).alias("alt_weight"),
            pl.lit(None, dtype=pl.Float64).alias("tweight"),
        ]
    )

    return bart_df


# Dictionary mappings are manually maintained in:
# ingestion/BART/BART_2024_dictionary_mappings.csv
# This includes both NONCATEGORICAL (ID, weight, etc.) and categorical (work_status, etc.) mappings


def main() -> None:  # noqa: PLR0915
    """Main preprocessing pipeline."""
    setup_logging()

    logger.info("=" * 80)
    logger.info("BART 2024 Preprocessing Pipeline")
    logger.info("=" * 80)
    # Read data
    survey_df, codebook_df = read_survey_excel(SURVEY_PATH)

    logger.info("\nInitial data shape: %s", survey_df.shape)
    logger.info("Initial columns: %s", len(survey_df.columns))

    logger.info("Loading station GeoJSON")
    stations_gdf = gpd.read_file(STATION_GEOJSON)

    # Decode station codes to names using codebook
    logger.info("Decoding station codes to names")
    entry_lookup = create_codebook_lookup(codebook_df, ENTRY_STATION_FIELD)
    exit_lookup = create_codebook_lookup(codebook_df, EXIT_STATION_FIELD)

    survey_df = survey_df.with_columns(
        [
            pl.col(ENTRY_STATION_FIELD)
            .replace_strict(entry_lookup, default=None, return_dtype=pl.Utf8)
            .alias("entry_station_name"),
            pl.col(EXIT_STATION_FIELD)
            .replace_strict(exit_lookup, default=None, return_dtype=pl.Utf8)
            .alias("exit_station_name"),
        ]
    )

    # Geocode decoded station names to coordinates
    logger.info("Geocoding stations")
    survey_df = geocode_stops_from_names(
        survey_df=survey_df,
        stops_gdf=stations_gdf,
        station_columns={
            "entry_station_name": {
                "station": "survey_board_station",
                "lat": "survey_board_lat",
                "lon": "survey_board_lon",
                "geo_level": "survey_board_geo_level",
            },
            "exit_station_name": {
                "station": "survey_alight_station",
                "lat": "survey_alight_lat",
                "lon": "survey_alight_lon",
                "geo_level": "survey_alight_geo_level",
            },
        },
        operator_names=OPERATOR_NAMES,
        stop_name_field=STOP_NAME_FIELD,
        agency_field=AGENCY_FIELD,
        fuzzy_threshold=FUZZY_MATCH_THRESHOLD,
    )

    # Use existing home coordinates from survey
    logger.info("Renaming home coordinate columns")
    survey_df = survey_df.rename(
        {
            HOME_ADDRESS_LAT: "home_lat",
            HOME_ADDRESS_LONG: "home_lon",
        }
    )
    # Add geo_level for home locations
    survey_df = survey_df.with_columns(
        [
            pl.when(pl.col("home_lat").is_not_null())
            .then(pl.lit("address"))
            .otherwise(None)
            .alias("home_geo_level")
        ]
    )

    # Process access/egress
    survey_df = process_access_egress(survey_df, codebook_df)

    # Process demographics
    survey_df = process_demographics(survey_df, codebook_df)

    # Process trip characteristics
    survey_df = process_trip_characteristics(survey_df, codebook_df)

    # Add metadata
    logger.info("Adding metadata columns")
    survey_df = survey_df.with_columns(
        [
            pl.lit(CANONICAL_OPERATOR).alias("canonical_operator"),
            pl.lit(SURVEY_TECH).alias("survey_tech"),
            pl.lit(SURVEY_YEAR).alias("survey_year"),
            pl.lit(CANONICAL_OPERATOR).alias("survey_name"),
            pl.lit(CANONICAL_OPERATOR).alias("operator"),
        ]
    )

    # Add route field (BART doesn't have routes in traditional sense, use line or null)
    logger.info("Adding route field")
    survey_df = survey_df.with_columns([pl.lit(None).cast(pl.Utf8).alias("route")])

    # Add direction field (BART doesn't have traditional direction data)
    logger.info("Adding direction field")
    survey_df = survey_df.with_columns([pl.lit(None).cast(pl.Utf8).alias("direction")])

    # Add survey_board and survey_alight (station names from geocoding)
    logger.info("Adding survey_board and survey_alight")
    survey_df = survey_df.with_columns(
        [
            pl.col("survey_board_station").alias("survey_board"),
            pl.col("survey_alight_station").alias("survey_alight"),
        ]
    )

    # Add first_board and last_alight coordinates
    # (same as survey_board/survey_alight for single-leg trips)
    logger.info("Adding first_board and last_alight coordinates")
    survey_df = survey_df.with_columns(
        [
            pl.col("survey_board_lat").alias("first_board_lat"),
            pl.col("survey_board_lon").alias("first_board_lon"),
            pl.col("survey_alight_lat").alias("last_alight_lat"),
            pl.col("survey_alight_lon").alias("last_alight_lon"),
        ]
    )

    # Format the ID columns
    # pipeline expects "ID" to be unique within the survey
    # thus this combined survey will need:
    #   ID -> subsurvey_ID
    #   UNIQUE_IDENTIFIER -> ID
    survey_df = survey_df.rename({"ID": "subsurvey_id", "UNIQUE_IDENTIFIER": "ID"})

    # Determine output directory (use local if M: drive not accessible)
    if Path(OUTPUT_DIR).exists() or Path(OUTPUT_DIR).drive == "":
        output_dir = Path(OUTPUT_DIR)
    else:
        output_dir = Path(__file__).parent.parent / "output"
        logger.warning(
            "OUTPUT_DIR %s not accessible, using local directory: %s", OUTPUT_DIR, output_dir
        )
        output_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize the comment field to remove bad characters
    survey_df = survey_df.with_columns(
        pl.col("COMMENT")
        .cast(pl.Utf8)
        .str.replace_all("\x00", "")
        .str.replace_all("\r\n", "\n")
        .str.replace_all("\r", "\n")
        .str.replace_all("\n", r"\n")
    )

    # Move COMMENT to the end to keep CSV slightly neater
    if "COMMENT" in survey_df.columns:
        cols = [col for col in survey_df.columns if col != "COMMENT"] + ["COMMENT"]
        survey_df = survey_df.select(cols)

    # Write output
    output_file = output_dir / "BART_2024_preprocessed.csv"
    logger.info("\nWriting output to %s", output_file)
    survey_df.write_csv(output_file, line_terminator="\n", quote_style="necessary")

    # Prepare codebook for R Pipeline
    codebook_df_r = (
        codebook_df.rename(
            {
                "field": "Survey_Variable",
                "value": "Survey_Response",
                "description": "Survey_Description",
                "value_description": "Survey_Value_Description",
            }
        )
        .with_columns(
            [
                pl.lit(CANONICAL_OPERATOR).alias("Survey_Name"),
                pl.lit(SURVEY_YEAR).alias("Survey_Year"),
                pl.col("Survey_Response").fill_null(pl.lit("NONCATEGORICAL")),
                pl.lit(None).cast(pl.Utf8).alias("Generic_Variable"),
                pl.lit(None).cast(pl.Utf8).alias("Generic_Response"),
            ]
        )
        .with_columns(
            pl.when(pl.col("Survey_Response") == "NONCATEGORICAL")
            .then(pl.lit("NONCATEGORICAL"))
            .otherwise(pl.col("Generic_Response"))
            .alias("Generic_Response")
        )
        .select(
            "Survey_Name",
            "Survey_Year",
            "Survey_Variable",
            "Survey_Response",
            "Generic_Variable",
            "Generic_Response",
            "Survey_Description",
            "Survey_Value_Description",
        )
    )

    codebook_file = output_dir / "BART_2024_codebook.csv"
    logger.info("Writing codebook to %s", codebook_file)
    codebook_df.write_csv(codebook_file, line_terminator="\n", quote_style="necessary")
    codebook_df_r.write_csv(
        output_dir / "BART_2024_codebook_for_R.csv", line_terminator="\n", quote_style="necessary"
    )

    # Check that the CSV is written correctly
    _ = pl.read_csv(output_file, infer_schema_length=10000)

    logger.info("Wrote %s records with %s columns", f"{len(survey_df):,}", len(survey_df.columns))

    logger.info("\n=== Output Summary ===")
    logger.info("Output file: %s", output_file)
    logger.info("Records: %s", f"{len(survey_df):,}")
    logger.info("Columns: %s", len(survey_df.columns))

    # Log sample of key columns
    key_cols = ["ID", "canonical_operator", "survey_tech", "survey_year"]
    key_cols = [col for col in key_cols if col in survey_df.columns]
    logger.info("\nSample data (first 3 rows): %d columns shown", len(key_cols))

    logger.info("%s", "\n" + "=" * 80)
    logger.info("Preprocessing complete!")
    logger.info("%s", "=" * 80)

    # Update main dictionary file
    update_main_dictionary()


def update_main_dictionary() -> None:
    """Update the main Dictionary_for_Standard_Database.csv with BART 2024 entries.

    Removes any existing BART 2024 entries and appends current entries from
    BART_2024_dictionary_mappings.csv to prevent duplicates.
    """
    logger.info("\n%s", "=" * 80)
    logger.info("Updating main dictionary file")
    logger.info("%s", "=" * 80)

    # Paths
    bart_dict_path = Path(__file__).parent / "BART_2024_dictionary_mappings.csv"
    main_dict_path = (
        Path(__file__).parent.parent.parent.parent
        / "archive"
        / "make-uniform"
        / "production"
        / "Dictionary_for_Standard_Database.csv"
    )

    if not bart_dict_path.exists():
        logger.error("BART dictionary file not found: %s", bart_dict_path)
        return

    if not main_dict_path.exists():
        logger.error("Main dictionary file not found: %s", main_dict_path)
        return

    # Read BART 2024 entries (skip header)
    logger.info("Reading BART 2024 entries from: %s", bart_dict_path)
    bart_entries = bart_dict_path.read_text(encoding="utf-8").split("\n")
    bart_data = [line for line in bart_entries[1:] if line.strip()]  # skip header
    logger.info("Found %d BART 2024 entries", len(bart_data))

    # Read main dictionary
    logger.info("Reading main dictionary from: %s", main_dict_path)
    main_content = main_dict_path.read_text(encoding="utf-8").strip().split("\n")
    header = main_content[0]
    main_data = main_content[1:]

    # Remove existing BART 2024 entries (keep only non-empty, non-BART 2024 lines)
    original_count = len([line for line in main_data if line.strip()])
    main_data_filtered = [
        line for line in main_data if line.strip() and not line.startswith("BART,2024,")
    ]
    filtered_count = len(main_data_filtered)
    removed_count = original_count - filtered_count
    logger.info("Removed %d existing BART 2024 entries", removed_count)

    # Combine: header + filtered main data + BART 2024 entries
    updated_content = [header, *main_data_filtered, *bart_data]

    # Write back with single newline at end
    main_dict_path.write_text("\n".join(updated_content) + "\n", encoding="utf-8")

    final_count = len([line for line in updated_content[1:] if line.strip()])
    logger.info(
        "Updated main dictionary: %d total entries (%d BART 2024)", final_count, len(bart_data)
    )
    logger.info("Main dictionary updated: %s", main_dict_path)


if __name__ == "__main__":
    main()
