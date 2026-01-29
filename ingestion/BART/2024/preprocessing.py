"""Preprocess BART 2024 survey data for standardization pipeline.

Reads raw BART 2024 Excel file, geocodes locations, processes demographics,
and returns DataFrame ready for pipeline processing.

Uses Polars for efficient data processing.
"""
import logging
import sys
from pathlib import Path

import geopandas as gpd
import polars as pl

from transit_passenger_tools.utils.stations import geocode_stops_from_names

# ============================================================================
# BART 2024 SURVEY FIELD CONSTANTS
# These are specific to the BART 2024 data structure and don't need to be
# configured at the ingestion level
# ============================================================================

# Survey field names (specific to BART 2024 Excel structure)
ENTRY_STATION_FIELD = "ENTRY_STATION_FINAL"
EXIT_STATION_FIELD = "EXIT_STATION_FINAL"
TRIP_WEIGHT_FIELD = "combined_OD_weight_NEW"  # Trip weight
BOARDING_WEIGHT_FIELD = "combined_entry_weight_NEW"  # Boarding weight

# Home coordinate column names (already in BART survey)
HOME_ADDRESS_LAT = "HOME_ADDRESS_LAT"
HOME_ADDRESS_LONG = "HOME_ADDRESS_LONG"


# Setup logging
def setup_logging(output_dir: str = None) -> None:
    """Setup logging to both console and file."""
    # Use provided output_dir or fallback to local directory
    if output_dir and (Path(output_dir).exists() or Path(output_dir).drive == ""):
        log_dir = Path(output_dir)
    else:
        # Fallback to local output directory
        log_dir = Path(__file__).parent / "output"
        logger_temp = logging.getLogger(__name__)
        if output_dir:
            logger_temp.warning(
                "Output directory %s not accessible, using local directory: %s",
                output_dir,
                log_dir
            )

    # Create directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "preprocessing_BART_2024.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler(sys.stdout)
        ]
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
    survey_df = pl.read_excel(path, sheet_name=data_sheet, infer_schema_length=20000)
    logger.info("Read %s records from data sheet", f"{len(survey_df):,}")
    logger.info("Columns: %d", len(survey_df.columns))

    # Read codebook sheet
    codebook_df = pl.read_excel(
        path,
        sheet_name=codebook_sheet,
        has_header=False
    )
    codebook_df.columns = codebook_columns

    # Forward fill field and description columns to handle merged cells
    # (Excel merged cells only have value in first row)
    codebook_df = codebook_df.with_columns([
        pl.col("field").fill_null(strategy="forward"),
        pl.col("description").fill_null(strategy="forward")
    ])

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
    lookup = dict(zip(
        field_codes.select("value").to_series().to_list(),
        field_codes.select("value_description").to_series().to_list(),
        strict=True
    ))

    return lookup

def process_access_egress(
    bart_df: pl.DataFrame, codebook_df: pl.DataFrame
) -> pl.DataFrame:
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
        13: "other"
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
            bart_df = bart_df.with_columns([
                pl.col(col).replace(codebook_lookup, default=None).alias(f"{col}_mode")
            ])
        else:
            logger.info("Using standard recode for %s", col)
            bart_df = bart_df.with_columns([
                pl.col(col).replace(standard_recode, default=None).alias(f"{col}_mode")
            ])

    # Process egress columns
    for col in egress_cols:
        codebook_lookup = create_codebook_lookup(codebook_df, col)

        if codebook_lookup:
            logger.info("Using codebook for %s", col)
            bart_df = bart_df.with_columns([
                pl.col(col).replace(codebook_lookup, default=None).alias(f"{col}_mode")
            ])
        else:
            logger.info("Using standard recode for %s", col)
            bart_df = bart_df.with_columns([
                pl.col(col).replace(standard_recode, default=None).alias(f"{col}_mode")
            ])

    # Add standard access_mode and egress_mode fields expected by R script
    # Use the first processed access/egress column if available, otherwise null
    logger.info("Adding access_mode and egress_mode fields")
    if access_cols:
        primary_access_col = f"{access_cols[0]}_mode"
        bart_df = bart_df.with_columns([
            pl.col(primary_access_col).alias("access_mode")
        ])
    else:
        bart_df = bart_df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("access_mode")
        ])

    if egress_cols:
        primary_egress_col = f"{egress_cols[0]}_mode"
        bart_df = bart_df.with_columns([
            pl.col(primary_egress_col).alias("egress_mode")
        ])
    else:
        bart_df = bart_df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("egress_mode")
        ])

    return bart_df


def process_demographics(  # noqa: PLR0912, PLR0915
    survey_df: pl.DataFrame,
    codebook_df: pl.DataFrame,
    survey_year: int,
    ) -> pl.DataFrame:
    """Process demographic fields: race, age, language, gender using codebook."""
    logger.info("Processing demographics")

    # Race - look for race columns and decode using codebook
    race_cols = [col for col in survey_df.columns if "race" in col.lower()]
    logger.info("Found race columns: %s", race_cols)
    for col in race_cols:
        race_lookup = create_codebook_lookup(codebook_df, col)
        if race_lookup:
            survey_df = survey_df.with_columns([
                pl.col(col).replace_strict(race_lookup, default=None, return_dtype=pl.Utf8)
            ])

    # Hispanic - look for hispanic/latino column and decode
    hispanic_cols = [
        col for col in survey_df.columns
        if "hisp" in col.lower() or "latin" in col.lower()
    ]
    if hispanic_cols:
        logger.info("Found hispanic column: %s", hispanic_cols[0])
        hispanic_lookup = create_codebook_lookup(codebook_df, hispanic_cols[0])
        if hispanic_lookup:
            # Decode first
            survey_df = survey_df.with_columns([
                pl.col(hispanic_cols[0]).replace_strict(
                    hispanic_lookup, default=None, return_dtype=pl.Utf8
                ).alias("hispanic_decoded")
            ])
            # Then create binary: any non-null value means Hispanic
            survey_df = survey_df.with_columns([
                pl.when(pl.col("hispanic_decoded").is_not_null())
                  .then(pl.lit(1))
                  .otherwise(pl.lit(0))
                  .alias("hispanic")
            ])
            survey_df = survey_df.drop("hispanic_decoded")
        else:
            # No codebook, assume it's already binary
            survey_df = survey_df.with_columns([
                pl.when(pl.col(hispanic_cols[0]).is_not_null())
                  .then(pl.lit(1))
                  .otherwise(pl.lit(0))
                  .alias("hispanic")
            ])

    # Age - look for age or birth year columns and decode
    age_cols = [
        col for col in survey_df.columns
        if "age" in col.lower() or "birth" in col.lower() or "born" in col.lower()
    ]
    logger.info("Found age columns: %s", age_cols)

    if age_cols:
        # If we have birth year, use it directly
        birth_year_cols = [
            col for col in age_cols
            if "birth" in col.lower() or "born" in col.lower()
        ]
        if birth_year_cols:
            logger.info("Using birth year column: %s", birth_year_cols[0])
            survey_df = survey_df.with_columns([
                pl.col(birth_year_cols[0]).alias("year_born_four_digit")
            ])
        else:
            # If we have age categories, map to approximate birth year
            logger.info("Converting age categories to year_born")
            # This mapping needs to be customized based on actual categories
            age_to_birth_year = {
                1: survey_year - 20,  # Approximate midpoint
                2: survey_year - 30,
                3: survey_year - 40,
                4: survey_year - 50,
                5: survey_year - 60,
                6: survey_year - 70,
            }
            survey_df = survey_df.with_columns([
                pl.col(age_cols[0]).replace_strict(
                    age_to_birth_year, default=None, return_dtype=pl.Int64
                ).alias("year_born_four_digit")
            ])

    # Language - decode using codebook, then create binary
    lang_cols = [
        col for col in survey_df.columns
        if "language" in col.lower() or "lang" in col.lower()
    ]
    # Filter out survey_language columns
    lang_cols = [col for col in lang_cols if "survey" not in col.lower()]
    logger.info("Found language columns: %s", lang_cols)

    # Decode PRIMARY_LANGUAGE1_FINAL using codebook for language_at_home_detail
    lang_lookup = create_codebook_lookup(codebook_df, "PRIMARY_LANGUAGE1_FINAL")
    if lang_lookup:
        survey_df = survey_df.with_columns([
            pl.col("PRIMARY_LANGUAGE1_FINAL").replace_strict(
                lang_lookup, default=None, return_dtype=pl.Utf8
            ).alias("language_at_home_detail")
        ])
    else:
        survey_df = survey_df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("language_at_home_detail")
        ])

    # Create language_at_home_binary (English only vs Other)
    survey_df = survey_df.with_columns([
        pl.when(pl.col("language_at_home_detail").str.to_lowercase().str.contains("english"))
          .then(pl.lit("ENGLISH ONLY"))
          .when(pl.col("language_at_home_detail").is_not_null())
          .then(pl.lit("OTHER"))
          .otherwise(None)
          .alias("language_at_home_binary"),
        pl.lit(None).cast(pl.Utf8).alias("language_at_home_detail_other")
    ])

    # Gender - decode using codebook
    gender_cols = [
        col for col in survey_df.columns
        if "gender" in col.lower() or "sex" in col.lower()
    ]
    if gender_cols:
        logger.info("Found gender column: %s", gender_cols[0])
        gender_lookup = create_codebook_lookup(codebook_df, gender_cols[0])
        if gender_lookup:
            survey_df = survey_df.with_columns([
                pl.col(gender_cols[0]).replace_strict(
                    gender_lookup, default=None, return_dtype=pl.Utf8
                ).alias("gender")
            ])
        else:
            survey_df = survey_df.with_columns([
                pl.col(gender_cols[0]).alias("gender")
            ])
    else:
        survey_df = survey_df.with_columns([pl.lit(None).cast(pl.Utf8).alias("gender")])

    # Add race dummy variable (1 if any race field is populated)
    logger.info("Adding race_dmy_ind field")
    race_cols = [col for col in survey_df.columns if "RACE_" in col]
    survey_df = survey_df.with_columns([
        pl.when(pl.any_horizontal([pl.col(c).is_not_null() for c in race_cols]))
          .then(pl.lit(1))
          .otherwise(pl.lit(0))
          .alias("race_dmy_ind")
    ])

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
        "Other, non-Hispanic": "OTHER"
    }

    survey_df = survey_df.with_columns([
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
        pl.col("RACE_OTHER").cast(pl.Utf8).alias("race_other_string")
    ])

    return survey_df

def process_trip_characteristics(
    bart_df: pl.DataFrame, codebook_df: pl.DataFrame
) -> pl.DataFrame:
    """Process trip characteristics: origin/destination purposes, transfers, etc."""
    logger.info("Processing trip characteristics")

    # BART 2024 doesn't have meaningful purpose data, add null fields
    logger.info(
        "Adding orig_purp, dest_purp, trip_purp, and related work/school tour fields as null"
        )
    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("orig_purp"),
        pl.lit(None).cast(pl.Utf8).alias("dest_purp"),
        pl.lit(None).cast(pl.Utf8).alias("trip_purp"),
        pl.lit(None).cast(pl.Boolean).alias("at_work_prior_to_orig_purp"),
        pl.lit(None).cast(pl.Boolean).alias("at_school_prior_to_orig_purp"),
        pl.lit(None).cast(pl.Boolean).alias("at_work_after_dest_purp"),
        pl.lit(None).cast(pl.Boolean).alias("at_school_after_dest_purp")
    ])

    # Add workplace and school location fields (BART 2024 has these)
    logger.info("Adding workplace and school location fields")
    bart_df = bart_df.with_columns([
        pl.col("WORK_ADDRESS_LAT").alias("workplace_lat"),
        pl.col("WORK_ADDRESS_LONG").alias("workplace_lon"),
        pl.col("SCHOOL_ADDRESS_LAT").alias("school_lat"),
        pl.col("SCHOOL_ADDRESS_LONG").alias("school_lon")
    ])

    # Rename household/demographics fields to standard names (these exist in survey)
    logger.info(
        "Renaming EMPLOYMENT_STATUS_EDITED -> work_status, STUDENT_STATUS_EDITED -> student_status"
    )
    bart_df = bart_df.rename({
        "EMPLOYMENT_STATUS_EDITED": "work_status",
        "STUDENT_STATUS_EDITED": "student_status",
        "HH_SIZE": "persons",
        "COUNT_VH_HH": "vehicles",
        "EMPLOYED_IN_HH_EDITED": "workers",
        "HHI_EDITED": "household_income",
        "ENGLISH_ABILITY": "eng_proficient"
    })

    # Add the _other fields as null (used when value is 6+ or "other")
    logger.info("Adding persons_other, vehicles_other, workers_other as null")
    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("persons_other"),
        pl.lit(None).cast(pl.Utf8).alias("vehicles_other"),
        pl.lit(None).cast(pl.Utf8).alias("workers_other")
    ])

    # Add transfer route fields (BART 2024 doesn't have detailed transfer routing)
    logger.info("Adding transfer route fields as null")
    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("first_route_before_survey_board"),
        pl.lit(None).cast(pl.Utf8).alias("first_route_after_survey_alight"),
        pl.lit(None).cast(pl.Utf8).alias("second_route_before_survey_board"),
        pl.lit(None).cast(pl.Utf8).alias("second_route_after_survey_alight"),
        pl.lit(None).cast(pl.Utf8).alias("third_route_before_survey_board"),
        pl.lit(None).cast(pl.Utf8).alias("third_route_after_survey_alight"),
        pl.lit(None).cast(pl.Utf8).alias("fourth_route_before_survey_board"),
        pl.lit(None).cast(pl.Utf8).alias("fourth_route_after_survey_alight")
    ])

    # Use the actual PREV_TRANSFERS and NEXT_TRANSFERS fields
    logger.info("Mapping PREV_TRANSFERS and NEXT_TRANSFERS to number_transfers fields")
    bart_df = bart_df.with_columns([
        pl.col("PREV_TRANSFERS").alias("number_transfers_orig_board"),
        pl.col("NEXT_TRANSFERS").alias("number_transfers_alight_dest")
    ])

    # Transfers
    transfer_cols = [col for col in bart_df.columns if "transfer" in col.lower()]
    logger.info("Found transfer columns: %s", transfer_cols)

    # Create date_string from DATE_COMPLETED (modeling) or DATE_STARTED_SAS (SAS)
    # Format as MM/DD/YYYY for R script (which will derive day_of_the_week)
    logger.info("Creating date_string from DATE_COMPLETED or DATE_STARTED_SAS")
    bart_df = bart_df.with_columns([
        pl.coalesce([
            pl.col("DATE_COMPLETED").cast(pl.Utf8),
            pl.col("DATE_STARTED_SAS").dt.strftime("%m/%d/%Y")
        ]).alias("date_string")
    ])

    # Create time_string from SURVEY_END_TIME or SURVEY_START_TIME
    logger.info("Creating time_string from SURVEY_END_TIME or SURVEY_START_TIME")
    bart_df = bart_df.with_columns([
        pl.coalesce([
            pl.col("SURVEY_END_TIME").cast(pl.Utf8),
            pl.col("DATE_STARTED_SAS").dt.strftime("%H:%M:%S")
        ]).alias("time_string")
    ])

    # Add time_period from TIME_ON_fnl (survey strata)
    logger.info("Mapping TIME_ON_fnl to time_period")
    bart_df = bart_df.with_columns([
        pl.col("TIME_ON_fnl").cast(pl.Utf8).alias("time_period")
    ])

    # We don't have a depart_time field, so set as null
    logger.info("Adding depart_time, return_time, depart_hour, return_hour fields as null")
    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("depart_time"),
        pl.lit(None).cast(pl.Utf8).alias("return_time"),
        pl.lit(None).cast(pl.Utf8).alias("depart_hour"),
        pl.lit(None).cast(pl.Utf8).alias("return_hour")
    ])

    # Add origin/destination coordinates (BART 2024 doesn't collect trip O/D, only home/work/school)
    logger.info("Adding orig_lat, orig_lon, dest_lat, dest_lon as null")
    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Float64).alias("orig_lat"),
        pl.lit(None).cast(pl.Float64).alias("orig_lon"),
        pl.lit(None).cast(pl.Float64).alias("dest_lat"),
        pl.lit(None).cast(pl.Float64).alias("dest_lon")
    ])

    # Add metadata fields
    logger.info("Adding interview_language and survey_type fields")
    # Create lookup for SURVEY_LANGUAGE_FINAL
    survey_lang_lookup = create_codebook_lookup(codebook_df, "SURVEY_LANGUAGE_FINAL")

    bart_df = bart_df.with_columns([
        pl.col("SURVEY_LANGUAGE_FINAL").replace(survey_lang_lookup, default="Missing").alias("interview_language"),
        pl.when(pl.col("DATE_COMPLETED").is_not_null())
          .then(pl.lit("online - modeling platform"))
          .when(pl.col("DATE_STARTED_SAS").is_not_null())
          .then(pl.lit("online - SAS platform"))
          .otherwise(pl.lit("online"))
          .alias("survey_type")
    ])

    # Add fare fields (household_income and eng_proficient already renamed above)
    logger.info("Adding fare_category and fare_medium fields")

    # Decode fare_category from TYPE_OF_FARE using codebook
    fare_cat_lookup = create_codebook_lookup(codebook_df, "TYPE_OF_FARE")
    if fare_cat_lookup:
        bart_df = bart_df.with_columns([
            pl.col("TYPE_OF_FARE").replace_strict(
                fare_cat_lookup, default=None, return_dtype=pl.Utf8
            ).alias("fare_category")
        ])
    else:
        bart_df = bart_df.with_columns([
            pl.col("TYPE_OF_FARE").cast(pl.Utf8).alias("fare_category")
        ])

    # Standardize fare_category to match enum
    bart_df = bart_df.with_columns([
        pl.when(pl.col("fare_category").str.contains("Adult", literal=False))
          .then(pl.lit("Adult"))
          .when(pl.col("fare_category").str.contains("Youth", literal=False))
          .then(pl.lit("Youth"))
          .when(pl.col("fare_category").str.contains("Senior", literal=False))
          .then(pl.lit("Senior"))
          .when(pl.col("fare_category").str.contains("Disabled|RTC", literal=False))
          .then(pl.lit("RTC"))
          .when(pl.col("fare_category").str.contains("Student|Pass", literal=False))
          .then(pl.lit("Student"))
          .when(pl.col("fare_category").str.contains("Medicare", literal=False))
          .then(pl.lit("Medicare"))
          .when(pl.col("fare_category").str.contains("Free", literal=False))
          .then(pl.lit("Free"))
          .when(pl.col("fare_category").is_not_null())
          .then(pl.lit("Other"))
          .otherwise(pl.lit("Missing"))
          .alias("fare_category")
    ])

    bart_df = bart_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("fare_medium"),
        pl.lit(None).cast(pl.Utf8).alias("clipper_detail"),
        pl.lit(None).cast(pl.Utf8).alias("fare_medium_other"),
        pl.lit(None).cast(pl.Utf8).alias("fare_category_other")
    ])

    # Decode work_status and student_status from numeric to text
    logger.info("Decoding work_status and student_status from codebook")
    work_status_lookup = create_codebook_lookup(codebook_df, "EMPLOYMENT_STATUS_EDITED")
    student_status_lookup = create_codebook_lookup(codebook_df, "STUDENT_STATUS_EDITED")

    if work_status_lookup:
        bart_df = bart_df.with_columns([
            pl.col("work_status").replace_strict(
                work_status_lookup, default=None, return_dtype=pl.Utf8
            ).alias("work_status")
        ])

    if student_status_lookup:
        bart_df = bart_df.with_columns([
            pl.col("student_status").replace_strict(
                student_status_lookup, default=None, return_dtype=pl.Utf8
            ).alias("student_status")
        ])

    # Standardize work_status and student_status to match enum
    bart_df = bart_df.with_columns([
        pl.when(pl.col("work_status").str.contains("full time|part time", literal=False))
          .then(pl.lit("Full- or part-time"))
          .when(pl.col("work_status").str.contains("No Answer|Not employed", literal=False))
          .then(pl.lit("Non-worker"))
          .when(pl.col("work_status").is_not_null())
          .then(pl.lit("Other"))
          .otherwise(pl.lit("Missing"))
          .alias("work_status"),
        pl.when(pl.col("student_status").str.to_lowercase() == "yes")
          .then(pl.lit("Full- or part-time"))
          .when(pl.col("student_status").str.to_lowercase() == "no")
          .then(pl.lit("Non-student"))
          .otherwise(pl.lit("Missing"))
          .alias("student_status")
    ])

    # Decode household_income from HHI_EDITED using codebook
    hh_income_lookup = create_codebook_lookup(codebook_df, "HHI_EDITED")
    if hh_income_lookup:
        bart_df = bart_df.with_columns([
            pl.col("household_income").replace_strict(
                hh_income_lookup, default=None, return_dtype=pl.Utf8
            ).alias("household_income")
        ])

    # Convert household_income from text ranges to numeric (use midpoint)
    # Keep original string and mark as approximated
    bart_df = bart_df.with_columns([
        pl.col("household_income").alias("household_income_original"),  # Keep original string
        pl.when(pl.col("household_income").str.contains("Less than|Under", literal=False))
          .then(pl.lit(7500.0))  # "Less than $15,000" -> 7,500
        .when(pl.col("household_income").str.contains("15,000.*29,999", literal=False))
          .then(pl.lit(22500.0))
        .when(pl.col("household_income").str.contains("30,000.*49,999", literal=False))
          .then(pl.lit(40000.0))
        .when(pl.col("household_income").str.contains("50,000.*74,999", literal=False))
          .then(pl.lit(62500.0))
        .when(pl.col("household_income").str.contains("75,000.*99,999", literal=False))
          .then(pl.lit(87500.0))
        .when(pl.col("household_income").str.contains("100,000.*149,999", literal=False))
          .then(pl.lit(125000.0))
        .when(pl.col("household_income").str.contains("150,000.*199,999", literal=False))
          .then(pl.lit(175000.0))
        .when(pl.col("household_income").str.contains("200,000.*above|200,000.*over", literal=False))
          .then(pl.lit(250000.0))  # "$200,000 and above" -> 250,000
        .when(pl.col("household_income").is_not_null())
          .then(pl.col("household_income").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("household_income"),
        pl.when(pl.col("household_income").is_not_null())
          .then(pl.lit(True))
          .otherwise(pl.lit(False))
          .alias("income_approximated")
    ])


    # Fix survey_type to match enum
    logger.info("Normalizing survey_type values")
    bart_df = bart_df.with_columns([
        pl.when(pl.col("survey_type").str.contains("modeling"))
          .then(pl.lit("Online"))
          .when(pl.col("survey_type").str.contains("SAS"))
          .then(pl.lit("Online"))
          .when(pl.col("survey_type") == "online")
          .then(pl.lit("Online"))
          .otherwise(pl.col("survey_type"))
          .alias("survey_type")
    ])

    # Fix hispanic field - convert from 0/1 to enum text
    logger.info("Converting hispanic from 0/1 to enum text")
    bart_df = bart_df.with_columns([
        pl.when(pl.col("hispanic") == 1)
          .then(pl.lit("Hispanic/Latino or of Spanish origin"))
          .when(pl.col("hispanic") == 0)
          .then(pl.lit("Not Hispanic/Latino or of Spanish origin"))
          .otherwise(pl.lit("Missing"))
          .alias("hispanic")
    ])

    # Add weight fields
    logger.info("Adding weight field")
    bart_df = bart_df.with_columns([
        pl.col(TRIP_WEIGHT_FIELD).alias("trip_weight"),
        pl.col(BOARDING_WEIGHT_FIELD).alias("weight"),
        # R expects these for some annoying reason
        pl.lit(None, dtype=pl.Float64).alias("alt_weight"),
        pl.lit(None, dtype=pl.Float64).alias("tweight"),
    ])


    return bart_df


# Dictionary mappings are manually maintained in:
# ingestion/BART/BART_2024_dictionary_mappings.csv
# This includes both NONCATEGORICAL (ID, weight, etc.) and categorical (work_status, etc.) mappings


def preprocess(
    survey_path: str,
    station_geojson: str,
    canonical_operator: str,
    vehicle_tech: str,
    survey_year: int,
    fuzzy_match_threshold: int,
    operator_names: list[str],
) -> pl.DataFrame:
    """Preprocess BART 2024 survey data.
    
    Args:
        survey_path: Path to survey Excel file
        station_geojson: Path to station GeoJSON file
        canonical_operator: Canonical operator name (e.g., 'BART')
        vehicle_tech: Vehicle technology type (e.g., 'Heavy Rail')
        survey_year: Survey year
        fuzzy_match_threshold: Threshold for fuzzy station name matching
        operator_names: List of operator name variants for geocoding
        
    Returns:
        Preprocessed survey DataFrame ready for canonical pipeline ingestion.
    """
    logger.info("=" * 80)
    logger.info("BART 2024 Preprocessing Pipeline")
    logger.info("=" * 80)
    # Read data
    survey_df, codebook_df = read_survey_excel(survey_path)

    logger.info("\nInitial data shape: %s", survey_df.shape)
    logger.info("Initial columns: %s", len(survey_df.columns))

    logger.info("Loading station GeoJSON")
    stations_gdf = gpd.read_file(station_geojson)

    # Decode station codes to names using codebook
    logger.info("Decoding station codes to names")
    entry_lookup = create_codebook_lookup(codebook_df, ENTRY_STATION_FIELD)
    exit_lookup = create_codebook_lookup(codebook_df, EXIT_STATION_FIELD)

    survey_df = survey_df.with_columns([
        pl.col(ENTRY_STATION_FIELD).replace_strict(
            entry_lookup, default=None, return_dtype=pl.Utf8
        ).alias("entry_station_name"),
        pl.col(EXIT_STATION_FIELD).replace_strict(
            exit_lookup, default=None, return_dtype=pl.Utf8
        ).alias("exit_station_name"),
    ])

    # Geocode decoded station names to coordinates
    logger.info("Geocoding stations")
    stations_gdf = gpd.read_file(station_geojson)
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
        operator_names=operator_names,
        stop_name_field="stop_name",
        agency_field="agency",
        fuzzy_threshold=fuzzy_match_threshold,
    )

    # Use existing home coordinates from survey
    logger.info("Renaming home coordinate columns")
    survey_df = survey_df.rename({
        HOME_ADDRESS_LAT: "home_lat",
        HOME_ADDRESS_LONG: "home_lon",
    })
    # Add geo_level for home locations
    survey_df = survey_df.with_columns([
        pl.when(pl.col("home_lat").is_not_null())
            .then(pl.lit("address"))
            .otherwise(None)
            .alias("home_geo_level")
    ])

    # Process access/egress
    survey_df = process_access_egress(survey_df, codebook_df)

    # Process demographics
    survey_df = process_demographics(survey_df, codebook_df, survey_year)

    # Process trip characteristics
    survey_df = process_trip_characteristics(survey_df, codebook_df)

    # Add metadata
    logger.info("Adding metadata columns")
    survey_df = survey_df.with_columns([
        pl.lit(canonical_operator).alias("canonical_operator"),
        pl.lit(vehicle_tech).alias("vehicle_tech"),  # Use correct schema field name
        pl.lit(survey_year).alias("survey_year"),
        pl.lit(canonical_operator).alias("survey_name"),
        pl.lit(canonical_operator).alias("operator"),
    ])

    # Add route field (BART doesn't have routes in traditional sense, use line or null)
    logger.info("Adding route field")
    survey_df = survey_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("route")
    ])

    # Add direction field (BART doesn't have traditional direction data)
    logger.info("Adding direction field")
    survey_df = survey_df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("direction")
    ])

    # Add survey_board and survey_alight (station names from geocoding)
    logger.info("Adding survey_board and survey_alight")
    survey_df = survey_df.with_columns([
        pl.col("survey_board_station").alias("survey_board"),
        pl.col("survey_alight_station").alias("survey_alight")
    ])

    # Add first_board and last_alight coordinates
    # (same as survey_board/survey_alight for single-leg trips)
    logger.info("Adding first_board and last_alight coordinates")
    survey_df = survey_df.with_columns([
        pl.col("survey_board_lat").alias("first_board_lat"),
        pl.col("survey_board_lon").alias("first_board_lon"),
        pl.col("survey_alight_lat").alias("last_alight_lat"),
        pl.col("survey_alight_lon").alias("last_alight_lon"),
    ])

    # Format the ID columns
    # pipeline expects "ID" to be unique within the survey
    # thus this combined survey will need:
    #   ID -> subsurvey_ID
    #   UNIQUE_IDENTIFIER -> ID
    survey_df = survey_df.rename(
        {
            "ID": "subsurvey_id",
            "UNIQUE_IDENTIFIER": "ID"
        })

    # Add missing required fields for schema validation
    logger.info("Adding missing required schema fields...")
    survey_df = survey_df.with_columns([
        # survey_id and original_id (will be set properly in ingest.py)
        pl.col("ID").alias("survey_id"),  # Placeholder, will be overwritten
        pl.col("ID").alias("original_id"),

        # Direction - set to Missing for BART (no traditional direction)
        pl.lit("Missing").alias("direction") if "direction" in survey_df.columns and survey_df["direction"].null_count() > 0 else pl.col("direction"),

        # Access/egress modes - set null to "Missing"
        pl.when(pl.col("access_mode").is_null()).then(pl.lit("Missing")).otherwise(pl.col("access_mode")).alias("access_mode"),
        pl.when(pl.col("egress_mode").is_null()).then(pl.lit("Missing")).otherwise(pl.col("egress_mode")).alias("egress_mode"),
        pl.lit("Missing").alias("immediate_access_mode"),
        pl.lit("Missing").alias("immediate_egress_mode"),

        # Transfer fields
        pl.lit("Missing").alias("transfer_from"),
        pl.lit("Missing").alias("transfer_to"),

        # Transfer operator/technology fields (before survey)
        pl.lit(None).cast(pl.Utf8).alias("first_before_operator"),
        pl.lit(None).cast(pl.Utf8).alias("first_before_operator_detail"),
        pl.lit("Missing").alias("first_before_technology"),
        pl.lit(None).cast(pl.Utf8).alias("second_before_operator"),
        pl.lit(None).cast(pl.Utf8).alias("second_before_operator_detail"),
        pl.lit("Missing").alias("second_before_technology"),
        pl.lit(None).cast(pl.Utf8).alias("third_before_operator"),
        pl.lit(None).cast(pl.Utf8).alias("third_before_operator_detail"),
        pl.lit("Missing").alias("third_before_technology"),

        # Transfer operator/technology fields (after survey)
        pl.lit(None).cast(pl.Utf8).alias("first_after_operator"),
        pl.lit(None).cast(pl.Utf8).alias("first_after_operator_detail"),
        pl.lit("Missing").alias("first_after_technology"),
        pl.lit(None).cast(pl.Utf8).alias("second_after_operator"),
        pl.lit(None).cast(pl.Utf8).alias("second_after_operator_detail"),
        pl.lit("Missing").alias("second_after_technology"),
        pl.lit(None).cast(pl.Utf8).alias("third_after_operator"),
        pl.lit(None).cast(pl.Utf8).alias("third_after_operator_detail"),
        pl.lit("Missing").alias("third_after_technology"),

        # First/last board technology
        pl.lit("Heavy Rail").alias("first_board_tech"),
        pl.lit("Heavy Rail").alias("last_alight_tech"),

        # Path labels (not yet derived)
        pl.lit(None).cast(pl.Utf8).alias("path_access"),
        pl.lit(None).cast(pl.Utf8).alias("path_egress"),
        pl.lit(None).cast(pl.Utf8).alias("path_line_haul"),
        pl.lit(None).cast(pl.Utf8).alias("path_label"),

        # Trip purposes - set null to "Missing"
        pl.when(pl.col("orig_purp").is_null()).then(pl.lit("Missing")).otherwise(pl.col("orig_purp")).alias("orig_purp"),
        pl.when(pl.col("dest_purp").is_null()).then(pl.lit("Missing")).otherwise(pl.col("dest_purp")).alias("dest_purp"),

        # On/off station fields
        pl.col("survey_board").alias("onoff_enter_station"),
        pl.col("survey_alight").alias("onoff_exit_station"),

        # survey_time field (combine date_string and time_string)
        (pl.col("date_string").cast(pl.Utf8) + " " + pl.col("time_string").cast(pl.Utf8)).alias("survey_time"),

        # day_of_the_week (derive from date_string)
        pl.when(pl.col("date_string").is_not_null())
          .then(pl.col("date_string").str.to_date("%m/%d/%Y").dt.weekday().cast(pl.Utf8)
            .replace_strict({
              "1": "Monday", "2": "Tuesday", "3": "Wednesday",
              "4": "Thursday", "5": "Friday", "6": "Saturday", "7": "Sunday"
            }, default="Missing", return_dtype=pl.Utf8))
          .otherwise(pl.lit("Missing"))
          .alias("day_of_the_week"),

        # approximate_age (calculate from year_born_four_digit)
        (survey_year - pl.col("year_born_four_digit")).alias("approximate_age"),

        # field_language (use same as interview_language)
        pl.col("interview_language").alias("field_language"),

        # race field (combine race_cat or use Missing)
        pl.when(pl.col("race_cat").is_not_null()).then(pl.col("race_cat")).otherwise(pl.lit("Missing")).alias("race"),

        # language_at_home (use language_at_home_binary)
        pl.when(pl.col("language_at_home_binary").is_not_null()).then(pl.col("language_at_home_binary")).otherwise(pl.lit("Missing")).alias("language_at_home"),
    ])

    # Convert eng_proficient from int to string enum
    survey_df = survey_df.with_columns([
        pl.when(pl.col("eng_proficient").cast(pl.Utf8, strict=False) == "1")
          .then(pl.lit("Very well"))
        .when(pl.col("eng_proficient").cast(pl.Utf8, strict=False) == "2")
          .then(pl.lit("Well"))
        .when(pl.col("eng_proficient").cast(pl.Utf8, strict=False) == "3")
          .then(pl.lit("Not well"))
        .when(pl.col("eng_proficient").cast(pl.Utf8, strict=False) == "4")
          .then(pl.lit("Not at all"))
        .when(pl.col("eng_proficient").is_null())
          .then(pl.lit("Missing"))
        .otherwise(pl.col("eng_proficient").cast(pl.Utf8))
        .alias("eng_proficient")
    ])

    # Sanitize the comment field to remove bad characters
    survey_df = survey_df.with_columns(
        pl.col("COMMENT")
        .cast(pl.Utf8)
        .str.replace_all("\x00", "")
        .str.replace_all("\r\n", "\n")
        .str.replace_all("\r", "\n")
        .str.replace_all("\n", r"\n")
    )

    logger.info("\n%s", "=" * 80)
    logger.info("Preprocessing complete!")
    logger.info("Records: %s", f"{len(survey_df):,}")
    logger.info("Columns: %s", len(survey_df.columns))
    logger.info("%s", "=" * 80)

    return survey_df


if __name__ == "__main__":
    # Debugging configuration - mirrors parameters in ingest.py
    # These values are used when running preprocessing.py directly for testing

    # Paths
    SURVEY_PATH = (
        r"M:/Data/OnBoard/Data and Reports/BART/"
        r"2024_StationProfileV1_NewWeights_ReducedVariables.xlsx"
    )

    STATION_GEOJSON = (
        r"M:/Data/OnBoard/Data and Reports/Geography Files/"
        r"cdot_ca_transit_stops_4312132402745178866.geojson"
    )

    # Replace letter drives with UNC paths for compatibility
    net_paths = {
        "M:": r"\\models.ad.mtc.ca.gov\data\models"
    }

    for drive, unc_path in net_paths.items():
        SURVEY_PATH = SURVEY_PATH.replace(drive, unc_path)
        STATION_GEOJSON = STATION_GEOJSON.replace(drive, unc_path)

    # High-level configuration constants
    CANONICAL_OPERATOR = "BART"
    VEHICLE_TECH = "Heavy Rail"
    SURVEY_YEAR = 2024
    FUZZY_MATCH_THRESHOLD = 80
    OPERATOR_NAMES = ["BART", "Bay Area Rapid Transit"]

    # Run preprocessing for debugging
    preprocess(
        survey_path=SURVEY_PATH,
        station_geojson=STATION_GEOJSON,
        canonical_operator=CANONICAL_OPERATOR,
        vehicle_tech=VEHICLE_TECH,
        survey_year=SURVEY_YEAR,
        fuzzy_match_threshold=FUZZY_MATCH_THRESHOLD,
        operator_names=OPERATOR_NAMES,
    )

