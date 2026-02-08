"""Preprocess BART 2015 survey data for standardization pipeline.

Converts BART 2015 vendor CSV to CoreSurveyResponse format, replicating
legacy Build_Standard_Database.R transformations for validation.

Source file: BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv
Dictionary: archive/make-uniform/production/Dictionary_for_Standard_Database.csv (BART,2015)
"""  # noqa: E501

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Constants
BART_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\BART\As CSV")
BART_CSV = (
    BART_DIR
    / "BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv"  # noqa: E501
)

# BART 2015 used Yes/No for race dummy variables
RACE_YES_NO_MAPPING = {"Yes": 1, "No": 0}


def infer_transfer_technology(operator: str | None, route: str | None) -> str | None:  # noqa: PLR0911
    """Infer technology from operator name and route description.

    BART 2015 transfer routes are not in canonical crosswalk (built from 2017-2018 surveys).
    This function infers technology based on operator names and route keywords.

    Args:
        operator: Transfer operator name
        route: Transfer route description

    Returns:
        Technology type (Heavy Rail, Light Rail, Commuter Rail, Ferry, Express Bus, Local Bus)
        or None if cannot determine
    """
    if operator is None or route is None:
        return None

    operator = str(operator).lower()
    route = str(route).lower()

    # Check route description for explicit technology keywords
    if "light rail" in route:
        return "Light Rail"
    if "ferry" in operator or "ferry" in route:
        return "Ferry"

    # Map operators to technology
    if "bart" in operator:
        return "Heavy Rail"
    if "caltrain" in operator or "ace" in operator or "amtrak" in operator:
        return "Commuter Rail"
    if "muni" in operator and any(
        line in route for line in ["j ", "k ", "l ", "m ", "n ", "t ", "s ", "f "]
    ):
        return "Light Rail"
    if "vta" in operator and "light rail" in route:
        return "Light Rail"
    if "golden gate transit" in operator or "greyhound" in operator:
        return "Express Bus"  # Intercounty service
    if any(
        bus_op in operator
        for bus_op in [
            "ac transit",
            "muni",
            "samtrans",
            "county connection",
            "soltrans",
            "wheels",
            "lavta",
            "tri delta",
            "westcat",
            "union city transit",
            "emery go-round",
            "fairfield",
            "suisun",
            "fast",
            "vacaville",
            "shuttle",
            "vta",
            "santa clara",
        ]
    ):
        return "Local Bus"

    # Default to Local Bus for unrecognized operators (most transfers are local bus)
    return "Local Bus"


def preprocess(
    survey_path: str | Path = BART_CSV,
    *,
    canonical_operator: str,
    survey_year: int,
) -> pl.DataFrame:
    """Preprocess BART 2015 survey data to CoreSurveyResponse format.

    Args:
        survey_path: Path to BART 2015 CSV file
        canonical_operator: Canonical operator name (should be "BART")
        survey_year: Survey year (should be 2015)

    Returns:
        Polars DataFrame in CoreSurveyResponse format
    """
    logger.info("Loading BART 2015 survey from %s", survey_path)

    # Read CSV with proper null handling
    df = pl.read_csv(
        survey_path,
        null_values=[
            "",
            "NA",
            "N/A",
            "Missing",
            "Unknown",
            "Missing - Respondent Refusal",
            "Missing - Question Not Asked",
            "Missing - Dummy Record",
            "Missing - Skip Logic",
            "Missing - Uncodeable",
            "Missing - Interviewer",
            "Missing - Geocoding",
            "Missing - Other",
        ],
        infer_schema_length=50000,  # Increase to handle large file
    )

    logger.info("Loaded %s raw records with %s columns", f"{len(df):,}", len(df.columns))

    # ===================
    # Core identifiers
    # ===================
    df = df.with_columns(
        [
            pl.col("ID").cast(pl.Int64).cast(pl.Utf8).alias("original_id"),
            # Generate response_id in legacy format: ID___OPERATOR___YEAR
            (
                pl.col("ID").cast(pl.Int64).cast(pl.Utf8)
                + "___"
                + pl.lit(canonical_operator)
                + "___"
                + pl.lit(str(survey_year))
            ).alias("response_id"),
            pl.lit(f"{canonical_operator}_{survey_year}").alias("survey_id"),
            pl.lit(canonical_operator).alias("operator"),
            pl.lit(survey_year).cast(pl.Int32).alias("year"),
            pl.lit("Heavy Rail").alias("vehicle_tech"),  # BART is heavy rail
        ]
    )

    # ===================
    # Survey metadata
    # ===================
    # Convert date completed to standardized format
    df = df.with_columns(
        [
            pl.col("DATE_COMPLETED").str.strptime(pl.Date, "%m/%d/%Y").alias("survey_date"),
            pl.col("PAIRED_WGHT_FCTR").cast(pl.Float64).alias("weight"),
        ]
    )

    # Derive day_of_the_week from survey_date
    df = df.with_columns(
        [
            pl.col("survey_date").dt.strftime("%A").alias("day_of_the_week"),
        ]
    )

    # ===================
    # Demographics
    # ===================
    # Age from COMPUTEDAGE
    df = df.with_columns(
        [
            pl.col("COMPUTEDAGE").cast(pl.Int32).alias("age"),
        ]
    )

    # Gender mapping (RESP_GENDER_CODE) - Note: codes are reversed from typical
    gender_mapping = {
        2: "Female",  # BART 2015 uses 2=female
        1: "Male",  # BART 2015 uses 1=male
        995: None,  # Missing
    }
    df = df.with_columns(
        [
            pl.col("RESP_GENDER_CODE").replace_strict(gender_mapping, default=None).alias("gender"),
        ]
    )

    # Race dummy variables - BART 2015 uses Yes/No format
    # RACE_OR_ETHNICITY_ASIAN -> race_dmy_asn
    # RACE_OR_ETHNICITY_BLACK -> race_dmy_blk
    # RACE_OR_ETHNICITY_HAWPI -> race_dmy_hwi
    # RACE_OR_ETHNICITY_AMALA -> race_dmy_ind (American Indian/Alaska Native)
    # RACE_OR_ETHNICITY_WHITE -> race_dmy_wht
    # HISP_LATINO_SPANISH_CODE -> hispanic (YES/NO codes)
    hispanic_mapping = {
        "YES": 1,
        "NO": 0,
    }
    df = df.with_columns(
        [
            pl.col("RACE_OR_ETHNICITY_ASIAN")
            .replace_strict(RACE_YES_NO_MAPPING, default=0)
            .cast(pl.Int8)
            .alias("race_dmy_asn"),
            pl.col("RACE_OR_ETHNICITY_BLACK")
            .replace_strict(RACE_YES_NO_MAPPING, default=0)
            .cast(pl.Int8)
            .alias("race_dmy_blk"),
            pl.col("RACE_OR_ETHNICITY_HAWPI")
            .replace_strict(RACE_YES_NO_MAPPING, default=0)
            .cast(pl.Int8)
            .alias("race_dmy_hwi"),
            pl.col("RACE_OR_ETHNICITY_AMALA")
            .replace_strict(RACE_YES_NO_MAPPING, default=0)
            .cast(pl.Int8)
            .alias("race_dmy_ind"),
            pl.col("RACE_OR_ETHNICITY_WHITE")
            .replace_strict(RACE_YES_NO_MAPPING, default=0)
            .cast(pl.Int8)
            .alias("race_dmy_wht"),
            pl.col("HISP_LATINO_SPANISH_CODE")
            .replace_strict(hispanic_mapping, default=0)
            .cast(pl.Int8)
            .alias("hispanic"),
        ]
    )

    # Income - will be handled by categorical income derivation in pipeline
    # HH_INCOME column exists but is processed by pipeline

    # Employment status (EMPLOYMENT_STATUS text field)
    employment_mapping = {
        "Yes": "Full- or part-time",
        "No": "Non-worker",
    }
    df = df.with_columns(
        [
            pl.col("EMPLOYMENT_STATUS")
            .replace_strict(employment_mapping, default=None)
            .alias("work_status"),
        ]
    )

    # Student status (STUDENT_STATUS text field)
    student_mapping = {
        "Yes - Campus": "Full- or part-time",
        "Yes - Online Only": "Full- or part-time",
        "No": "Non-student",
    }
    df = df.with_columns(
        [
            pl.col("STUDENT_STATUS")
            .replace_strict(student_mapping, default=None)
            .alias("student_status"),
        ]
    )

    # ===================
    # Household
    # ===================
    # Vehicles (VEH_IN_HH) - text field: "None", "1", "2", "3", "4", "5", "6 or more"
    vehicle_mapping = {
        "None": 0,
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6 or more": 6,
    }
    df = df.with_columns(
        [
            pl.col("VEH_IN_HH").replace_strict(vehicle_mapping, default=None).alias("vehicles"),
        ]
    )

    # Workers (EMPLOYED_IN_HH_CODE) - numeric codes 0-6 for workers
    df = df.with_columns(
        [
            pl.col("EMPLOYED_IN_HH_CODE").cast(pl.Int32).alias("workers"),
        ]
    )

    # Language (SRVY_IN_ENGL_SPANISH) - interview language
    df = df.with_columns(
        [
            pl.when(pl.col("SRVY_IN_ENGL_SPANISH") == "English")
            .then(pl.lit("ENGLISH ONLY"))
            .otherwise(None)
            .alias("language"),
        ]
    )

    # ===================
    # Geography
    # ===================
    # Home location from OR_ADDRESS_* fields
    # Cast with strict=False to handle non-numeric values
    df = df.with_columns(
        [
            pl.col("OR_ADDRESS_LAT").cast(pl.Float64, strict=False).alias("home_lat"),
            pl.col("OR_ADDRESS_LONG").cast(pl.Float64, strict=False).alias("home_lon"),
            pl.col("OR_ADDRESS_ZIP").cast(pl.Utf8).alias("home_zip"),
        ]
    )

    # Destination location from DE_ADDRESS_* fields
    df = df.with_columns(
        [
            pl.col("DE_ADDRESS_LAT").cast(pl.Float64, strict=False).alias("dest_lat"),
            pl.col("DE_ADDRESS_LONG").cast(pl.Float64, strict=False).alias("dest_lon"),
            pl.col("DE_ADDRESS_ZIP").cast(pl.Utf8).alias("dest_zip"),
        ]
    )

    # ===================
    # Trip characteristics
    # ===================
    # Survey time (departure time from origin)
    # TIME_COMPLETED is in "H:MM:SS AM/PM" format - convert to string HH:MM
    df = df.with_columns(
        [
            pl.col("TIME_COMPLETED")
            .str.strptime(pl.Time, "%I:%M:%S %p")
            .cast(pl.Utf8)  # Cast to string for pipeline
            .alias("survey_time"),
        ]
    )

    # Access mode (ACCESS_MODE)
    # BART 2015 Dictionary maps ACCESS_MODE text values to access_mode categories
    access_mode_mapping = {
        "Bicycle": "Bike",
        "A bicycle": "Bike",  # Variant with "A " prefix
        "Motorcycle/motorized scooter": "PNR",
        "A motorcycle/motorized scooter": "PNR",  # Variant with "A " prefix
        "Ridesharing (Uber, Lyft, etc.)": "TNC",
        "Was dropped off by someone going someplace else": "KNR",
        "Taxi": "TNC",
        "A Taxi": "TNC",  # Variant with "A " prefix
        "Drove alone and parked": "PNR",
        "Drove with others/carpooled and parked": "PNR",
        "Walked (includes wheelchair, skateboard)": "Walk",
        "Walked all the way to BART (includes wheelchair, skateboard)": "Walk",
        "Walked (OAK)": "Walk",  # Oakland airport variant
        "Bus, train, or other public transit (includes ferry, paratransit, shuttle)": "Transit",
    }
    df = df.with_columns(
        [
            pl.col("ACCESS_MODE")
            .replace_strict(access_mode_mapping, default=None)
            .alias("access_mode"),
        ]
    )

    # Egress mode (EGRESS_MODE)
    egress_mode_mapping = {
        # Bicycle variants
        "A bicycle": "Bike",
        "Bicycle": "Bike",
        # Motorcycle variants
        "A motorcycle/motorized scooter": "PNR",
        "Motorcycle/motorized scooter": "PNR",
        # Pick-up variants (Kiss-and-Ride)
        "Get picked up": "KNR",
        "Will be picked up by someone going someplace else": "KNR",
        # Taxi/TNC variants - following ACE convention, Taxi → KNR, Rideshare → TNC
        "A Taxi": "KNR",
        "Taxi": "KNR",
        "Rideshare (Uber, Lyft)": "TNC",
        "Ridesharing (Uber, Lyft, etc.)": "TNC",
        # Drive variants (Park-and-Ride)
        "Drive alone": "PNR",
        "Drive with others/carpool": "PNR",
        # Walk variants
        "Walk (OAK)": "Walk",
        "Walk (includes wheelchair, skateboard)": "Walk",
        "Walk all the way from BART (includes wheelchair, skateboard)": "Walk",
        # Transit
        "Bus, train, or other public transit (includes ferry, paratransit, shuttle)": "Transit",
    }
    df = df.with_columns(
        [
            pl.col("EGRESS_MODE")
            .replace_strict(egress_mode_mapping, default=None)
            .alias("egress_mode"),
        ]
    )

    # Route - For BART, route is "BART___<enter_station>&&&<exit_station>"
    # This is constructed from FIRST_ENTERED_BART and BART_EXIT_STATION
    # Keep original station names for onoff fields (match legacy database as-is)
    df = df.with_columns(
        [
            pl.col("FIRST_ENTERED_BART").cast(pl.Utf8).alias("onoff_enter_station"),
            pl.col("BART_EXIT_STATION").cast(pl.Utf8).alias("onoff_exit_station"),
        ]
    )

    # Build route string: "BART___<enter>&&&<exit>"
    # Normalize station names for canonical format:
    # - Remove periods: "St." → "St"
    # - Replace "/UN " with " UN " for "Civic Center/UN Plaza"
    df = df.with_columns(
        [
            (
                pl.lit("BART___")
                + pl.col("onoff_enter_station")
                .str.replace_all(r"\.", "", literal=False)  # Remove all periods
                .str.replace_all("/UN ", " UN ", literal=True)  # Fix slash before UN
                + pl.lit("&&&")
                + pl.col("onoff_exit_station")
                .str.replace_all(r"\.", "", literal=False)  # Remove all periods
                .str.replace_all("/UN ", " UN ", literal=True)  # Fix slash before UN
            ).alias("route"),
        ]
    )

    # ===================
    # Transfer routes
    # ===================
    # Map BART transfer columns to pipeline format
    # Pipeline expects specific column names for route/operator pairs
    # Access transfers: *_route_before_survey_board
    # Egress transfers: *_route_after_survey_alight
    df = df.with_columns(
        [
            # Access transfers (before BART)
            pl.col("ACCESSTRNSFR_LIST1").alias("first_before_route_before_survey_board"),
            pl.col("ACCESS_TRNSFR_LIST1AGENCY").str.to_uppercase().alias("first_before_operator"),
            pl.col("ACCESSTRNSFR_LIST2").alias("second_before_route_before_survey_board"),
            pl.col("ACCESS_TRNSFR_LIST2AGENCY").str.to_uppercase().alias("second_before_operator"),
            # Egress transfers (after BART)
            pl.col("EGRESS_TRNSFR_LIST1").alias("first_after_route_after_survey_alight"),
            pl.col("EGRESS_TRNSFR_LIST1_AGENCY").str.to_uppercase().alias("first_after_operator"),
            pl.col("EGRESS_TRNSFR_LIST2").alias("second_after_route_after_survey_alight"),
            pl.col("EGRESS_TRNSFR_LIST2_AGENCY").str.to_uppercase().alias("second_after_operator"),
            pl.col("EGRESSTRNSFR_LIST3").alias("third_after_route_after_survey_alight"),
            pl.col("EGRESSTRNSFR_LIST3_AGENCY").str.to_uppercase().alias("third_after_operator"),
        ]
    )

    # Assign transfer technologies based on operator names and route descriptions
    # BART 2015 routes not in canonical crosswalk, so infer from operator
    df = df.with_columns(
        [
            pl.struct(["first_before_operator", "first_before_route_before_survey_board"])
            .map_elements(
                lambda x: infer_transfer_technology(
                    x["first_before_operator"], x["first_before_route_before_survey_board"]
                ),
                return_dtype=pl.Utf8,
            )
            .alias("first_before_technology"),
            pl.struct(["second_before_operator", "second_before_route_before_survey_board"])
            .map_elements(
                lambda x: infer_transfer_technology(
                    x["second_before_operator"], x["second_before_route_before_survey_board"]
                ),
                return_dtype=pl.Utf8,
            )
            .alias("second_before_technology"),
            pl.lit(None)
            .cast(pl.Utf8)
            .alias("third_before_technology"),  # BART 2015 has no third_before
            pl.struct(["first_after_operator", "first_after_route_after_survey_alight"])
            .map_elements(
                lambda x: infer_transfer_technology(
                    x["first_after_operator"], x["first_after_route_after_survey_alight"]
                ),
                return_dtype=pl.Utf8,
            )
            .alias("first_after_technology"),
            pl.struct(["second_after_operator", "second_after_route_after_survey_alight"])
            .map_elements(
                lambda x: infer_transfer_technology(
                    x["second_after_operator"], x["second_after_route_after_survey_alight"]
                ),
                return_dtype=pl.Utf8,
            )
            .alias("second_after_technology"),
            pl.struct(["third_after_operator", "third_after_route_after_survey_alight"])
            .map_elements(
                lambda x: infer_transfer_technology(
                    x["third_after_operator"], x["third_after_route_after_survey_alight"]
                ),
                return_dtype=pl.Utf8,
            )
            .alias("third_after_technology"),
        ]
    )

    # ===================
    # Trip purpose
    # ===================
    # Origin purpose from OR_PLACE_TYPE_CODE_FINAL (per Dictionary, title case for enums)
    origin_purpose_mapping = {
        1: "Home",
        2: "Work",
        3: "Work-related",
        4: "College",
        5: "School",
        6: "Shopping",
        7: "Other maintenance",
        8: "Eat out",
        9: "Other discretionary",
        10: "Social recreation",
        11: "Other maintenance",
        12: "Other maintenance",
        13: "Other discretionary",
        14: "Other discretionary",
        15: "Escorting",
        16: "Other discretionary",
        17: "Other maintenance",
    }
    df = df.with_columns(
        [
            pl.col("OR_PLACE_TYPE_CODE_FINAL")
            .replace_strict(origin_purpose_mapping, default=None)
            .alias("orig_purp"),
        ]
    )

    # Destination purpose from DE_PLACE_TYPE_CODE_FINAL (per Dictionary, title case for enums)
    dest_purpose_mapping = {
        1: "Home",
        2: "Work",
        3: "Work-related",
        4: "College",
        5: "School",
        6: "Shopping",
        7: "Other maintenance",
        8: "Eat out",
        9: "Other discretionary",
        10: "Social recreation",
        11: "Other maintenance",
        12: "Other maintenance",
        13: "Other discretionary",
        14: "Other discretionary",
        15: "Escorting",
        16: "Other discretionary",
        17: "Other maintenance",
    }
    df = df.with_columns(
        [
            pl.col("DE_PLACE_TYPE_CODE_FINAL")
            .replace_strict(dest_purpose_mapping, default=None)
            .alias("dest_purp"),
        ]
    )

    # School before/after survey trip
    school_before_mapping = {
        "Yes": "at school before surveyed trip",
        "No": "not at school before surveyed trip",
    }
    school_after_mapping = {
        "Yes": "at school after surveyed trip",
        "No": "not at school after surveyed trip",
    }

    df = df.with_columns(
        [
            pl.col("BEEN_2SCHOOL_TODAY")
            .replace_strict(school_before_mapping, default=None)
            .alias("at_school_prior_to_orig_purp"),
            pl.col("WILL_GO2SCHOOL_TODAY")
            .replace_strict(school_after_mapping, default=None)
            .alias("at_school_after_dest_purp"),
        ]
    )

    # ===================
    # Fare and payment
    # ===================
    # Fare type from BART_TICKET
    # Clipper card type from CLIPPER_REG_ADULT_CODE

    # ===================
    # Final validation and cleanup
    # ===================
    logger.info("Preprocessing complete: %s records", f"{len(df):,}")

    # Select only CoreSurveyResponse fields that we've mapped
    core_fields = [
        "response_id",
        "survey_id",
        "original_id",
        "operator",
        "year",
        "vehicle_tech",
        "survey_date",
        "day_of_the_week",
        "survey_time",
        "weight",
        "age",
        "gender",
        "race_dmy_asn",
        "race_dmy_blk",
        "race_dmy_hwi",
        "race_dmy_ind",
        "race_dmy_wht",
        "hispanic",
        "work_status",
        "student_status",
        "vehicles",
        "workers",
        "language",
        "home_lat",
        "home_lon",
        "home_zip",
        "dest_lat",
        "dest_lon",
        "dest_zip",
        "access_mode",
        "egress_mode",
        "boardings",
        "route",
        "onoff_enter_station",
        "onoff_exit_station",
        "orig_purp",
        "dest_purp",
        "at_school_prior_to_orig_purp",
        "at_school_after_dest_purp",
        # Transfer routes for boardings calculation
        "first_before_route_before_survey_board",
        "first_before_operator",
        "first_before_technology",
        "second_before_route_before_survey_board",
        "second_before_operator",
        "second_before_technology",
        "third_before_technology",
        "first_after_route_after_survey_alight",
        "first_after_operator",
        "first_after_technology",
        "second_after_route_after_survey_alight",
        "second_after_operator",
        "second_after_technology",
        "third_after_route_after_survey_alight",
        "third_after_operator",
        "third_after_technology",
    ]

    # Only select fields that exist in df
    available_fields = [f for f in core_fields if f in df.columns]
    df = df.select(available_fields)

    logger.info("Selected %s core fields for pipeline", len(available_fields))

    return df


if __name__ == "__main__":
    # Test preprocessing locally
    logging.basicConfig(level=logging.INFO)
    df = preprocess(canonical_operator="BART", survey_year=2015)
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"Columns: {df.columns}")
