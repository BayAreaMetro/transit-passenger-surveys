"""Backfill the data lake with historical survey data from standardized CSV files.

This script reads survey_combined.csv from the most recent standardized data directory,
validates the data against the Pydantic schema, and writes Parquet files partitioned
by operator and year to the data lake.

Does not currently ingest survey_decomposition.csv, and ancillary_variables.csv.

This script contains all the data cleaning and normalization logic needed to ensure
the imported data conforms to the database schema. It is very naughty to have this much
logic outside of the main library, but this is a one-time backfill operation so it's
acceptable. Thanks Claude.

"""
# ruff: noqa: C901, PLR0912, PLR0915, PLR0911


import logging
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools import db
from transit_passenger_tools.data_models import SurveyMetadata, SurveyResponse, SurveyWeight
from transit_passenger_tools.pipeline import auto_sufficiency

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

# Date format lengths
SHORT_DATE_LENGTH = 8  # MM-DD-YY format
ACRONYM_MIN_LENGTH = 2  # Minimum length for acronym detection
ACRONYM_MAX_LENGTH = 4  # Maximum length for acronym detection
MAX_SAMPLE_LOCATIONS = 3  # Number of sample locations to show in error messages

# ============================================================================
# SHARED MAPPING DICTIONARIES
# Defined once at module level to avoid duplication
# ============================================================================

# Word-to-number conversion for numeric fields (persons, workers, vehicles)
WORD_TO_NUMBER = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
    "twenty-one": 21, "twenty-two": 22, "twenty-three": 23, "twenty-four": 24, "twenty-five": 25,
    "twenty-six": 26, "twenty-seven": 27, "twenty-eight": 28, "twenty-nine": 29, "thirty": 30,
    "none": 0, "other": None, "DON'T KNOW": None, "Ref": None, "missing": None,
    "four or more": 4, "five or more": 5, "six or more": 6, "ten or more": 10
}

# Language normalization (ALL CAPS → Title Case) - shared by field_language and interview_language
LANGUAGE_MAPPINGS = {
    "CHINESE": "Chinese", "ENGLISH": "English", "SPANISH": "Spanish", "OTHER": "Other",
    "FRENCH": "French", "KOREAN": "Korean", "RUSSIAN": "Russian", "ARABIC": "Arabic",
    "PUNJABI": "Punjabi", "CANTONESE": "Cantonese", "TAGALOG": "Tagalog",
    "TAGALOG/FILIPINO": "Tagalog", "Tagalog/Filipino": "Tagalog",
    "VIETNAMESE": "Vietnamese", "DONT KNOW/REFUSE": None,
    "Skip - Paper Survey": "Skip - Paper Survey", "Skip - paper survey": "Skip - Paper Survey"
}

# Access/Egress mode fixes - shared by both fields
ACCESS_EGRESS_MODE_FIXES = {
    "Tnc": "TNC",
    "Missing - dummy record": None,
    "Missing - dummy Record": None,
    "Unknown": "Other",
    "Missing - question not asked": None,
    "Missing - question Not Asked": None,
}

# Path to standardized data
STANDARDIZED_DATA_PATH = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized"
)

def find_latest_standardized_dir() -> Path:
    """Find the most recent standardized data directory."""
    dirs = [d for d in STANDARDIZED_DATA_PATH.iterdir()
            if d.is_dir() and d.name.startswith("standardized_")]
    if not dirs:
        msg = f"No standardized directories found in {STANDARDIZED_DATA_PATH}"
        raise FileNotFoundError(msg)

    latest = sorted(dirs, key=lambda x: x.name)[-1]
    logger.info("Found latest standardized directory: %s", latest.name)
    return latest


def _convert_hispanic_to_bool(df: pl.DataFrame) -> pl.DataFrame:
    """Convert hispanic enum strings to boolean."""
    if "hispanic" not in df.columns:
        return df
    
    return df.with_columns([
        pl.when(pl.col("hispanic").str.to_uppercase() == "HISPANIC/LATINO OR OF SPANISH ORIGIN")
          .then(True)
          .when(pl.col("hispanic").str.to_uppercase() == "NOT HISPANIC/LATINO OR OF SPANISH ORIGIN")
          .then(False)
          .otherwise(None)
          .alias("is_hispanic")
    ]).drop("hispanic")


def _clean_numeric_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Convert 'missing' strings and field name literals to NULL for numeric fields."""
    numeric_fields = ["approximate_age", "persons", "workers", "boardings"]
    return df.with_columns([
        pl.when(pl.col(field).cast(pl.Utf8).str.to_lowercase() == "missing").then(None)
        .when(pl.col(field).cast(pl.Utf8).str.to_lowercase() == field.lower()).then(None)
        .otherwise(pl.col(field))
        .alias(field)
        for field in numeric_fields
    ])


def _convert_word_numbers(df: pl.DataFrame) -> pl.DataFrame:
    """Convert word numbers to integers for numeric fields (persons, workers, vehicles)."""
    return df.with_columns([
        pl.col(field)
        .replace_strict(WORD_TO_NUMBER, default=pl.col(field))
        .cast(pl.Int32, strict=False)
        .alias(field)
        for field in ["persons", "workers", "vehicles"]
    ])


def _convert_binary_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Convert TRUE/FALSE strings to 0/1 for binary flag fields."""
    binary_fields = [
        "commuter_rail_present", "heavy_rail_present", "express_bus_present",
        "ferry_present", "light_rail_present"
    ]
    return df.with_columns([
        pl.when(pl.col(field).str.to_uppercase() == "TRUE").then(1)
        .when(pl.col(field).str.to_uppercase() == "FALSE").then(0)
        .otherwise(pl.col(field).cast(pl.Int64, strict=False))
        .alias(field)
        for field in binary_fields
    ])


def _add_optional_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Add missing Tier 3 (Optional) fields with NULL if they don't exist in CSV."""
    optional_fields_tier3 = [
        # Transfer routes
        "immediate_access_mode", "immediate_egress_mode",
        "first_route_before_survey_board", "second_route_before_survey_board",
        "third_route_before_survey_board", "first_route_after_survey_alight",
        "second_route_after_survey_alight", "third_route_after_survey_alight",
        # Transfer operators
        "first_before_operator", "first_before_operator_detail", "first_before_technology",
        "second_before_operator", "second_before_operator_detail", "second_before_technology",
        "third_before_operator", "third_before_operator_detail", "third_before_technology",
        "first_after_operator", "first_after_operator_detail", "first_after_technology",
        "second_after_operator", "second_after_operator_detail", "second_after_technology",
        "third_after_operator", "third_after_operator_detail", "third_after_technology",
        # Optional county/geography fields
        "home_county", "workplace_county", "school_county",
        # Optional MAZ/TAZ fields
        "orig_maz", "dest_maz", "home_maz", "workplace_maz", "school_maz",
        "orig_taz", "dest_taz", "home_taz", "workplace_taz", "school_taz",
        "first_board_tap", "last_alight_tap",
        # Optional sparse response fields
        "race_other_string", "auto_to_workers_ratio",
    ]
    for field in optional_fields_tier3:
        if field not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field))
    return df


def load_survey_data(csv_path: Path) -> pl.DataFrame:
    """Load survey data from CSV file."""
    logger.info("Reading CSV: %s", csv_path)

    def _handle_pums_placeholders(df: pl.DataFrame) -> pl.DataFrame:
        """Handle PUMS synthetic data placeholders in a single operation.

        PUMS records lack survey metadata, so we fill required fields with placeholders:
        - field_start/field_end: Use Jan 1 - Dec 31 of survey_year
        - response_id, original_id, survey_name: Use "REGIONAL - PUMS"
        """
        return df.with_columns([
            # PUMS dates: January 1st - December 31st of survey year
            pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
            .then(pl.col("survey_year").cast(pl.Utf8) + "-01-01")
            .otherwise(pl.col("field_start"))
            .alias("field_start"),

            pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
            .then(pl.col("survey_year").cast(pl.Utf8) + "-12-31")
            .otherwise(pl.col("field_end"))
            .alias("field_end"),

            # PUMS string fields: Fill with operator name
            pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
            .then(pl.lit("REGIONAL - PUMS"))
            .otherwise(pl.col("response_id"))
            .alias("response_id"),

            pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
            .then(pl.lit("REGIONAL - PUMS"))
            .otherwise(pl.col("original_id"))
            .alias("original_id"),

            pl.when(pl.col("canonical_operator") == "REGIONAL - PUMS")
            .then(pl.lit("REGIONAL - PUMS"))
            .otherwise(pl.col("survey_name"))
            .alias("survey_name")
        ])

    df = pl.read_csv(
        str(csv_path),
        null_values=["NA", "N/A", "", "Inf"],  # Treat these as null
        infer_schema_length=10000  # Look at more rows for schema inference
    )
    logger.info("Loaded %s rows with %s columns", len(df), len(df.columns))

    # Calculate household_income from bounds if hh_income_nominal_continuous is missing/invalid
    # Try to cast to float, set invalid strings (like "Refused") to null
    df = df.with_columns([
        pl.col("hh_income_nominal_continuous").cast(pl.Float64, strict=False).alias("hh_income_nominal_continuous")
    ])

    # For null values, calculate midpoint from bounds
    df = df.with_columns([
        pl.when(pl.col("hh_income_nominal_continuous").is_null())
        .then(
            (pl.col("income_lower_bound") + pl.col("income_upper_bound")) / 2.0
        )
        .otherwise(pl.col("hh_income_nominal_continuous"))
        .alias("hh_income_nominal_continuous"),

        # Flag rows where we approximated from bounds (where original was null after cast)
        pl.col("hh_income_nominal_continuous").is_null().alias("income_approximated")
    ])
    logger.info("Calculated household_income from bounds for rows with missing/invalid values")

    # Rename columns FIRST before stripping (so we don't strip the float income column)
    # For legacy data: preserve original household_income category and flag as approximated
    df = df.rename({
        "unique_ID": "response_id",
        "ID": "original_id",
        "household_income": "household_income_category",  # Preserve original category string
        "hh_income_nominal_continuous": "household_income",  # Use continuous value as main field
        "survey_tech": "vehicle_tech"  # Rename to match schema
    })

    # Strip whitespace from all string columns to simplify corrections
    string_cols = [
        col for col, dtype in zip(df.columns, df.dtypes, strict=False)
        if dtype == pl.Utf8
    ]
    df = df.with_columns([pl.col(col).str.strip_chars() for col in string_cols])

    # Fill NULL canonical_operator with "REGIONAL - PUMS" (for PUMS synthetic data)
    df = df.with_columns(
        pl.col("canonical_operator").fill_null("REGIONAL - PUMS")
    )

    # Handle PUMS placeholders (dates, IDs, survey_name) then fix date formats
    df = _handle_pums_placeholders(df)
    df = df.with_columns([
        pl.when(pl.col("field_start").str.len_chars() == SHORT_DATE_LENGTH)
        .then(pl.col("field_start").str.strptime(pl.Date, format="%m-%d-%y", strict=False))
        .otherwise(pl.col("field_start").str.to_date(format="%Y-%m-%d", strict=False))
        .alias("field_start"),

        pl.when(pl.col("field_end").str.len_chars() == SHORT_DATE_LENGTH)
        .then(pl.col("field_end").str.strptime(pl.Date, format="%m-%d-%y", strict=False))
        .otherwise(pl.col("field_end").str.to_date(format="%Y-%m-%d", strict=False))
        .alias("field_end"),
    ])

    # Convert hispanic to boolean
    df = _convert_hispanic_to_bool(df)

    # Convert "missing" strings and field name literals to NULL for numeric fields
    df = _clean_numeric_fields(df)

    # Convert word numbers to integers for numeric fields (persons, workers, vehicles)
    df = _convert_word_numbers(df)


    # Standardize field names: replace periods with underscores, simplify GEOID suffixes
    rename_map = {col: col.replace(".", "_").replace("_GEOID20", "_GEOID")
                  for col in df.columns if "." in col or "_GEOID20" in col}
    if rename_map:
        logger.info("Standardizing %s column names...", len(rename_map))
        df = df.rename(rename_map)

    # Some records have placeholder string values that need to be converted to NULL
    # e.g., approximate_age="approximate_age", transfer_from="None"
    placeholder_fields = {
        "approximate_age": "approximate_age",
        "transfer_from": "None",
        "transfer_to": "None",
        "survey_time": "None",
        "day_part": "None",
        "vehicle_numeric_cat": "None",
        "worker_numeric_cat": "None",
        "tour_purp_case": "None",
        "transfers_surveyed": "None",
    }

    df = df.with_columns([
        pl.when(pl.col(col) == placeholder_fields[col]).then(None).otherwise(pl.col(col)).alias(col)
        if col in placeholder_fields else pl.col(col)
        for col in df.columns
    ])

    # Convert TRUE/FALSE strings to 0/1 for binary flag fields
    df = _convert_binary_fields(df)

    # Add missing Tier 3 (Optional) fields with NULL if they don't exist in CSV
    df = _add_optional_fields(df)

    # This word-to-number conversion was already handled earlier using WORD_TO_NUMBER constant

    # Helper functions for text normalization
    def _check_special_patterns(text: str) -> str | None:
        """Check for special text patterns that need specific handling. Returns None if no match."""
        text_lower = text.lower()

        # Full-time/part-time patterns
        if text_lower.startswith(("full-", "full ")):
            return "Full- or part-time"

        # Non- prefix patterns
        if text_lower.startswith("non-"):
            if "binary" in text_lower:
                return "Non-binary"
            if "student" in text_lower:
                return "Non-student"
            if "worker" in text_lower:
                return "Non-worker"

        return None

    def _simplify_fare_medium(text: str) -> str | None:
        """Simplify complex fare medium values. Returns None if not a fare medium pattern."""
        text_lower = text.lower()

        # Clipper variants
        if "clipper" in text_lower and "(" in text:
            if "e-cash" in text_lower or "ecash" in text_lower:
                return "Clipper (e-cash)"
            if "monthly" in text_lower:
                return "Clipper (monthly)"
            # Default for ride variants and others
            return "Clipper (pass)"

        # Ticket variants - all map to Paper Ticket
        if "ticket" in text_lower:
            return "Paper Ticket"

        # Pass variants
        if "pass" in text_lower and "(" in text:
            if "day" in text_lower or "24" in text:
                return "Day Pass"
            if "monthly" in text_lower or "31" in text:
                return "Monthly Pass"
            return "Pass"

        return None

    # Normalize categorical fields to sentence case to match codebook
    def normalize_to_sentence_case(text: str) -> str:
        """Convert text to proper sentence case matching codebook standards."""
        if not text or text == ".":
            return text

        # Strip whitespace and replace underscores
        text = text.strip().replace("_", " ")

        # Exact match mappings (case-insensitive input) - consolidated for readability
        # Note: Hispanic mappings removed - now handled by _convert_hispanic_to_bool
        exact_mappings = {
            "PREFER NOT TO ANSWER": "Prefer not to answer",
            "MUNI": "Muni", "AC TRANSIT": "AC Transit", "SAMTRANS": "SamTrans",
            "WESTCAT": "WestCat", "SOLTRANS": "SolTrans", "TRI-DELTA": "Tri-Delta",
            "LAVTA": "LAVTA", "PNR": "PNR", "KNR": "KNR",
            "NOT WELL": "Not well", "NOT AT ALL": "Not at all",
            "LESS THAN WELL": "Less than well", "NOT WELL AT ALL": "Not well at all",
            "VERY WELL": "Very well",
            "TABLET_PI": "Tablet PI", "TABLET PI": "Tablet PI",
            "BRIEF_CATI": "Brief CATI", "BRIEF CATI": "Brief CATI",
            "OTHER MAINTENANCE": "Other maintenance", "OTHER DISCRETIONARY": "Other discretionary",
            "SOCIAL RECREATION": "Social recreation", "WORK-RELATED": "Work-related",
            "BUSINESS APT": "Business apt", "EAT OUT": "Eat out",
            "HIGH SCHOOL": "High school", "GRADE SCHOOL": "Grade school", "AT WORK": "At work",
            "CASH (BILLS AND COINS)": "Cash", "TRANSFER (BART PAPER + CASH)": "Transfer",
            "FOUR OR MORE": "Four or more", "SIX OR MORE": "Six or more",
            "DON'T KNOW": "Don't know", "HOTELS": "Hotel",
            "ON OFF DUMMY": "On-off dummy",
            "INTERVIEWER - ADMINISTERED": "Interviewer-administered",
            "SELF - ADMINISTERED": "Self-administered",
            "SKIP - PAPER SURVEY": "Skip - Paper Survey",
            "TAGALOG/FILIPINO": "Tagalog",
            "SANTA ROSA CITYBUS": "Santa Rosa City Bus", "EMERYVILLE MTA": "Emeryville MTA",
            "WHEELS (LAVTA)": "Wheels (LAVTA)", "EMERY-GO-ROUND": "Emery-Go-Round",
            "RIO-VISTA": "Rio-Vista", "FAIRFIELD-SUISUN": "Fairfield-Suisun",
            "BLUE & GOLD FERRY": "Blue Gold Ferry",
        }

        text_upper = text.upper()
        for key, value in exact_mappings.items():
            if text_upper == key.upper():
                return value

        # Check special patterns
        special_result = _check_special_patterns(text)
        if special_result:
            return special_result

        # Check fare medium simplifications
        fare_result = _simplify_fare_medium(text)
        if fare_result:
            return fare_result

        # Convert to title case
        words = text.split()
        result = []
        known_acronyms = {
            "AC", "BART", "VTA", "ACE", "SMART", "SF", "TAP", "TAZ",
            "MAZ", "ID", "CATI", "PI", "AM", "PM", "RTC", "PNR", "KNR", "TNC"
        }
        lowercase_words = {
            "or", "of", "and", "the", "a", "an", "in", "on", "at", "to", "by", "with"
        }

        for i, word in enumerate(words):
            # Keep all-caps acronyms
            if word.isupper() and ACRONYM_MIN_LENGTH <= len(word) <= ACRONYM_MAX_LENGTH:
                result.append(word if word in known_acronyms else word.capitalize())
            # Lowercase specific words when not first, or words after hyphens
            elif (i > 0 and word.lower() in lowercase_words) or (
                i > 0 and len(result) > 0 and result[-1].endswith("-")
            ):
                result.append(word.lower())
            # Handle compound words with hyphens
            elif "-" in word and i == 0:
                parts = word.split("-")
                result.append(parts[0].capitalize() + "-" + "-".join(p.lower() for p in parts[1:]))
            # First word or proper nouns - capitalize
            elif i == 0:
                result.append(word.capitalize())
            # Keep mixed case words that look like proper nouns
            elif word[0].isupper() and not word.isupper() and len(word) > 1:
                result.append(word)
            else:
                result.append(word.capitalize())

        return " ".join(result)

    # Normalize categorical fields efficiently by mapping unique values
    # This avoids slow row-by-row Python UDF calls
    # Note: hispanic removed from list - now converted to boolean is_hispanic
    categorical_fields_complex = [
        "work_status", "student_status", "household_income_category",
        "survey_type", "interview_language", "field_language",
        "orig_purp", "dest_purp", "tour_purp", "trip_purp", "transfer_from", "transfer_to",
        "access_mode", "egress_mode", "fare_medium", "eng_proficient", "fare_category"
    ]
    # Normalize complex categorical fields efficiently by mapping unique values
    # This avoids slow row-by-row Python UDF calls
    for field in categorical_fields_complex:
        unique_vals = df[field].unique().drop_nulls().to_list()
        if unique_vals:
            mapping = {val: normalize_to_sentence_case(val) for val in unique_vals}
            df = df.with_columns(
                pl.col(field).replace_strict(mapping, default=pl.col(field)).alias(field)
            )

    # Simple title case for fields without complex rules
    categorical_fields_simple = [
        "weekpart", "day_of_the_week", "direction",
        "vehicle_tech", "first_board_tech", "last_alight_tech", "gender", "race"
    ]
    df = df.with_columns([
        pl.col(field).str.to_titlecase().alias(field)
        for field in categorical_fields_simple
    ])

    # Fix Ferry→Ferry Boat for technology fields
    for tech_field in ["vehicle_tech", "first_board_tech", "last_alight_tech"]:
        df = df.with_columns(
            pl.when(pl.col(tech_field) == "Ferry")
            .then(pl.lit("Ferry Boat"))
            .otherwise(pl.col(tech_field))
            .alias(tech_field)
        )

    # ============================================================================
    # TYPO FIXES & EXPLICIT VALUE MAPPINGS
    # Applied after normalize_to_sentence_case to catch edge cases
    # Organized by field category for maintainability
    # ============================================================================
    typo_fixes = {
        # ------------------------------------------------------------------------
        # ACCESS & EGRESS MODES
        # ------------------------------------------------------------------------
        "access_mode": ACCESS_EGRESS_MODE_FIXES,
        "egress_mode": {**ACCESS_EGRESS_MODE_FIXES, ".": None},

        # ------------------------------------------------------------------------
        # SURVEY & FIELD METADATA
        # ------------------------------------------------------------------------
        "eng_proficient": {
            "Very Well": "Very well",
            "Unknown": None,
            "Skip - paper survey": "Skip - Paper Survey",
            "Missing": None,
        },
        "direction": {
            "Outboudn": "Outbound",
            "Oubound": "Outbound"
        },

        # ------------------------------------------------------------------------
        # FARE FIELDS (Largest section - fare_category and fare_medium)
        # ------------------------------------------------------------------------
        "fare_category": {
            "Easypass or class pass": "Other",
            "ADA": "Disabled",
            "Ada": "Disabled",
            "ADA ": "Disabled",
            " ADA": "Disabled",
            "ada": "Disabled",
            "Other Discount": "Other",
            "Senior Or Disabled": "Senior",
            "Senior or Disabled": "Senior",
            "Easypass Or Class Pass": "Other",
            "Easypass or Class Pass": "Other",
            "Rtc": "RTC",
            "Round Trip": "Adult",
            "Employee Discount": "Other",
            "Staff Discount-sjsu": "Other",
            "Ada/accomp": "Disabled",
            "Program Bus Pass Grom Turning Point": "Other",
            "City Pass": "Other",
            "Lynx Pass": "Other",
            "Lynxs": "Other",
            "Paper Ticket": "Adult",
            "One Way": "Adult",
            "Adult/student": "Adult",
            "Disabled/rtc": "RTC",
            "Youth(<19)": "Youth",
            "Do Not Know": "Missing",
            "Other/discounted Fare Category": "Other",
            "Promotional/special (not Specified)": "Other",
            "Student Discount": "Other",
            "Police Officer/leo": "Other",
            "Group Discount": "Other",
            "Senior 1/2 Off Promotion": "Senior",
            "Oakland A's Promotion": "Other",
            "Para": "Disabled",
            "Free Trip Card": "Other",
            "Machine Gives You Free Transfer, Bart Pays Cash": "Other",
            "Buy One/2nd Passenger Half-price/free Promotion": "Other",
        },

        "survey_type": {
            "Paper-collected by Interviewer": "Paper - collected by interviewer",
            "Paper-Collected by Interviewer": "Paper - collected by interviewer",
            "paper-collected by interviewer": "Paper - collected by interviewer",
            "Brief_cati": "Brief CATI",
            "Full_paper": "Full paper",
            "Tablet_pi": "Tablet PI",
            "On_off_dummy": "On-off dummy",
            "On off dummy": "On-off dummy",
            "Paper-mail-in": "Paper - mail-in",
            "Paper mail-in": "Paper - mail-in",
            "Interviewer-administered": "Interviewer-administered",
            "Interviewer - administered": "Interviewer-administered",
            "Self-administered": "Self-administered",
            "Self - administered": "Self-administered",
            "ONBOARD": "Onboard",
            "PHONE": "Phone",
            # work_status value appearing in survey_type - map to Missing
            "Full- or part-time": "Missing"
        },

        "fare_medium": {
            # Transfer types - consolidate variations
            "Transfer (bart)":  "Transfer",
            "Transfer (Bart)": "Transfer",
            "Transfer (Bart Paper)": "Transfer",
            "Transfer (bart Paper)": "Transfer",
            "Transfer (paper with Mag Strip)": "Transfer",
            "Transfer (ace)": "Transfer",
            "Transfer (golden Gate or Ferry Shutt": "Transfer",
            "Transfer (county Connection Paper)": "Transfer",
            "Transfer (wheels)": "Transfer",
            "Transfer (bus)": "Transfer",
            "Transfer (santa Rosa Citybus)": "Transfer",
            "Transfer (ac/dex)": "Transfer",
            "Transfer (not Santa Rosa Citybus)": "Transfer",
            "Capitol Corridor Transfer": "Transfer",

            # Mobile/online - fix capitalization
            "Amtrak Mobile App": "Mobile App",
            "Mobile app": "Mobile App",
            "Website (capitol Corridor or Amtrak)": "E-Ticket",
            "Website": "E-Ticket",
            "Conductor/on Train": "Paper Ticket",

            # Cash variations - fix capitalization
            "Clipper Cash": "Clipper (e-cash)",
            "Cash (coins and Bills)": "Cash",
            "Cash (round Trip)": "Cash",
            "Cash (single Ride)": "Cash",
            "Cash (one Way)": "Cash",
            "Cash or Paper": "Cash",
            "Paper or Cash": "Cash",
            "Token": "Cash",
            "Free Ride Token": "Free",
            "Exempt (employee, Law Enforcement)": "Free",
            "Paratransit Free Rider": "Free",
            "Courtesy Fare": "Free",
            "No Payment": "Free",
            "Did Not Pay": "Free",

            # Disability-related fare types
            "Disabled": "Pass",

            # Clipper variations
            "Clipper Pass": "Clipper (pass)",
            "Clipper Start": "Clipper (pass)",
            "Clipper 31-day Pass": "Clipper (monthly)",
            "Clipper Day Pass": "Clipper (pass)",

            # Pass types - fix capitalization
            "Gold Card": "Pass",
            "Employer/wage Works": "Pass",
            "Employer/university/group pass": "Pass",
            "Employer/university/group Pass": "Pass",
            "Srjc Student Pass": "Pass",
            "College of Marin Pass": "Pass",
            "College": "Pass",
            "Youth Quarterly Pass": "Pass",
            "Youth Unlimited Pass": "Pass",
            "Way2go Pass": "Pass",
            "Veteran Pass": "Pass",
            "Rtc Discount Card": "Pass",
            "Discount (rtc)": "Pass",
            "10-ride Pass": "Pass",
            "20-ride Pass": "Pass",
            "20 Ride": "Pass",
            "Punch-20": "Pass",
            "Paper Pass": "Pass",
            "Low Income Pass": "Pass",
            "Free Youth/student Pass": "Pass",
            "Hopthru Pass": "Pass",
            "Ecopass": "Pass",
            "Change Card": "Pass",
            "Superpass": "Pass",
            "Parking Pass Hercules": "Pass",
            "Parking Pass Hercules Transit": "Pass",

            # Monthly pass variations - fix capitalization
            "Monthly": "Monthly Pass",
            "Monthly pass": "Monthly Pass",
            "31-day Pass": "Monthly Pass",
            "Adult 31-day Pass": "Monthly Pass",
            "Half Price 31-day Pass": "Monthly Pass",
            "Half Price Monthly": "Monthly Pass",
            "Youth 31-day Pass": "Monthly Pass",
            "Adult 31-day": "Monthly Pass",
            "Adult Monthly": "Monthly Pass",
            "Youth Monthly": "Monthly Pass",
            "Cash 31-day Pass": "Monthly Pass",
            "31 Day Pass Lynx": "Monthly Pass",
            "Lynx 31 Day": "Monthly Pass",
            "Lynx 31 Day Pass": "Monthly Pass",
            "31 Day Lynx": "Monthly Pass",
            "Monthly Intercity": "Monthly Pass",

            # Day pass variations - fix capitalization
            "Cash 24-hour Pass": "Day Pass",
            "One Day Pass": "Day Pass",
            "Adult Day Pass": "Day Pass",
            "Youth Day Pass": "Day Pass",
            "Half Price Day Pass": "Day Pass",
            "20 Day Pass": "Pass",  # Not monthly, not day - general pass
            "Bart Transfer For $1.50": "Transfer",

            # Other fare media
            "From a Station Agent": "Other",
            "Local City or Transit Office (city of Roseville, Placer County Transit, Etc.)": (
                "Other"
            ),
            "1-way Rv/isleton Dev": "Other",
            "Do Not Know": "Missing",

            # Final batch of specific fare media from validation errors
            "Transfer From Bart Free": "Transfer",
            "Ac Transit Transfer": "Transfer",
            "Transfer (county Connection)": "Transfer",
            "Caltrain (2+ Zones)": "Paper Ticket",
            "Westcat Parking Pass Trip": "Pass",
            "Clipper 10-ride Pass": "Clipper (pass)",
            "Senior Pass Buy at Lake Merriot Station": "Pass",
            "12 Day Pass": "Day Pass",
            "20 Ride Pass": "Pass",
            "The Superpass": "Pass",
            "Medicare": "Pass",
            "Fact Bus Buss Program": "Pass",
            "County Provided": "Pass",
            "Gvnt Bus Pass": "Pass",
            "Bus Pass Provided by Work": "Pass",
            "Senior": "Pass",
            "Free City Bus Employye": "Free",
            "County Employee": "Free",
            "Ada": "Free",
            "Ambassador Pass": "Pass",

            # RTC and other program-specific fare media
            "Rediwheels Rtc": "Pass",
            "Redi-Wheels Rtc": "Pass",
            "Rtc": "Pass"
        },

        # ------------------------------------------------------------------------
        # DEMOGRAPHICS & HOUSEHOLD
        # ------------------------------------------------------------------------
        "household_income_category": {
            "Refused": "Missing",
            "refused": "Missing",
            "$200,000 or Higher": "$200,000 or higher",
            "$150,000 or Higher": "$150,000 or higher",
            "$35,000 or Higher": "$35,000 or higher",
            "under $10,000": "Under $10,000",
            "under $35,000": "Under $35,000"
        },
        "work_status": {
            "Full- Or Part-Time": "Full- or part-time",
            "Non-Worker": "Non-worker"
        },
        "student_status": {
            "Full- Or Part-Time": "Full- or part-time",
            "Non-Student": "Non-student",
            "Missing": None
        },
        "gender": {
            "Non-Binary": "Non-binary",
            "Prefer Not To Answer": "Prefer not to answer",
            "female": "Female",
            "male": "Male"
        },
        "race": {
            "WHITE": "White",
            "ASIAN": "Asian",
            "BLACK": "Black",
            "OTHER": "Other",
            "Missing": None
        },

        # ------------------------------------------------------------------------
        # LANGUAGE FIELDS
        # ------------------------------------------------------------------------
        "field_language": LANGUAGE_MAPPINGS,
        "interview_language": {
            **LANGUAGE_MAPPINGS,
            "Cantonese Chinese": "Cantonese Chinese",  # Keep as-is (matches enum)
            "Mandarin Chinese": "Mandarin Chinese"      # Keep as-is (matches enum)
        },

        # ------------------------------------------------------------------------
        # TRANSFER OPERATORS
        # ------------------------------------------------------------------------
        "transfer_from": {
            "Santa Rosa CityBus": "Santa Rosa City Bus",
            "Emeryville Mta": "Emeryville MTA",
            "Wheels (lavta)": "Wheels (LAVTA)",
            "Emery-go-round": "Emery-Go-Round",
            "Rio-vista": "Rio-Vista",
            "Fairfield-suisun": "Fairfield-Suisun",
            "Blue & Gold Ferry": "Blue Gold Ferry"
        },

        "transfer_to": {
            "Santa Rosa CityBus": "Santa Rosa City Bus",
            "Santa Rosa Citybus": "Santa Rosa City Bus",
            "Emeryville Mta": "Emeryville MTA",
            "Wheels (lavta)": "Wheels (LAVTA)",
            "Emery-go-round": "Emery-Go-Round",
            "Blue & Gold Ferry": "Blue Gold Ferry",
            "Fairfield-suisun": "Fairfield-Suisun"
        },
        "day_part": {
            "NIGHT": "EVENING",  # Normalize NIGHT to EVENING for DayPart enum
            "WEEKEND": None,  # WEEKEND not a valid time of day, map to None
            ".": None,  # Period indicates missing data
            "Missing": None  # Explicit missing values
        }
    }
    # ============================================================================

    for field, mapping in typo_fixes.items():
        df = df.with_columns(
            pl.col(field).replace_strict(mapping, default=pl.col(field)).alias(field)
        )

    # Global cleanup: Replace any remaining "Missing" strings with None for enum fields
    enum_fields = [
        "direction", "access_mode", "egress_mode", "first_board_tech", "last_alight_tech",
        "orig_purp", "dest_purp", "fare_category", "day_of_the_week", "race",
        "work_status", "student_status", "interview_language", "field_language",
        "transfer_1_operator", "transfer_2_operator", "transfer_3_operator",
        "fare_medium", "household_income_category", "survey_type", "weekpart",
        "day_part", "eng_proficient", "tour_purp", "trip_purp", "gender"
    ]
    df = df.with_columns([
        pl.when(pl.col(field) == "Missing").then(None).otherwise(pl.col(field)).alias(field)
        for field in enum_fields if field in df.columns
    ])

    # Add survey_id (computed from canonical_operator and survey_year)
    df = df.with_columns(
        (
            pl.col("canonical_operator") + "_" + pl.col("survey_year").cast(pl.Utf8)
        ).alias("survey_id")
    )

    # Log column names for debugging
    logger.info("Columns after rename & normalization: %s...", df.columns[:10])

    return df


def validate_schema(df: pl.DataFrame, sample_size: int = 100) -> None:
    """Validate a sample of records against the Pydantic schema.

    Note: Only validates fields that exist in both the DataFrame and the schema.
    The CSV may have additional columns (geography, derived fields) that we want to preserve.
    """
    logger.info("Validating %s sample records against schema...", sample_size)
    sample = df.head(sample_size)

    # Get schema fields
    schema_fields = set(SurveyResponse.model_fields.keys())
    df_columns = set(df.columns)

    # Only validate columns that exist in both
    common_fields = schema_fields & df_columns
    logger.info(
        "Validating %s common fields (DataFrame has %s columns total)",
        len(common_fields),
        len(df_columns)
    )

    validation_errors = []
    error_details = {}  # Group errors by field

    for i, row_dict in enumerate(sample.iter_rows(named=True)):
        # Filter to only fields in schema
        filtered_row = {k: v for k, v in row_dict.items() if k in schema_fields}
        try:
            SurveyResponse(**filtered_row)
        except ValidationError as e:
            validation_errors.append((i, e))
            # Group errors by field
            for error in e.errors():
                field = error["loc"][0] if error["loc"] else "unknown"
                if field not in error_details:
                    error_details[field] = {
                        "count": 0,
                        "unique_values": set(),
                        "rows": [],
                        "error_types": set(),
                    }
                error_details[field]["count"] += 1
                error_details[field]["rows"].append(i)
                error_details[field]["error_types"].add(error["type"])
                # Store actual field value, not entire record
                if "input" in error:
                    # For missing fields, input is the whole dict - just note it's missing
                    if error["type"] == "missing" or isinstance(error["input"], dict):
                        error_details[field]["unique_values"].add("<missing>")
                    else:
                        error_details[field]["unique_values"].add(str(error["input"]))

    if validation_errors:
        logger.error(
            "Validation failed on %s of %s sample records:",
            len(validation_errors),
            len(sample)
        )
        logger.error("\nErrors grouped by field:")
        for field, details in sorted(error_details.items()):
            logger.error("\n  Field: %s", field)
            logger.error("    Error types: %s", ", ".join(sorted(details["error_types"])))
            logger.error("    Failed in %s records (rows: %s...)", details["count"], details["rows"][:5])
            if details["unique_values"]:
                logger.error(
                    "    Unique problematic values (%s total): %s",
                    len(details["unique_values"]),
                    sorted(details["unique_values"])[:10]
                )
        msg = "Schema validation failed. Fix data or schema before importing."
        raise ValueError(msg)

    logger.info("Schema validation passed")


def reorder_columns_to_match_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Reorder DataFrame columns to match Pydantic model field order.

    This ensures Parquet files have columns in the same logical order as the
    data model definition, with primary keys first.

    Args:
        df: DataFrame with survey response data

    Returns:
        DataFrame with columns reordered to match SurveyResponse model
    """
    # Get field order from Pydantic model
    schema_order = list(SurveyResponse.model_fields.keys())

    # Select only fields that exist in both schema and DataFrame
    existing_cols = [col for col in schema_order if col in df.columns]

    # Preserve extra columns (geography, derived fields) at the end
    extra_cols = [col for col in df.columns if col not in schema_order]

    logger.info(
        "Reordering %s columns to match schema (%s extra columns preserved)",
        len(existing_cols),
        len(extra_cols)
    )

    return df.select(existing_cols + extra_cols)


def ingest_survey_batches(
    df: pl.DataFrame, collect_errors: bool = False
) -> tuple[int, int, dict, dict]:
    """Ingest survey data to the data lake, partitioned by operator and year.

    Args:
        df: DataFrame to ingest
        collect_errors: If True, collect validation errors instead of raising

    Returns:
        Tuple of (total_records, num_batches, error_summary, version_info).
        version_info maps (operator, year) -> (version, commit)
        error_summary is a dict mapping field names to dicts of
        {invalid_value: [(operator, year, row_idx), ...]}
    """
    logger.info("Grouping data by (survey_year, canonical_operator)...")

    # Group by survey_year and canonical_operator
    groups = df.group_by(["survey_year", "canonical_operator"])

    total_records = 0
    num_batches = 0
    error_summary = defaultdict(lambda: defaultdict(list))
    batches_with_errors = []
    version_info = {}  # Track (operator, year) -> (version, commit, hash)

    for (survey_year, canonical_operator), group_df in groups:
        logger.info(
            "Processing: %s %s (%s records)", canonical_operator, survey_year, len(group_df)
        )

        if collect_errors:
            # Validate each row individually to collect all errors
            batch_errors = validate_batch_collect_errors(
                group_df, survey_year, canonical_operator
            )

            if batch_errors:
                batches_with_errors.append((canonical_operator, survey_year, len(batch_errors)))
                # Merge batch errors into summary
                for field, values_dict in batch_errors.items():
                    for value, occurrences in values_dict.items():
                        error_summary[field][value].extend(occurrences)
                logger.warning("  Found %s field(s) with validation errors", len(batch_errors))
            else:
                # No errors - but don't write in error collection mode, just count
                logger.info("  ✓ Validation passed (would write %s records)", len(group_df))
                total_records += len(group_df)
                num_batches += 1
        else:
            # Original behavior: raise on first error
            _output_path, version, commit, data_hash = db.ingest_survey_batch(
                df=group_df,
                survey_year=survey_year,
                canonical_operator=canonical_operator,
                validate=True
            )
            version_info[(canonical_operator, survey_year)] = (version, commit, data_hash)
            total_records += len(group_df)
            num_batches += 1

    if collect_errors and error_summary:
        logger.info(
            "\n%s\nVALIDATION ERROR SUMMARY\n%s\n\nBatches with errors: %s",
            "="*80,
            "="*80,
            len(batches_with_errors)
        )
        for operator, year, num_fields in batches_with_errors:
            logger.info("  - %s %s: %s fields with errors", operator, year, num_fields)

        logger.info("\nTotal fields with errors: %s", len(error_summary))
        for field in sorted(error_summary.keys()):
            values_dict = error_summary[field]
            logger.info("\n%s: %s invalid value(s)", field, len(values_dict))
            for value in sorted(
                values_dict.keys(), key=lambda v: len(values_dict[v]), reverse=True
            )[:20]:
                occurrences = values_dict[value]
                sample_locs = [
                    f"{op} {yr} row {idx}"
                    for op, yr, idx in occurrences[:MAX_SAMPLE_LOCATIONS]
                ]
                more_text = (
                    f" ... and {len(occurrences) - MAX_SAMPLE_LOCATIONS} more"
                    if len(occurrences) > MAX_SAMPLE_LOCATIONS else ""
                )
                logger.info(
                    "  '%s': %s occurrence(s) - %s%s",
                    value,
                    len(occurrences),
                    ", ".join(sample_locs),
                    more_text
                )

    logger.info("\n%s", "="*80)
    if collect_errors:
        logger.info(
            "Validated %s batches (%s records passed) - Skipped %s batches due to errors",
            num_batches,
            total_records,
            len(batches_with_errors)
        )
    else:
        logger.info("Wrote %s batches totaling %s records", num_batches, total_records)
    return total_records, num_batches, dict(error_summary), version_info


def validate_batch_collect_errors(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str
) -> dict:
    """Validate a batch and collect all validation errors.

    Returns:
        Dict mapping field names to dicts of {invalid_value: [(operator, year, row_idx), ...]}
    """
    errors = defaultdict(lambda: defaultdict(list))

    for row_idx, row in enumerate(df.iter_rows(named=True)):
        try:
            SurveyResponse(**row)
        except ValidationError as e:
            # Parse validation error and collect field/value info
            for error in e.errors():
                field = error["loc"][0] if error["loc"] else "unknown"
                # Get the actual value from the row
                if field in row:
                    invalid_value = str(row[field])
                    errors[field][invalid_value].append(
                        (canonical_operator, survey_year, row_idx)
                    )

    return dict(errors)


def extract_and_ingest_metadata(
    df: pl.DataFrame, version_info: dict, collect_errors: bool = False
) -> int:
    """Extract survey metadata and write to data lake.

    Args:
        df: Survey data DataFrame
        version_info: Dict mapping (operator, year) -> (version, commit)
        collect_errors: If True, collect and report validation errors instead of failing

    Returns:
        Number of unique survey metadata records written.
    """
    logger.info("\nExtracting survey metadata...")

    # Get unique combinations of metadata fields
    metadata_df = (
        df.select([
            "survey_year",
            "canonical_operator",
            "survey_name",
            "field_start",
            "field_end",
        ])
        .unique()
        .with_columns([
            pl.lit("standardized_csv").alias("source"),
            pl.lit(None).alias("inflation_year"),
        ])
    )

    # Add version tracking fields from version_info
    rows = []
    for row in metadata_df.iter_rows(named=True):
        operator = row["canonical_operator"]
        year = row["survey_year"]
        version, commit, data_hash = version_info.get(
            (operator, year), (1, "unknown", "unknown")
        )
        row["survey_id"] = f"{operator}_{year}"
        row["data_version"] = version
        row["data_commit"] = commit
        row["data_hash"] = data_hash
        row["ingestion_timestamp"] = datetime.now(tz=UTC)
        row["processing_notes"] = "Initial backfill from standardized_* CSV"
        rows.append(row)

    metadata_df = pl.DataFrame(rows)

    # Reorder columns to match SurveyMetadata model field order
    metadata_df = metadata_df.select([
        "survey_id",
        "survey_year",
        "canonical_operator",
        "survey_name",
        "source",
        "field_start",
        "field_end",
        "inflation_year",
        "data_version",
        "data_commit",
        "data_hash",
        "ingestion_timestamp",
        "processing_notes",
    ])

    logger.info("Found %d unique survey combinations", len(metadata_df))

    if collect_errors:
        # Validate and collect errors
        errors = []
        valid_rows = []

        for i, row_dict in enumerate(metadata_df.iter_rows(named=True)):
            try:
                SurveyMetadata(**row_dict)
                valid_rows.append(i)
            except ValidationError as e:
                operator = row_dict["canonical_operator"]
                year = row_dict["survey_year"]
                errors.append({
                    "operator": operator,
                    "year": year,
                    "error": str(e)
                })

        if errors:
            logger.warning("\n%s", "="*80)
            logger.warning("METADATA VALIDATION ERRORS")
            logger.warning("%s", "="*80)
            logger.warning("\nFound %s metadata record(s) with validation errors:", len(errors))

            for err in errors:
                logger.warning("\n%s %s:", err["operator"], err["year"])
                # Extract just the field names from the error
                error_lines = err["error"].split("\n")
                for line in error_lines:
                    if "field_start" in line or "field_end" in line or "Input should be" in line:
                        logger.warning("  %s", line)

            logger.warning("\n%s", "="*80)
            logger.warning("Validated %s metadata records successfully", len(valid_rows))
            logger.warning("Skipped %s metadata records due to validation errors", len(errors))
            logger.warning("%s", "="*80)
            return len(valid_rows)
        logger.info("All %s metadata records validated successfully", len(metadata_df))
        return len(metadata_df)
    # Write metadata to data lake (will fail on first error)
    db.ingest_survey_metadata(metadata_df, validate=True)
    return len(metadata_df)

def extract_and_ingest_weights(
    df: pl.DataFrame, collect_errors: bool = False
) -> int:
    """Extract survey weights and write to data lake.

    Args:
        df: Survey data DataFrame with weight and trip_weight columns
        collect_errors: If True, collect and report validation errors instead of failing

    Returns:
        Number of weight records written.
    """
    logger.info("\nExtracting survey weights...")

    # Select weight columns and add constants
    weights_df = df.select([
        pl.col("response_id"),
        pl.col("weight").alias("boarding_weight"),
        "trip_weight",
    ]).with_columns([
        pl.lit("baseline").alias("weight_scheme"),
        pl.lit("Baseline weights").alias("description"),
    ])

    logger.info("Found %d weight records", len(weights_df))

    if collect_errors:
        # Validate and collect errors
        errors = []
        valid_rows = []

        for i, row_dict in enumerate(weights_df.iter_rows(named=True)):
            try:
                SurveyWeight(**row_dict)
                valid_rows.append(i)
            except ValidationError as e:
                response_id = row_dict["response_id"]
                errors.append({
                    "response_id": response_id,
                    "error": str(e)
                })

        if errors:
            logger.warning("\n%s", "="*80)
            logger.warning("WEIGHT VALIDATION ERRORS")
            logger.warning("%s", "="*80)
            logger.warning("\nFound %s weight record(s) with validation errors:", len(errors))

            max_errors_to_show = 10
            for err in errors[:max_errors_to_show]:
                logger.warning("\n%s:", err["response_id"])
                error_lines = err["error"].split("\n")
                for line in error_lines:
                    if (
                        "boarding_weight" in line
                        or "trip_weight" in line
                        or "Input should be" in line
                    ):
                        logger.warning("  %s", line)

            if len(errors) > max_errors_to_show:
                logger.warning("\n... and %s more errors", len(errors) - max_errors_to_show)

            logger.warning("\n%s", "="*80)
            logger.warning("Validated %s weight records successfully", len(valid_rows))
            logger.warning("Skipped %s weight records due to validation errors", len(errors))
            logger.warning("%s", "="*80)
            return len(valid_rows)
        logger.info("All %s weight records validated successfully", len(weights_df))
        return len(weights_df)
    # Write weights to data lake (will fail on first error)
    db.ingest_survey_weights(weights_df, validate=True)
    return len(weights_df)


def main() -> None:
    """Main backfill process."""
    def _validate_csv_exists(csv_path: Path) -> None:
        """Validate that CSV file exists, raise FileNotFoundError if not."""
        if not csv_path.exists():
            msg = f"survey_combined.csv not found in {csv_path.parent}"
            raise FileNotFoundError(msg)

    try:
        # Use specific standardized directory
        # latest_dir = find_latest_standardized_dir()
        latest_dir = STANDARDIZED_DATA_PATH / "standardized_2025-10-13"
        csv_path = latest_dir / "survey_combined.csv"
        _validate_csv_exists(csv_path)

        # Load and prepare data
        logger.info("\n%s", "="*80)
        logger.info("LOADING DATA")
        logger.info("%s", "="*80)
        df = load_survey_data(csv_path)

        logger.info("\nTotal records in CSV: %s", f"{len(df):,}")
        logger.info("Unique operators: %s", df["canonical_operator"].n_unique())
        logger.info("Year range: %s - %s", df["survey_year"].min(), df["survey_year"].max())

        # Derive auto_to_workers_ratio from vehicles and workers
        logger.info("\n%s", "="*80)
        logger.info("DERIVING AUTO_TO_WORKERS_RATIO")
        logger.info("%s", "="*80)
        df = auto_sufficiency.derive_auto_sufficiency(df)
        logger.info("Columns after derivation: auto_to_workers_ratio present = %s", "auto_to_workers_ratio" in df.columns)

        # Validate schema
        logger.info("\n%s", "="*80)
        logger.info("VALIDATING SCHEMA")
        logger.info("%s", "="*80)
        validate_schema(df)

        # Reorder columns to match schema
        logger.info("\n%s", "="*80)
        logger.info("REORDERING COLUMNS TO MATCH SCHEMA")
        logger.info("%s", "="*80)
        df = reorder_columns_to_match_schema(df)

        # Ingest survey data (collect errors mode)
        logger.info("\n%s", "="*80)
        logger.info("INGESTING SURVEY DATA (ERROR COLLECTION MODE)")
        logger.info("%s", "="*80)
        total_records, num_batches, error_summary, version_info = ingest_survey_batches(
            df, collect_errors=False
        )

        if error_summary:
            logger.warning("\n%s", "="*80)
            logger.warning("VALIDATION ERRORS FOUND - NO DATA WRITTEN FOR FAILED BATCHES")
            logger.warning("="*80)
            logger.warning("Review the error summary above and apply fixes to backfill_database.py")
            logger.warning("Then re-run the backfill to write all data.")
            return

        # Extract and ingest metadata
        logger.info("\n%s", "="*80)
        logger.info("INGESTING METADATA")
        logger.info("%s", "="*80)
        num_metadata = extract_and_ingest_metadata(df, version_info, collect_errors=False)

        # Extract and ingest weights
        logger.info("\n%s", "="*80)
        logger.info("INGESTING WEIGHTS")
        logger.info("%s", "="*80)
        num_weights = extract_and_ingest_weights(df, collect_errors=False)

        # Create DuckDB views
        logger.info("\n%s", "="*80)
        logger.info("CREATING DUCKDB VIEWS")
        logger.info("="*80)
        db.create_views()

        # Summary
        logger.info("\n%s", "="*80)
        logger.info("BACKFILL COMPLETE")
        logger.info("="*80)
        logger.info("Survey data: %s batches, %s records", num_batches, f"{total_records:,}")
        logger.info("Metadata: %s survey combinations", num_metadata)
        logger.info("Weights: %s records", f"{num_weights:,}")
        logger.info("Data lake: %s", db.DATA_LAKE_ROOT)
        logger.info("DuckDB: %s", db.DUCKDB_PATH)

    except Exception:
        logger.exception("Backfill failed")
        raise


if __name__ == "__main__":
    main()
