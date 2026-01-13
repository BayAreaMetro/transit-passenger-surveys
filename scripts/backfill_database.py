"""
Backfill the database with historical survey data from standardized CSV files.

This script reads survey_combined.csv from the most recent standardized data directory,
validates the data against the Pydantic schema, and inserts only new records (no duplicates).

This script contains all the data cleaning and normalization logic needed to ensure
the imported data conforms to the database schema. It is very naughty to have this much
logic outside of the main library, but this is a one-time backfill operation so it's
acceptable. Thanks Claude.

"""

import logging
from pathlib import Path

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools import db
from transit_passenger_tools.data_models import SurveyResponse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Path to standardized data
STANDARDIZED_DATA_PATH = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized"
)

def find_latest_standardized_dir() -> Path:
    """Find the most recent standardized data directory."""
    dirs = [d for d in STANDARDIZED_DATA_PATH.iterdir() 
            if d.is_dir() and d.name.startswith("standardized_")]
    if not dirs:
        raise FileNotFoundError(f"No standardized directories found in {STANDARDIZED_DATA_PATH}")
    
    latest = sorted(dirs, key=lambda x: x.name)[-1]
    logger.info(f"Found latest standardized directory: {latest.name}")
    return latest


def load_survey_data(csv_path: Path) -> pl.DataFrame:
    """Load survey data from CSV file."""
    logger.info(f"Reading CSV: {csv_path}")
    df = pl.read_csv(
        str(csv_path),
        null_values=["NA", "N/A", "", "Inf"],  # Treat these as null
        infer_schema_length=10000  # Look at more rows for schema inference
    )
    logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    
    # Rename columns to match our schema (CSV uses unique_ID, we use unique_id)
    df = df.rename({"unique_ID": "unique_id", "ID": "id"})
    
    # Standardize field names: remove periods and simplify GEOID names
    rename_map = {}
    for col in df.columns:
        new_col = col
        # Replace periods with underscores
        if "." in new_col:
            new_col = new_col.replace(".", "_")
        # Simplify GEOID suffixes
        if "_GEOID20" in new_col:
            new_col = new_col.replace("_GEOID20", "_GEOID")
        if new_col != col:
            rename_map[col] = new_col
    
    if rename_map:
        logger.info(f"Standardizing {len(rename_map)} column names...")
        df = df.rename(rename_map)
    
    # Note: CSV headers are already parsed by Polars, so we don't need to filter
    # "header rows". The 19K records with id="ID" are legitimate survey responses
    # with placeholder/missing values, not duplicate headers.
    
    # However, some records have placeholder string values that need to be converted to NULL
    # For example: approximate_age="approximate_age", transfer_from="None"
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
    
    # Build expressions for all fields at once
    exprs = []
    for col in df.columns:
        if col in placeholder_fields:
            placeholder = placeholder_fields[col]
            exprs.append(
                pl.when(pl.col(col) == placeholder)
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
        else:
            exprs.append(pl.col(col))
    
    df = df.with_columns(exprs)
    
    # Convert TRUE/FALSE strings to 0/1 for binary flag fields
    binary_fields = [
        "commuter_rail_present", "heavy_rail_present", "express_bus_present",
        "ferry_present", "light_rail_present"
    ]
    for field in binary_fields:
        if field in df.columns:
            df = df.with_columns(
                pl.when(pl.col(field).str.to_uppercase() == "TRUE")
                .then(1)
                .when(pl.col(field).str.to_uppercase() == "FALSE")
                .then(0)
                .otherwise(pl.col(field).cast(pl.Int64, strict=False))
                .alias(field)
            )
    
    # Add missing Tier 3 (Optional) fields with NULL if they don't exist in CSV
    optional_fields_tier3 = [
        # Transfer routes
        "immediate_access_mode",
        "immediate_egress_mode",
        "first_route_before_survey_board",
        "second_route_before_survey_board",
        "third_route_before_survey_board",
        "first_route_after_survey_alight",
        "second_route_after_survey_alight",
        "third_route_after_survey_alight",
        # Transfer operators
        "first_before_operator",
        "first_before_operator_detail",
        "first_before_technology",
        "second_before_operator",
        "second_before_operator_detail",
        "second_before_technology",
        "third_before_operator",
        "third_before_operator_detail",
        "third_before_technology",
        "first_after_operator",
        "first_after_operator_detail",
        "first_after_technology",
        "second_after_operator",
        "second_after_operator_detail",
        "second_after_technology",
        "third_after_operator",
        "third_after_operator_detail",
        "third_after_technology",
        # Optional county/geography fields
        "home_county",
        "workplace_county",
        "school_county",
        # Optional MAZ/TAZ fields
        "orig_maz",
        "dest_maz",
        "home_maz",
        "workplace_maz",
        "school_maz",
        "orig_taz",
        "dest_taz",
        "home_taz",
        "workplace_taz",
        "school_taz",
        "first_board_tap",
        "last_alight_tap",
        # Optional sparse response fields
        "race_other_string",
        "auto_suff",
    ]
    
    for field in optional_fields_tier3:
        if field not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field))
    
    # Add missing Tier 1 (Required) string/enum fields with defaults
    required_string_defaults = {
        "date_string": "Missing",
        "time_string": "Missing",
        "time_period": "Missing",
        "transit_type": "Missing",
        "immediate_access_mode": "Missing",
        "immediate_egress_mode": "Missing",
    }
    
    for field, default_value in required_string_defaults.items():
        if field not in df.columns:
            df = df.with_columns(pl.lit(default_value).alias(field))
        else:
            # Fill NULLs in existing field
            df = df.with_columns(
                pl.col(field).fill_null(default_value)
            )
    
    # Fill NULL values for existing required enum fields
    # trip_purp is required, fill with "Missing"
    if "trip_purp" in df.columns:
        df = df.with_columns(
            pl.col("trip_purp").fill_null("Missing")
        )
    
    # Fill NULL values for required string fields only
    # Note: transfer_from/transfer_to are now Optional (Tier 3), so don't fill them
    null_fill_mappings = {
        "path_access": "Missing",
        "path_egress": "Missing",
        "path_line_haul": "Missing",
        "path_label": "Missing",
        "eng_proficient": "Missing",
    }
    
    for field, fill_value in null_fill_mappings.items():
        if field in df.columns:
            df = df.with_columns(
                pl.col(field).fill_null(fill_value)
            )
    
    # Convert word numbers to integers for numeric fields
    number_word_map = {
        "zero": 0, "none": 0,
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
    }
    
    for field in ["persons", "workers"]:
        if field in df.columns:
            # Convert word numbers to integers
            df = df.with_columns(
                pl.col(field).str.to_lowercase().replace(number_word_map).cast(pl.Int64, strict=False).alias(field)
            )
    
    # Normalize categorical fields to sentence case to match codebook
    def normalize_to_sentence_case(text: str) -> str:
        """Convert text to proper sentence case matching codebook standards."""
        if not text or text == ".":
            return text
        
        # Strip whitespace
        text = text.strip()
        
        # Replace underscores with spaces
        text = text.replace("_", " ")
        
        # Exact match mappings (case-insensitive input)
        exact_mappings = {
            "HISPANIC/LATINO OR OF SPANISH ORIGIN": "Hispanic/Latino or of Spanish origin",
            "NOT HISPANIC/LATINO OR OF SPANISH ORIGIN": "Not Hispanic/Latino or of Spanish origin",
            "PREFER NOT TO ANSWER": "Prefer not to answer",
            "MUNI": "Muni",
            "AC TRANSIT": "AC Transit",
            "SAMTRANS": "SamTrans",
            "WESTCAT": "WestCat",
            "SOLTRANS": "SolTrans",
            "TRI-DELTA": "Tri-Delta",
            "LAVTA": "LAVTA",
            # Access modes
            "PNR": "PNR",
            "KNR": "KNR",
            "NOT WELL": "Not well",
            "NOT AT ALL": "Not at all",
            "LESS THAN WELL": "Less than well",
            "NOT WELL AT ALL": "Not well at all",
            # English proficiency
            "VERY WELL": "Very well",
            # Survey types
            "TABLET_PI": "Tablet PI",
            "TABLET PI": "Tablet PI",
            "BRIEF_CATI": "Brief CATI",
            "BRIEF CATI": "Brief CATI",
            # Purpose
            "OTHER MAINTENANCE": "Other maintenance",
            "OTHER DISCRETIONARY": "Other discretionary",
            "SOCIAL RECREATION": "Social recreation",
            "WORK-RELATED": "Work-related",
            "BUSINESS APT": "Business apt",
            "EAT OUT": "Eat out",
            "HIGH SCHOOL": "High school",
            "GRADE SCHOOL": "Grade school",
            "AT WORK": "At work",
            # Fare medium
            "CASH (BILLS AND COINS)": "Cash",
            "TRANSFER (BART PAPER + CASH)": "Transfer",
            # Vehicles
            "FOUR OR MORE": "Four or more",
            "SIX OR MORE": "Six or more",
            "DON'T KNOW": "Don't know",
        }
        
        text_upper = text.upper()
        for key, value in exact_mappings.items():
            if text_upper == key.upper():
                return value
        
        # Special handling for specific patterns
        if text.lower().startswith("full-") or text.lower().startswith("full "):
            return "Full- or part-time"
        if text.lower().startswith("non-"):
            if "student" in text.lower():
                return "Non-student"
            elif "worker" in text.lower():
                return "Non-worker"
        
        # Handle fare medium simplifications - map complex values to simple ones
        if "clipper" in text.lower() and "(" in text:
            if "8-ride" in text.lower() or "ride" in text.lower():
                return "Clipper (pass)"
            if "e-cash" in text.lower() or "ecash" in text.lower():
                return "Clipper (e-cash)"
            if "monthly" in text.lower():
                return "Clipper (monthly)"
            return "Clipper (pass)"  # Default for other Clipper variants
        if "ticket" in text.lower():
            if "paper" in text.lower():
                return "Paper ticket"
            return "Paper ticket"  # Default for ticket variants
        if "pass" in text.lower() and "(" in text:
            if "day" in text.lower() or "24" in text:
                return "Day pass"
            if "monthly" in text.lower() or "31" in text:
                return "Monthly pass"
            return "Pass"  # Default for pass variants
        
        # Convert to title case first
        words = text.split()
        result = []
        
        for i, word in enumerate(words):
            # Keep all-caps acronyms (2-4 letters)
            if word.isupper() and 2 <= len(word) <= 4:
                # But check if it's a known acronym that should stay uppercase
                if word in ['AC', 'BART', 'VTA', 'ACE', 'SMART', 'SF', 'TAP', 'TAZ', 
                            'MAZ', 'ID', 'CATI', 'PI', 'AM', 'PM', 'RTC', 'PNR', 'KNR', 'TNC']:
                    result.append(word)
                else:
                    result.append(word.capitalize())
            # Lowercase specific words when not first
            elif i > 0 and word.lower() in ['or', 'of', 'and', 'the', 'a', 'an', 'in', 'on', 'at', 'to', 'by', 'with']:
                result.append(word.lower())
            # Lowercase words after hyphens (except first word)
            elif i > 0 and len(result) > 0 and result[-1].endswith('-'):
                result.append(word.lower())
            # Handle compound words with hyphens
            elif '-' in word and i == 0:
                # First word with hyphen - capitalize first part, lowercase second
                parts = word.split('-')
                result.append(parts[0].capitalize() + '-' + '-'.join(p.lower() for p in parts[1:]))
            # First word or proper nouns - capitalize
            elif i == 0:
                result.append(word.capitalize())
            # Keep mixed case words that look like proper nouns
            elif word[0].isupper() and not word.isupper() and len(word) > 1:
                result.append(word)
            else:
                result.append(word.capitalize())
        
        return ' '.join(result)
    
    categorical_fields = [
        "weekpart", "day_of_the_week", "direction", "access_mode", "egress_mode",
        "transfer_from", "transfer_to", "fare_medium", "fare_category",
        "orig_purp", "dest_purp", "tour_purp", "survey_tech", "first_board_tech",
        "last_alight_tech", "gender", "hispanic", "race", "work_status", 
        "student_status", "eng_proficient", "vehicles", "household_income", 
        "survey_type", "interview_language", "field_language"
    ]
    
    for field in categorical_fields:
        if field in df.columns:
            df = df.with_columns(
                pl.col(field).map_elements(normalize_to_sentence_case, return_dtype=pl.Utf8).alias(field)
            )
    
    # Fix specific typos and inconsistencies
    typo_fixes = {
        "direction": {"Outboudn": "Outbound", "Oubound": "Outbound"},
    }
    
    for field, mapping in typo_fixes.items():
        if field in df.columns:
            for old_val, new_val in mapping.items():
                df = df.with_columns(
                    pl.when(pl.col(field) == old_val)
                    .then(pl.lit(new_val))
                    .otherwise(pl.col(field))
                    .alias(field)
                )
    
    # Log column names for debugging
    logger.info(f"Columns after rename & normalization: {df.columns[:10]}...")
    
    return df


def get_existing_unique_ids() -> set[str]:
    """Get all existing unique_ids from the database."""
    logger.info("Fetching existing unique_ids from database...")
    try:
        existing_df = db.read_query("SELECT unique_id FROM survey_responses")
        existing_ids = set(existing_df["unique_id"].to_list())
        logger.info(f"Found {len(existing_ids)} existing records")
        return existing_ids
    except Exception as e:
        if "no such table" in str(e):
            logger.info("Table doesn't exist yet - all records will be inserted")
            return set()
        raise


def filter_new_records(df: pl.DataFrame, existing_ids: set[str]) -> pl.DataFrame:
    """Filter out records that already exist in the database."""
    logger.info("Filtering for new records...")
    new_df = df.filter(~pl.col("unique_id").is_in(existing_ids))
    logger.info(f"Found {len(new_df)} new records to insert ({len(df) - len(new_df)} duplicates skipped)")
    return new_df


def validate_schema(df: pl.DataFrame, sample_size: int = 100) -> None:
    """Validate a sample of records against the Pydantic schema.
    
    Note: Only validates fields that exist in both the DataFrame and the schema.
    The CSV may have additional columns (geography, derived fields) that we want to preserve.
    """
    logger.info(f"Validating {sample_size} sample records against schema...")
    sample = df.head(sample_size)
    
    # Get schema fields
    schema_fields = set(SurveyResponse.model_fields.keys())
    df_columns = set(df.columns)
    
    # Only validate columns that exist in both
    common_fields = schema_fields & df_columns
    logger.info(f"Validating {len(common_fields)} common fields (DataFrame has {len(df_columns)} columns total)")
    
    validation_errors = []
    for i, row_dict in enumerate(sample.iter_rows(named=True)):
        # Filter to only fields in schema
        filtered_row = {k: v for k, v in row_dict.items() if k in schema_fields}
        try:
            SurveyResponse(**filtered_row)
        except ValidationError as e:
            validation_errors.append((i, e))
            if len(validation_errors) >= 5:  # Stop after 5 errors
                break
    
    if validation_errors:
        logger.error(f"Validation failed on {len(validation_errors)} sample records:")
        for i, error in validation_errors:
            logger.error(f"  Row {i}: {error}")
        raise ValueError("Schema validation failed. Fix data or schema before importing.")
    
    logger.info("✓ Schema validation passed")


def insert_records(df: pl.DataFrame) -> int:
    """Insert records into the database."""
    if len(df) == 0:
        logger.info("No new records to insert")
        return 0
    
    logger.info(f"Inserting {len(df)} records ({len(df.columns)} columns) into database...")
    rows_inserted = db.insert_dataframe(
        df,
        table_name="survey_responses",
        if_exists="append",
        validate=False  # Already validated
    )
    logger.info(f"✓ Successfully inserted {rows_inserted} records")
    return rows_inserted



def main():
    """Main backfill process."""
    try:
        # Find latest data
        latest_dir = find_latest_standardized_dir()
        csv_path = latest_dir / "survey_combined.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(f"survey_combined.csv not found in {latest_dir}")
        
        # Load data
        df = load_survey_data(csv_path)
        
        # Get existing records
        existing_ids = get_existing_unique_ids()
        
        # Filter for new records
        new_df = filter_new_records(df, existing_ids)
        
        if len(new_df) == 0:
            logger.info("No new records to insert. Database is up to date.")
            return
        
        # Validate schema
        validate_schema(new_df)
        
        # Insert records
        rows_inserted = insert_records(new_df)
        
        logger.info("=" * 60)
        logger.info("Backfill complete!")
        logger.info(f"Total records in CSV: {len(df)}")
        logger.info(f"Existing records (skipped): {len(existing_ids)}")
        logger.info(f"New records inserted: {rows_inserted}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
