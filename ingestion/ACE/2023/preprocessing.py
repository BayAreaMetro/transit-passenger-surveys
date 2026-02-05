"""Preprocess ACE 2023 survey data for standardization pipeline.

Converts legacy Pandas preprocessing to Polars, replicating exact transformations
to validate new pipeline against existing database records.

Based on: archive/make-uniform/production/preprocess/preprocessing_ACE_2023.py
"""

import logging
from pathlib import Path

import geopandas as gpd
import polars as pl

from transit_passenger_tools.pipeline.validate import validate_enum_values

logger = logging.getLogger(__name__)

# Constants from legacy script (use UNC paths)
ACE_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\ACE\2023")
ACE_XLSX = ACE_DIR / "ACE Onboard Data (sent 7.7.23).xlsx"
RIDERSHIP_CSV = ACE_DIR / "April 2023 Monthly Performance Report.csv"
ZCTA_SHAPEFILE = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\Census\Geography\2020_ZCTAs\2020_ZCTAs.shp"
)

# Age category to birth year mapping
AGE_CAT_TO_YEAR_BORN = {
    1: 2008,  # Under 18, 2023-15 = 2008
    2: 2002,  # 18-24, 2023-21 = 2002
    3: 1993,  # 25-34, 2023-30 = 1993
    4: 1983,  # 35-44, 2023-40 = 1983
    5: 1973,  # 45-54, 2023-50 = 1973
    6: 1965,  # 55-61, 2023-58 = 1965
    7: 1960,  # 62-64, 2023-63 = 1960
    8: 1953,  # 65+,   2023-70 = 1953
}

# Train number to survey date mapping (from report Table 3 - verified)
TRAIN_NUMBER_TO_SURVEY_DATE = {
    1: "2023-04-20",  # ACE 04 (Thursday)
    2: "2023-04-19",  # ACE 06 (Wednesday)
    3: "2023-04-18",  # ACE 08 (Tuesday)
    4: "2023-04-17",  # ACE 10 (Monday)
}

# Gender mapping (codebook enum values)
GENDER_MAPPING = {
    1: "Female",
    2: "Male",
    3: "Non-binary",
    4: "Other",
}

# Access mode mapping (codebook enum values)
# Based on ACE 2023 survey instrument
ACCESS_MODE_MAPPING = {
    1: "PNR",  # Arrived in a car and left it parked at the station
    2: "KNR",  # Got dropped off by a family member or friend
    3: "Transit",  # ACE Shuttle (at Great America Station)
    4: "Transit",  # BART Connection via Wheels Commuter rail
    5: "Transit",  # Amtrak train
    6: "Transit",  # Local bus
    7: "KNR",  # Taxi (legacy categorizes as KNR, not TNC)
    8: "TNC",  # App-based rideshare
    9: "Walk",  # Walked/wheelchair
    10: "Bike",  # Biked/scootered
    11: "Other",  # Other
}

# Egress mode mapping (codebook enum values)
# Based on ACE 2023 survey instrument
EGRESS_MODE_MAPPING = {
    1: "PNR",  # Drove away in a car parked at the station
    2: "KNR",  # Picked up by a family member or friend
    3: "Transit",  # Local bus
    4: "Transit",  # Amtrak train
    5: "KNR",  # Taxi (legacy categorizes as KNR, not TNC)
    6: "TNC",  # App-based rideshare
    7: "Walk",  # Walked/wheelchair
    8: "Bike",  # Biked/scootered
    9: "Other",  # Other
}

# Language mapping
LANGUAGE_BINARY_MAPPING = {
    1: "ENGLISH ONLY",
}

# Base estimate is 15:00 (3 PM) for ACE 04
TRAIN_TIME_OFFSET = {
    1: 0,  # ACE 04
    2: 1,  # ACE 06
    3: 2,  # ACE 08
    4: 3,  # ACE 10
}

# Station code to name mapping (from ACE schedule/report)
STATION_MAPPING = {
    1: "SAN JOSE",
    2: "SANTA CLARA",
    3: "GREAT AMERICA",
    4: "FREMONT",
    5: "PLEASANTON",
    6: "LIVERMORE",
    7: "VASCO ROAD",
    8: "TRACY",
    9: "LATHROP / MANTECA",
    10: "STOCKTON",
}


def load_zcta_centroids() -> pl.DataFrame:
    """Load ZCTA shapefile and compute centroids.

    Returns:
        Polars DataFrame with columns: ZCTA, home_lat, home_lon
    """
    logger.info("Loading ZCTA shapefile from %s", ZCTA_SHAPEFILE)

    # Load shapefile
    zcta_gdf = gpd.read_file(ZCTA_SHAPEFILE)
    logger.info("Loaded %s ZCTAs", f"{len(zcta_gdf):,}")

    # Ensure GEOID is treated as string
    zcta_gdf["ZCTA"] = zcta_gdf["GEOID"].astype(str)

    # Reproject to projected CRS for accurate centroid calculation
    zcta_gdf = zcta_gdf.to_crs(epsg=26910)

    # Replace geometry with centroids (in projected CRS)
    zcta_gdf = zcta_gdf.copy()
    zcta_gdf["geometry"] = zcta_gdf.geometry.centroid

    # Reproject centroids to lat/lon
    zcta_gdf = zcta_gdf.to_crs(epsg=4326)

    # Extract lon/lat
    zcta_gdf["home_lon"] = zcta_gdf.geometry.x
    zcta_gdf["home_lat"] = zcta_gdf.geometry.y

    # Create ZIP-to-centroid lookup table
    zcta_centroids = zcta_gdf[["ZCTA", "home_lat", "home_lon"]]

    # Convert to Polars
    return pl.from_pandas(zcta_centroids)


def preprocess(  # noqa: PLR0915
    survey_path: str | Path = ACE_XLSX,
    canonical_operator: str = "ACE",
    survey_year: int = 2023,
) -> pl.DataFrame:
    """Preprocess ACE 2023 survey data.

    Exactly replicates legacy preprocessing logic to validate pipeline.

    Args:
        survey_path: Path to ACE Excel file
        canonical_operator: Canonical operator name
        survey_year: Survey year

    Returns:
        Polars DataFrame in CoreSurveyResponse format
    """
    survey_path = Path(survey_path)
    logger.info("Reading survey data from %s", survey_path)

    # Read Excel file
    df = pl.read_excel(
        survey_path, sheet_name="ACE Onboard Data Weighted 7.7.2", infer_schema_length=10000
    )
    logger.info("Read %s records from %s", f"{len(df):,}", survey_path)

    # Log access/egress "other" values (legacy had this diagnostic)
    logger.info("Access mode value counts:")
    access_counts = df.group_by(["access", "access_other"]).len().sort("len", descending=True)
    logger.info("Found %s unique access/access_other combinations", len(access_counts))
    logger.info("Egress mode value counts:")
    egress_counts = df.group_by(["egress", "egress_other"]).len().sort("len", descending=True)
    logger.info("Found %s unique egress/egress_other combinations", len(egress_counts))

    # ========== TRANSFORMATIONS (matching legacy exactly) ==========

    # 1. Age category → year_born_four_digit
    df = df.with_columns(
        [
            pl.col("age")
            .replace_strict(AGE_CAT_TO_YEAR_BORN, default=None)
            .alias("year_born_four_digit")
        ]
    )

    # 2. Gender - map numeric codes to codebook Gender enum values
    df = df.with_columns(
        [pl.col("gender").replace_strict(GENDER_MAPPING, default=None).alias("gender")]
    )

    # 3. Access and egress mode - map numeric codes to codebook enum values
    df = df.with_columns(
        [
            pl.col("access").replace_strict(ACCESS_MODE_MAPPING, default=None).alias("access"),
            pl.col("egress").replace_strict(EGRESS_MODE_MAPPING, default=None).alias("egress"),
        ]
    )

    # 4. Language binary (lang=1 is "ENGLISH ONLY", else "OTHER")
    df = df.with_columns(
        [
            pl.col("lang")
            .replace_strict(LANGUAGE_BINARY_MAPPING, default="OTHER")
            .alias("lang_binary")
        ]
    )

    # 5. Direction (all trains surveyed were eastbound) - use codebook Direction enum value
    df = df.with_columns([pl.lit("Eastbound").alias("direction")])

    # 5b. Boardings (ACE is single-leg survey, no transfers)
    df = df.with_columns([pl.lit(1).alias("boardings")])

    # 6. Survey date and day of week from train_number
    # Validate train_number is expected value
    invalid_trains = df.filter(~pl.col("train_number").is_in([1, 2, 3, 4]))
    if len(invalid_trains) > 0:
        logger.warning("Found %s records with unexpected train_number values", len(invalid_trains))
        logger.warning(
            "Unique train_number values: %s",
            invalid_trains.select(["train_number"]).unique()["train_number"].to_list(),
        )

    # Map train_number to survey_date
    df = df.with_columns(
        [
            pl.col("train_number")
            .replace_strict(TRAIN_NUMBER_TO_SURVEY_DATE, default=None)
            .alias("survey_date")
        ]
    )

    # Derive day_of_the_week from survey_date - use codebook DayOfWeek enum values (title case)
    df = df.with_columns(
        [pl.col("survey_date").str.to_date("%Y-%m-%d").dt.strftime("%A").alias("day_of_the_week")]
    )

    # 7. Calculate survey time estimate based on boarding location
    # Survey time estimation constants (hour of day)
    df = df.with_columns(
        [
            pl.when(pl.col("board") >= 8)  # noqa: PLR2004
            .then(pl.lit(17))
            .when(pl.col("board") >= 4)  # noqa: PLR2004
            .then(pl.lit(16))
            .otherwise(pl.lit(15))
            .alias("survey_time_estimate")
        ]
    )
    # Shift hour for later trains
    df = df.with_columns(
        [
            (
                pl.col("survey_time_estimate")
                + pl.col("train_number").replace_strict(TRAIN_TIME_OFFSET, default=0)
            ).alias("survey_time_estimate")
        ]
    )
    # Convert to time string and rename to survey_time (pipeline expects this field name)
    df = df.with_columns(
        [(pl.col("survey_time_estimate").cast(pl.Utf8) + ":00:00").alias("survey_time")]
    ).drop("survey_time_estimate")

    # 8. Convert home_zip to home_lat, home_lon via ZCTA centroids
    logger.info("Geocoding home ZIP codes to lat/lon")

    # Convert home_zip to string with leading zeros
    df = df.with_columns(
        [
            pl.col("home_zip")
            .fill_null(0)
            .cast(pl.Int64)
            .cast(pl.Utf8)
            .str.zfill(5)
            .alias("home_zip")
        ]
    )

    # Load ZCTA centroids
    zcta_centroids = load_zcta_centroids()

    # Join with ZCTA centroids
    df = df.join(zcta_centroids, left_on="home_zip", right_on="ZCTA", how="left")

    # Log geocoding success rate
    geocoded_count = df.filter(pl.col("home_lat").is_not_null()).height
    logger.info(
        "Geocoded %s / %s home locations (%.1f%%)",
        f"{geocoded_count:,}",
        f"{len(df):,}",
        100 * geocoded_count / len(df),
    )

    # 9. Weight calculation from ridership data
    logger.info("Calculating weights from ridership data")

    # Load ridership CSV
    ridership_df = pl.read_csv(RIDERSHIP_CSV)
    apr_value_2023 = (
        ridership_df.filter(
            pl.col("Fiscal_Year") == 2023  # noqa: PLR2004
        )
        .select("APR")
        .item()
    )

    logger.info("April 2023 monthly ridership: %s", f"{apr_value_2023:,}")

    # Calculate average daily ridership (20 weekdays in April 2023)
    days = 20
    num_records = len(df)
    naive_weight = (apr_value_2023 / days) / num_records

    logger.info("Naive weight (expansion factor): %.6f", naive_weight)

    # Rename existing "weight" to "initial_weight"
    # Multiply initial_weight by naive_weight to get final weight
    df = df.with_columns(
        [
            pl.col("weight").alias("initial_weight"),
            (pl.col("weight") * naive_weight).alias("weight"),
        ]
    )

    # Log weight statistics
    logger.info(
        "Weight sum: %.2f (target: %.2f)",
        df.select(pl.col("weight").sum()).item(),
        apr_value_2023 / days,
    )

    # ========== FIELD MAPPING TO SCHEMA ==========

    # Map station codes to station names for route generation
    df = df.with_columns(
        [
            pl.col("board")
            .replace_strict(STATION_MAPPING, default=None)
            .alias("onoff_enter_station"),
            pl.col("alight")
            .replace_strict(STATION_MAPPING, default=None)
            .alias("onoff_exit_station"),
        ]
    )

    # Convert vehicles and workers from 1-indexed (Excel) to 0-indexed (database)
    # Excel has 1=0 vehicles, 2=1 vehicle, etc.
    df = df.with_columns(
        [
            (pl.col("veh") - 1).alias("veh"),
            (pl.col("hh_emp") - 1).alias("hh_emp"),
        ]
    )

    # Rename fields to match CoreSurveyResponse schema
    df = df.rename(
        {
            "d_lat": "dest_lat",
            "d_lng": "dest_lon",
            "id": "original_id",
            "veh": "vehicles",
            "hh_emp": "workers",
            "access": "access_mode",
            "egress": "egress_mode",
        }
    )

    # Add required metadata fields
    df = df.with_columns(
        [
            pl.lit(canonical_operator).alias("canonical_operator"),
            pl.lit(survey_year).alias("survey_year"),
            pl.lit(canonical_operator).alias("survey_name"),  # Legacy uses operator name only
            # Generate response_id and survey_id
            (
                pl.col("original_id").cast(pl.Utf8)
                + "___"
                + pl.lit(canonical_operator)
                + "___"
                + pl.lit(str(survey_year))
            ).alias("response_id"),
            pl.lit(f"{canonical_operator}_{survey_year}").alias("survey_id"),
            # Vehicle technology (ACE is commuter rail)
            pl.lit("Commuter Rail").alias("vehicle_tech"),
        ]
    )

    # Map race checkboxes to race_dmy_* fields for demographics module
    # ACE 2023 codebook mapping:
    # race_1=Black, race_2=Indigenous, race_3=Asian, race_4=White,
    # race_5=Pacific Islander, race_6/race_other=Other
    df = df.with_columns(
        [
            pl.col("race_1").cast(pl.Int32).fill_null(0).alias("race_dmy_blk"),  # Black
            pl.col("race_2").cast(pl.Int32).fill_null(0).alias("race_dmy_ind"),  # Indigenous
            pl.col("race_3").cast(pl.Int32).fill_null(0).alias("race_dmy_asn"),  # Asian
            pl.col("race_4").cast(pl.Int32).fill_null(0).alias("race_dmy_wht"),  # White
            pl.col("race_5")
            .cast(pl.Int32)
            .fill_null(0)
            .alias("race_dmy_hwi"),  # Hawaiian/Pacific Islander
            # ACE 2023 doesn't collect Middle Eastern separately
            pl.lit(0).alias("race_dmy_mdl_estn"),  # Middle Eastern (not collected)
            # Map race_6 checkbox or race_other text to race_other_string
            pl.when((pl.col("race_6") == 1) | (pl.col("race_other").is_not_null()))
            .then(pl.col("race_other").cast(pl.Utf8))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("race_other_string"),
        ]
    )

    # Keep race_1 through race_5 and race_other as-is for debugging/reference
    # The canonical pipeline's demographics module will derive the race field from race_dmy_* fields

    # ========== ENUM VALIDATION AND CLEANUP ==========

    # Convert "Missing" strings to null for all enum fields that exist
    # Only process string columns to avoid type comparison errors
    enum_fields = [
        "access_mode",
        "egress_mode",
        "direction",
        "gender",
        "race",
        "work_status",
        "student_status",
        "vehicle_tech",
    ]

    for field in enum_fields:
        if field in df.columns and df[field].dtype == pl.Utf8:
            df = df.with_columns(
                [
                    pl.when(pl.col(field) == "Missing")
                    .then(None)
                    .otherwise(pl.col(field))
                    .alias(field)
                ]
            )

    # Validate enum values
    logger.info("Validating enum fields...")

    enum_errors = validate_enum_values(df)
    if enum_errors:
        error_msg = "\n".join(enum_errors)
        msg = f"Enum validation failed:\n{error_msg}"
        raise ValueError(msg)

    logger.info("[PASS] All enum fields validated successfully")

    logger.info("Preprocessing complete: %s records", f"{len(df):,}")

    return df


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Run preprocessing
    df = preprocess()

    # Display sample
    print("\nSample of preprocessed data:")
    print(df.head())

    # Display columns
    print(f"\nColumns ({len(df.columns)}):")
    print(df.columns)
