"""Export SuperDistrict OD flows by tour purpose and vehicle tech to one CSV.

Requires Python and Polars (pip install polars).
Edit the settings below, then run:
    python summarize_superdistrict_od.py

Only responses with both OD SuperDistricts are included. Weight columns are
summed within each group; an entirely missing weight remains blank, not zero.
"""

from pathlib import Path

import polars as pl

# Query settings and output location. Generated data stays out of Git.
DATA_ROOT = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data"
)
RESPONSES_FILE = DATA_ROOT / "survey_responses.parquet"
WEIGHTS_FILE = DATA_ROOT / "survey_weights.parquet"
SURVEY_NAME = "Regional Snapshot"
SURVEY_YEAR = 2023
WEIGHT_SCHEME = "baseline"
OUTPUT_DIR = Path(
    r"\\models.ad.mtc.ca.gov\data\models"
    r"\Development\Travel Model One\Calibration\Version 1.7\2023_snapshot_survey"
    )
OUTPUT_FILE = "regional_snapshot_2023_superdistrict_od_by_tour_purpose_by_vehicle_tech_revised.csv"

# The names of the superdistricts, used to add human-readable labels to the OD flows
NAMES_FILE = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\GIS layers\TM1_taz"
    r"\superdistrict_names.csv"
)


def main() -> None:
    """Read the warehouse, group OD flows, and export the CSV."""
    responses = (
        pl.scan_parquet(RESPONSES_FILE)
        .filter(
            (pl.col("survey_name") == SURVEY_NAME)
            & (pl.col("survey_year") == SURVEY_YEAR)
        )
        .select(
            "response_id", "tour_purp", "vehicle_tech",
            "orig_superdistrict", "dest_superdistrict",
        )
        .with_columns(
            pl.col("tour_purp").fill_null("Missing"),
            pl.col("vehicle_tech").fill_null("Missing"),
        )
    )
    weights = (
        pl.scan_parquet(WEIGHTS_FILE)
        .filter(pl.col("weight_scheme") == WEIGHT_SCHEME)
        .select("response_id", "boarding_weight", "trip_weight")
    )
    names = pl.read_csv(NAMES_FILE).select(
        pl.col("SUPERD").cast(pl.Int64), "SD_NAME"
    )
    records = responses.join(
        weights, on="response_id", how="left", validate="m:1"
    ).collect()
    if records.is_empty():
        msg = f"No responses found for {SURVEY_NAME}, {SURVEY_YEAR}"
        raise ValueError(msg)
    if records["boarding_weight"].null_count():
        msg = f"Some responses have no boarding weight for {WEIGHT_SCHEME}"
        raise ValueError(msg)

    flows = (
        records.filter(
            pl.col("orig_superdistrict").is_not_null()
            & pl.col("dest_superdistrict").is_not_null()
        )
        .group_by("tour_purp", "vehicle_tech", "orig_superdistrict", "dest_superdistrict")
        .agg(
            pl.len().alias("response_count"),
            pl.col("boarding_weight").sum().alias("weighted_boardings"),
        )
        .join(
            names.rename({"SUPERD": "orig_superdistrict", "SD_NAME": "origin_name"}),
            on="orig_superdistrict", how="left", validate="m:1",
        )
        .join(
            names.rename({"SUPERD": "dest_superdistrict", "SD_NAME": "destination_name"}),
            on="dest_superdistrict", how="left", validate="m:1",
        )
        .select(
            "tour_purp", "vehicle_tech", "orig_superdistrict", "origin_name",
            "dest_superdistrict", "destination_name", "response_count",
            "weighted_boardings",
        )
        .sort(
            "tour_purp", "vehicle_tech", "weighted_boardings",
            "orig_superdistrict", "dest_superdistrict",
            descending=[False, False, True, False, False],
        )
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / OUTPUT_FILE
    flows.write_csv(output_path)
    print(f"Wrote {len(flows):,} flow groups to {output_path}")


if __name__ == "__main__":
    main()
