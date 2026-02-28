"""Airport Transit Data Request.

Asana Task:
https://app.asana.com/1/11860278793487/project/1211760862991332/task/1212780679760302?focus=true

transit trip origins for trips destined to the airport (weighted, unweighted)
by worker vs air passenger

Search for BART, AC Transit, SamTrans trips to/from SFO, OAK, SJC airports
"""

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import polars as pl

from transit_passenger_tools.codebook import Purpose
from transit_passenger_tools.database import DATA_ROOT, export_to_hyper

# Output DIR
OUTPUT_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\Airport Data")


# Load super district shapefile
SHP_DIR = Path(
    r"\\model3-a\Model3A-Share\travel-model-one-master\utilities"
    r"\geographies\taz1454_SUPERD_dissolve.shp"
)

# Load shapefile with geopandas
superdistricts = gpd.read_file(SHP_DIR)

# Project to WGS84 for spatial joins with lat/lon points
superdistricts = superdistricts.to_crs(epsg=4326)

# Load data from Parquet files using Polars
responses = pl.read_parquet(DATA_ROOT / "survey_responses.parquet")
weights = pl.read_parquet(DATA_ROOT / "survey_weights.parquet")

# Filter and join
trips = (
    responses.filter(
        pl.col("operator").is_in(["BART", "AC TRANSIT", "SAMTRANS"])
        & (
            pl.col("onoff_enter_station").is_in(
                ["San Francisco International Airport", "Oakland International Airport"]
            )
            | pl.col("onoff_exit_station").is_in(
                ["San Francisco International Airport", "Oakland International Airport"]
            )
        )
    )
    .join(
        weights.filter(pl.col("weight_scheme") == "baseline").select(
            "response_id", "boarding_weight", "trip_weight"
        ),
        on="response_id",
        how="left",
    )
)

# Save the resulting DataFrame to Parquet for further analysis
# Get month/year of today
month_year = datetime.now(tz=UTC).strftime("%Y-%m")

# Export as .hyper file for Tableau with separate tables for trips and weights
hyper_path = OUTPUT_DIR / f"airport_trips - {month_year}.hyper"

# since those are not air passengers
df_nonwork = trips.filter(
    ~(
        # Exclude: work trip TO airport (exit station is airport)
        (
            (pl.col("orig_purp") == Purpose.WORK.value)
            | (pl.col("orig_purp") == Purpose.WORK_RELATED.value)
        )
        & pl.col("orig_purp").is_not_null()  # Only filter if purpose is known
        & pl.col("onoff_exit_station").is_in(
            ["San Francisco International Airport", "Oakland International Airport"]
        )
    )
    & ~(
        # Exclude: work trip FROM airport (enter station is airport)
        (
            (pl.col("dest_purp") == Purpose.WORK.value)
            | (pl.col("dest_purp") == Purpose.WORK_RELATED.value)
        )
        & pl.col("dest_purp").is_not_null()  # Only filter if purpose is known
        & pl.col("onoff_enter_station").is_in(
            ["San Francisco International Airport", "Oakland International Airport"]
        )
    )
)


# Join with superdistricts to get superdistrict of origin and destination
df_trips = df_nonwork.select(
    "response_id", "orig_lat", "orig_lon", "dest_lat", "dest_lon"
).to_pandas()

gdf_origins = gpd.GeoDataFrame(
    df_trips[["response_id"]],
    geometry=gpd.points_from_xy(df_trips["orig_lon"], df_trips["orig_lat"]),
    crs="EPSG:4326",
)
gdf_dests = gpd.GeoDataFrame(
    df_trips[["response_id"]],
    geometry=gpd.points_from_xy(df_trips["dest_lon"], df_trips["dest_lat"]),
    crs="EPSG:4326",
)

# Perform spatial join, convert to polars and join back to original DataFrame
for gdf, prefix in [(gdf_origins, "orig"), (gdf_dests, "dest")]:
    joined = gpd.sjoin(gdf, superdistricts[["SUPERD", "geometry"]], how="left", predicate="within")
    joined_polars = pl.from_pandas(
        joined[["response_id", "SUPERD"]].rename(columns={"SUPERD": f"{prefix}_superd"})
    ).with_columns(pl.col(f"{prefix}_superd").cast(pl.Int64))
    df_nonwork = df_nonwork.join(joined_polars, on="response_id", how="left")


# Check that we have >2015 bart data
if (
    df_nonwork.filter(
        (pl.col("operator") == "BART") & (pl.col("year") > 2015)  # noqa: PLR2004
    ).shape[0]
    == 0
):
    msg = "No BART data found for years > 2015"
    raise ValueError(msg)

# Print a table of counts of trips by trips FROM airports
print(
    df_nonwork.filter(
        pl.col("onoff_enter_station").is_in(
            ["San Francisco International Airport", "Oakland International Airport"]
        )
    )
    .group_by("dest_superd")
    .agg(pl.sum("boarding_weight"), pl.sum("trip_weight"), pl.len())
    .sort("dest_superd", descending=True)
)
# Print a table of counts of trips by trips TO airports
print(
    df_nonwork.filter(
        pl.col("onoff_exit_station").is_in(
            ["San Francisco International Airport", "Oakland International Airport"]
        )
    )
    .group_by("orig_superd")
    .agg(pl.sum("boarding_weight"), pl.sum("trip_weight"), pl.len())
    .sort("orig_superd", descending=True)
)


# Create a combined weight

# Calculate scaling factor by year to account for scale differences
# factor_year_i = sum(trip_weight_year_i) / (sum(trip_weight_year_i) + sum(boarding_weight_year_i))
# Do this for both boarding and trip weight to get a combined weight that accounts for both

# Calculate year-level totals
year_totals = df_nonwork.group_by("year").agg(
    pl.sum("trip_weight").alias("total_trip_weight"),
    pl.sum("boarding_weight").alias("total_boarding_weight"),
)

# Calculate scaling factors for each year
year_totals = year_totals.with_columns(
    (
        pl.col("total_trip_weight")
        / (pl.col("total_trip_weight") + pl.col("total_boarding_weight"))
    ).alias("trip_scale_factor"),
    (
        pl.col("total_boarding_weight")
        / (pl.col("total_trip_weight") + pl.col("total_boarding_weight"))
    ).alias("boarding_scale_factor"),
)

# Join scaling factors back to main dataframe and calculate combined weights
df_nonwork = df_nonwork.join(
    year_totals.select("year", "trip_scale_factor", "boarding_scale_factor"), on="year", how="left"
)

df_nonwork = df_nonwork.with_columns(
    (pl.col("trip_weight") * pl.col("trip_scale_factor")).alias("trip_weight_combined"),
    (pl.col("boarding_weight") * pl.col("boarding_scale_factor")).alias("boarding_weight_combined"),
)

# Drop intermediate calculation columns if desired
df_nonwork = df_nonwork.drop("trip_scale_factor", "boarding_scale_factor")


export_to_hyper(hyper_path, {"trips": df_nonwork})
