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

from transit_passenger_tools.database import connect
from transit_passenger_tools.database.export import export_df_to_hyper
from transit_passenger_tools.schemas.codebook import TripPurpose

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

# Load data from database using Polars with DuckDB connection
# Filter in SQL for efficiency using bounding boxes for each airport
conn = connect(read_only=True)

# Get all trips for target operators and years with lat/lon fields for spatial filtering
trips = conn.execute(
    """
    SELECT
        sr.* EXCLUDE (sr.trip_weight),
        sw.boarding_weight,
        sw.trip_weight
    FROM survey_responses sr
    LEFT JOIN survey_weights sw
        ON sr.response_id = sw.response_id
        AND sw.weight_scheme = 'baseline'
    WHERE sr.operator IN ('BART', 'AC TRANSIT', 'SAMTRANS')
    AND (
        sr.onoff_enter_station IN (
            'San Francisco International Airport',
            'Oakland International Airport'
        )
        OR
        sr.onoff_exit_station IN (
            'San Francisco International Airport',
            'Oakland International Airport'
        )
    )
    """,
).pl()

# Save the resulting DataFrame to Parquet for further analysis
# Get month/year of today
month_year = datetime.now(tz=UTC).strftime("%Y-%m")

# Export as .hyper file for Tableau with separate tables for trips and weights
hyper_path = OUTPUT_DIR / f"airport_trips - {month_year}.hyper"

# since those are not air passengers
df_nonwork = trips.filter(
    ~(
        ((pl.col("orig_purp") == TripPurpose.WORK.value) | pl.col("orig_purp").is_null())
        & (
            pl.col("onoff_exit_station").is_in(
                ["San Francisco International Airport", "Oakland International Airport"]
            )
        )
    )
    & ~(
        ((pl.col("dest_purp") == TripPurpose.WORK.value) | pl.col("dest_purp").is_null())
        & (
            pl.col("onoff_enter_station").is_in(
                ["San Francisco International Airport", "Oakland International Airport"]
            )
        )
    )
)

# add to_airport and from_airport filtered categories as for easier analysis in Tableau
df_nonwork = df_nonwork.with_columns()

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
    .agg(pl.sum("boarding_weight"), pl.sum("trip_weight"), pl.count())
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
    .agg(pl.sum("boarding_weight"), pl.sum("trip_weight"), pl.count())
    .sort("orig_superd", descending=True)
)

export_df_to_hyper({"trips": df_nonwork}, hyper_path)
