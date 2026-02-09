"""Airport Transit Data Request.

Asana Task:
https://app.asana.com/1/11860278793487/project/1211760862991332/task/1212780679760302?focus=true

transit trip origins for trips destined to the airport (weighted, unweighted)
by worker vs air passenger

Search for BART, AC Transit, SamTrans trips to/from SFO, OAK, SJC airports
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from transit_passenger_tools.database import connect
from transit_passenger_tools.database.export import export_df_to_hyper
from transit_passenger_tools.schemas.codebook import TripPurpose

# Output DIR
OUTPUT_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\Airport Data")


# Target operators
OPERATORS = ["BART", "AC TRANSIT", "SAMTRANS"]

# Airport coordinates (lat, lon) and proximity threshold
AIRPORTS = {
    "SFO": (37.6213, -122.3790),  # San Francisco International
    "OAK": (37.7126, -122.2197),  # Oakland International
    "SJC": (37.3639, -121.9289),  # San Jose International
}
PROXIMITY_KM = 2  # Consider trips within 2km of airport as airport trips

# Load data from database using Polars with DuckDB connection
# Filter in SQL for efficiency using bounding boxes for each airport
conn = connect(read_only=True)

# Build SQL query with lat/lon filters
# Approximate: 1 degree latitude ≈ 111km, 1 degree longitude ≈ 85km at Bay Area latitude
lat_buffer = PROXIMITY_KM / 111  # ~0.018 degrees
lon_buffer = PROXIMITY_KM / 85  # ~0.024 degrees

# Build SQL query with spatial filter and airport classification in a single pass
case_conditions = []

for airport, (lat, lon) in AIRPORTS.items():
    # Airport classification conditions (also serves as spatial filter)
    case_conditions.append(f"""
        WHEN (dest_lat BETWEEN {lat - lat_buffer} AND {lat + lat_buffer}
              AND dest_lon BETWEEN {lon - lon_buffer} AND {lon + lon_buffer})
        THEN 'to_{airport}'
        WHEN (orig_lat BETWEEN {lat - lat_buffer} AND {lat + lat_buffer}
              AND orig_lon BETWEEN {lon - lon_buffer} AND {lon + lon_buffer})
        THEN 'from_{airport}'""")

# Query with airport classification
query = f"""
    SELECT
        *,
        CASE
            {" ".join(case_conditions)}
            ELSE 'non_airport_trip'
        END as airport_trip_type
    FROM survey_responses
    WHERE operator IN ({",".join(f"'{op}'" for op in OPERATORS)})
"""  # noqa: S608

# Separate query for weights
weights_query = f"""
    SELECT response_id, boarding_weight, trip_weight
    FROM survey_weights
    WHERE weight_scheme = 'baseline'
    AND response_id IN (
        SELECT DISTINCT response_id FROM survey_responses
        WHERE operator IN ({",".join(f"'{op}'" for op in OPERATORS)})
    )
"""  # noqa: S608


# Execute queries and close connection
df = conn.execute(query).pl()
weights_df = conn.execute(weights_query).pl()
conn.close()

# Filter to only airport trips (remove 'non_airport_trip' rows)
df = df.filter(pl.col("airport_trip_type") != "non_airport_trip")

# Drop fields that are entirely NULL to clean up the dataset
# (e.g., if some fields are missing from older surveys)
df = df.select([col for col in df.columns if not df[col].is_null().all()])

# Distribution of airport trips by purpose
print(df.group_by("trip_purp", "year").agg(pl.count()).sort("count", descending=True))


# Drop work trips to focus on air passenger trips
df_nonwork = df.filter(
    ~pl.col("trip_purp").is_in(
        [
            TripPurpose.WORK.value,
            TripPurpose.WORK_RELATED.value,
            TripPurpose.AT_WORK.value,
            TripPurpose.BUSINESS_APT.value,
        ],
        nulls_equal=True,
    )
)

# Save the resulting DataFrame to Parquet for further analysis
# Get month/year of today
month_year = datetime.now(tz=UTC).strftime("%Y-%m")
df_nonwork.write_parquet(OUTPUT_DIR / f"airport_trips - {month_year}.parquet")

# Export as .hyper file for Tableau with separate tables for trips and weights
hyper_path = OUTPUT_DIR / f"airport_trips - {month_year}.hyper"

# Filter weights to only those with airport non-work trips
airport_weights = weights_df.filter(
    pl.col("response_id").is_in(df_nonwork["response_id"].implode())
)

export_df_to_hyper({"trips": df_nonwork, "weights": airport_weights}, hyper_path)
