"""Airport Transit Data Request.

Asana Task:
https://app.asana.com/1/11860278793487/project/1211760862991332/task/1212780679760302?focus=true

transit trip origins for trips destined to the airport (weighted, unweighted)
by worker vs air passenger

Search for BART, AC Transit, SamTrans trips to/from SFO, OAK, SJC airports
"""

import polars as pl

from transit_passenger_tools.database import connect

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
lon_buffer = PROXIMITY_KM / 85   # ~0.024 degrees

airport_conditions = []
for (lat, lon) in AIRPORTS.values():
    condition = f"""(
        (orig_lat BETWEEN {lat - lat_buffer} AND {lat + lat_buffer}
         AND orig_lon BETWEEN {lon - lon_buffer} AND {lon + lon_buffer})
        OR
        (dest_lat BETWEEN {lat - lat_buffer} AND {lat + lat_buffer}
         AND dest_lon BETWEEN {lon - lon_buffer} AND {lon + lon_buffer})
    )"""
    airport_conditions.append(condition)

query = f"""
    SELECT *
    FROM survey_responses
    WHERE operator IN ({','.join(f"'{op}'" for op in OPERATORS)})
    AND ({' OR '.join(airport_conditions)})
"""  # noqa: S608

# We execute the query over duckdb and convert to polars dataframe
df = conn.execute(query).pl()
conn.close()

# Assign airport trip type based on origin/destination proximity using Polars expressions
# Create conditions for each airport
conditions = []
for airport, (lat, lon) in AIRPORTS.items():
    # Destination is near airport
    dest_near = (
        (pl.col("dest_lat") - lat).abs() <= lat_buffer
    ) & (
        (pl.col("dest_lon") - lon).abs() <= lon_buffer
    )
    # Origin is near airport
    orig_near = (
        (pl.col("orig_lat") - lat).abs() <= lat_buffer
    ) & (
        (pl.col("orig_lon") - lon).abs() <= lon_buffer
    )
    conditions.append((dest_near, f"to_{airport}"))
    conditions.append((orig_near, f"from_{airport}"))

# Build when-then chain
airport_type = pl.lit("non_airport_trip")
for condition, value in reversed(conditions):
    airport_type = pl.when(condition).then(pl.lit(value)).otherwise(airport_type)

df = df.with_columns(airport_type.alias("airport_trip_type"))

