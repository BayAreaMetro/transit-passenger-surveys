"""Test if DuckDB views work properly after cleaning columns."""

import duckdb

conn = duckdb.connect(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard"
    r"\Data and Reports\_transit_data_hive\surveys.duckdb",
    read_only=True
)

result = conn.execute("""
    SELECT operator, year, COUNT(*) as count
    FROM survey_responses
    WHERE operator IN ('BART', 'AC TRANSIT')
    GROUP BY operator, year
    ORDER BY operator, year
""").fetchall()

for _operator, _year, _count in result:
    pass

columns = [desc[0] for desc in conn.execute("SELECT * FROM survey_responses LIMIT 0").description]
raw_columns = [
    c for c in columns
    if c.upper() in ["RIDE_BART_FREQ", "COMMENT", "ACCESS_BIKE_ONBART", "ID", "SURVEY"]
]

if raw_columns:
    pass
else:
    pass


conn.close()
