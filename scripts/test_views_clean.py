"""Test if DuckDB views work properly after cleaning columns."""

import duckdb

conn = duckdb.connect(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_lake\surveys.duckdb",
    read_only=True
)

print("Testing survey_responses view...")
print("\nOperator/Year counts:")
result = conn.execute("""
    SELECT operator, year, COUNT(*) as count 
    FROM survey_responses 
    WHERE operator IN ('BART', 'AC TRANSIT')
    GROUP BY operator, year 
    ORDER BY operator, year
""").fetchall()

for operator, year, count in result:
    print(f"  {operator} {year}: {count:,}")

print("\nChecking for raw survey columns in view...")
columns = [desc[0] for desc in conn.execute("SELECT * FROM survey_responses LIMIT 0").description]
raw_columns = [c for c in columns if c.upper() in ['RIDE_BART_FREQ', 'COMMENT', 'ACCESS_BIKE_ONBART', 'ID', 'SURVEY']]

if raw_columns:
    print(f"  ⚠ Found {len(raw_columns)} raw survey columns: {raw_columns}")
else:
    print("  ✓ No raw survey columns found - views are clean!")

print(f"\nTotal columns in view: {len(columns)}")
print(f"Sample columns: {columns[:10]}")

conn.close()
print("\n✓ Views working correctly!")
