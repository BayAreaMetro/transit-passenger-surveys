# Accessing the Survey Database

## Connection Details

**Database Path:** `\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\surveys.duckdb`

**Database Type:** DuckDB

## Read-Only Access (Required for Querying)

**Important: DuckDB Concurrency Model**
- DuckDB allows **only ONE read-write connection** at a time
- DuckDB allows **multiple read-only connections** simultaneously
- **Always use read-only mode** when querying or analyzing data

If you don't connect in read-only mode:
- You'll lock out other users from accessing the database
- You'll block administrative operations (view refreshes, data ingestion)
- You may accidentally modify the data

**Bottom line:** Always connect in read-only mode unless you're specifically performing data ingestion or administrative tasks.

### DBeaver

1. Create a new DuckDB connection
2. Set the database path to the UNC path above
3. **Important:** Configure read-only access to allow multiple concurrent users:
   - Go to "Edit Connection" → "Driver properties" tab
   - Click "Add" to add a new property
   - Set Property: `access_mode`
   - Set Value: `read_only`
   - Click OK to save

**Why this matters:**
- Read-only mode allows multiple users to query simultaneously
- Prevents accidental data modifications
- Avoids blocking administrative operations (view refreshes, data ingestion)

### Python (Manual Queries)

```python
import duckdb

# Read-only connection
conn = duckdb.connect(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\surveys.duckdb",
    read_only=True
)

# Query the data
df = conn.execute("SELECT * FROM survey_responses LIMIT 10").df()
conn.close()
```

### Using the Library

```python
from transit_passenger_tools import db

# Read-only session (safe for concurrent use)
with db.read_session() as conn:
    df = conn.execute("SELECT * FROM survey_responses LIMIT 10").df()
```

## Write Access (Administrative Only)

**Write operations should only be performed by data pipeline scripts**, not by end users.

Operations requiring write access:
- Ingesting new survey data
- Refreshing views (`scripts/refresh_views.py`)
- Running backfill (`ingestion/backfill_database_from_legacy.py`)

**Important:** Only one process can have write access at a time. If you see:
```
IO Error: Cannot open file ... The process cannot access the file because it is being used by another process.
```

This means:
1. Someone has the database open in a query tool without read-only mode, OR
2. Another administrative operation is running

**Solution:** Close all database connections, run the administrative operation, then reconnect.

## Available Views

### survey_responses
Latest version of survey responses for each operator/year combination. Use this for analysis.

```sql
SELECT * FROM survey_responses LIMIT 10;
```

### survey_responses_all_versions
All versions of survey data, including historical versions. Includes `data_version` and `data_commit` columns.

```sql
SELECT * FROM survey_responses_all_versions 
WHERE operator = 'BART' AND year = 2024
ORDER BY data_version DESC;
```

### survey_metadata
Survey-level metadata (dates, sample sizes, etc.)

```sql
SELECT * FROM survey_metadata;
```

### survey_weights
Survey weights for expansion and analysis

```sql
SELECT * FROM survey_weights LIMIT 10;
```

## Common Queries

### Count responses by operator and year
```sql
SELECT operator, year, COUNT(*) as responses
FROM survey_responses
GROUP BY operator, year
ORDER BY year DESC, operator;
```

### Get BART 2024 data with weights
```sql
SELECT 
    r.*,
    w.boarding_weight,
    w.trip_weight
FROM survey_responses r
LEFT JOIN survey_weights w ON r.response_id = w.response_id
WHERE r.operator = 'BART' AND r.year = 2024
LIMIT 100;
```

### Check for schema issues
```sql
-- See all columns and their types
DESCRIBE survey_responses;

-- Check for NULL values in key fields
SELECT 
    COUNT(*) as total,
    COUNT(response_id) as has_response_id,
    COUNT(is_hispanic) as has_hispanic,
    COUNT(race) as has_race
FROM survey_responses;
```

## Troubleshooting

### "Binder Error: Contents of view were altered"

The Parquet file schema has changed. Run:
```powershell
# Close all database connections first!
uv run python scripts\refresh_views.py
```

### "IO Error: Cannot open file"

Someone has a write lock on the database. Close all connections and try again.

### Slow queries

DuckDB is very fast for analytical queries. If queries are slow:
1. Make sure you're filtering by `operator` and `year` to use partition pruning
2. Consider adding indexes (contact data team)
3. Check if you're accidentally querying `survey_responses_all_versions` instead of `survey_responses`
