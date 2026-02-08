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

## Tableau Access (No DuckDB Setup Required)

**For Tableau users who don't want to install/configure DuckDB**, we provide two export formats with different structures:

### Export File Locations

**Hyper Extract (Recommended for Tableau - Relational):**
`\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\surveys_export.hyper`

**Parquet File (Flat file for other tools):**
`\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\surveys_export.parquet`

### What's Included in Exports

**Hyper File (Relational Structure):**
The .hyper file preserves the relational database structure with **three separate tables**:

1. **survey_responses** - All response fields (220K+ rows)
2. **survey_weights** - Weight schemes with columns:
   - `response_id` (FK to survey_responses)
   - `weight_scheme` (e.g., 'expansion', 'onboard', 'linked')
   - `boarding_weight`, `trip_weight`
3. **survey_metadata** - Survey-level info:
   - `survey_id` (PK), `survey_name`, `source`
   - `field_start`, `field_end` (data collection dates)
   - `inflation_year`, `processing_notes`

**Relationships** (Tableau will auto-detect these):
- `survey_responses.response_id` = `survey_weights.response_id` (many-to-many)
- `survey_responses.survey_id` = `survey_metadata.survey_id` (many-to-one)

**Parquet File (Single Flattened Table):**
The .parquet file contains a **fully joined dataset** combining all three tables:
- All survey_responses columns
- Pivoted survey_weights (weight schemes become separate columns):
  - `weight_expansion_boarding` / `weight_expansion_trip`
  - `weight_onboard_boarding` / `weight_onboard_trip`
  - `weight_linked_boarding` / `weight_linked_trip`
- Survey metadata fields (joined by survey_id)

**Key benefit of Hyper:** Preserves relational structure for flexible analysis
**Key benefit of Parquet:** Pre-joined for simple tool compatibility

### Using in Tableau Desktop

**With Hyper File (Relational):**
1. Open Tableau Desktop
2. Connect to Data → Files → More... → "Tableau Hyper"
3. Navigate to the `surveys_export.hyper` file on the network share
4. Set up relationships between tables (Tableau usually auto-detects):
   - survey_responses ← survey_weights (on response_id)
   - survey_responses → survey_metadata (on survey_id)
5. Build visualizations using fields from any table!

**With Parquet File (Flat):**
1. Open Tableau Desktop
2. Connect to Data → Files → "Parquet File"
3. Navigate to the `surveys_export.parquet` file
4. Start building - everything is already joined!

**Performance note:** The .hyper format is Tableau's native format and provides the best query performance with full relational capabilities.

### Using the Parquet File

The `.parquet` file can be opened in:
- Tableau Desktop (Connect → "Parquet File")
- Python (pandas, polars, DuckDB)
- Power BI (via Parquet connector)
- Excel (via Power Query)
- Any tool with Parquet support

### When Are Exports Refreshed?

Export files are **automatically regenerated** when:
- Running `python scripts/refresh_cache.py` (default behavior)
- Ingesting new survey data (triggers cache refresh)

To manually regenerate exports:
```powershell
uv run python scripts\export_tableau_cache.py
```

### Weight Column Usage

**In Hyper File (Relational):**
Weights are in a separate `survey_weights` table with multiple rows per response. Filter by `weight_scheme` in your analysis:

```
// Weighted boardings using expansion weights
SUM(IF [Weight Scheme] = "expansion" THEN [Boarding Weight] END)

// Or create a parameter to select weight scheme
SUM(IF [Weight Scheme] = [Selected Weight Scheme] THEN [Boarding Weight] END)
```

**In Parquet File (Flat):**
Weights are pre-pivoted into separate columns for direct use:

```
// Weighted boardings by operator
SUM([weight_expansion_boarding])

// Weighted trips (accounting for transfers)
SUM([weight_expansion_trip])

// Filter to weighted responses only
[weight_expansion_boarding] IS NOT NULL
```

### Differences from Direct DuckDB Access

| Feature | Hyper (Relational) | Parquet (Flat) | Direct DuckDB |
|---------|-------------------|----------------|---------------|
| **Setup** | None - just open file | None - just open file | Requires DuckDB connector |
| **Structure** | 3 tables with relationships | Single joined table | 3+ tables with SQL JOINs |
| **Data freshness** | Refreshed with cache | Refreshed with cache | Real-time access |
| **Joins** | Create in Tableau | Pre-joined | Manual SQL JOINs |
| **Concurrency** | File-based (no limits) | File-based (no limits) | Read-only mode required |
| **Performance** | Optimized for Tableau | Good for most tools | Optimized for SQL |
| **Weight format** | Filterable by scheme | Pivoted columns | Multiple rows per response |
| **Flexibility** | High - custom relationships | Limited - fixed schema | Highest - full SQL |

**Recommendation:**
- Use **Hyper** for Tableau - best performance and flexibility
- Use **Parquet** for other tools or simple flat-file needs
- Use **Direct DuckDB** for Python/SQL analysis and custom queries

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
