# Accessing the Survey Data

## File Layout

All data lives as flat Parquet files at the root of the data directory:

```
\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\
+-- survey_responses.parquet     # All survey response records (includes survey metadata columns)
+-- survey_weights.parquet       # Expansion / weighting records
+-- surveys_export.hyper         # Tableau Hyper (relational, 2 tables)
+-- archive/                     # Date-stamped copies of previous files
    +-- survey_responses_2026-02-27.parquet
    +-- ...
```

When new data is ingested the existing file is moved to ``archive/``
with a date-stamped name, and the new combined file replaces it.

## Reading Data with Python (Polars)

```python
import polars as pl

# Read the full tables
responses = pl.read_parquet(r"\\...\survey_responses.parquet")
weights   = pl.read_parquet(r"\\...\survey_weights.parquet")

# Join responses with weights
joined = responses.join(
    weights.filter(pl.col("weight_scheme") == "expansion"),
    on="response_id",
    how="left",
)

# Filter to a single operator/year
bart_2024 = responses.filter(
    (pl.col("canonical_operator") == "BART") & (pl.col("survey_year") == 2024)
)
```

### Common Operations

**Count responses by operator and year:**
```python
responses.group_by("canonical_operator", "survey_year").len().sort("survey_year", descending=True)
```

**Get weighted trip counts:**
```python
(
    responses.join(
        weights.filter(pl.col("weight_scheme") == "expansion"),
        on="response_id",
        how="left",
    )
    .group_by("canonical_operator", "survey_year")
    .agg(
        pl.len().alias("unweighted"),
        pl.sum("boarding_weight").alias("weighted_boardings"),
        pl.sum("trip_weight").alias("weighted_trips"),
    )
)
```

## Tableau Access (No Python Required)

### Hyper File (Recommended)

**Path:**
``\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive\surveys_export.hyper``

The ``.hyper`` file contains two relational tables:

| Table | Description |
|-------|------------|
| **survey_responses** | All response fields including survey metadata (220K+ rows) |
| **survey_weights** | ``response_id``, ``weight_scheme``, ``boarding_weight``, ``trip_weight`` |

**Setting up in Tableau Desktop:**

1. Connect to Data > Files > More... > "Tableau Hyper"
2. Navigate to the ``.hyper`` file on the network share
3. Create relationships between tables:
   - ``survey_responses.response_id`` = ``survey_weights.response_id``
4. Build visualisations using fields from any table

### Weight Columns in Tableau

Weights are in the ``survey_weights`` table with multiple rows per response.
Filter by ``weight_scheme`` in your analysis:

```
// Weighted boardings using expansion weights
SUM(IF [Weight Scheme] = "expansion" THEN [Boarding Weight] END)
```

### Refreshing Exports

Export files are **not** automatically regenerated during ingestion.
To refresh the Hyper file after new data has been ingested:

```powershell
uv run python scripts/export_hyper.py
```

## Archive Folder

Each time data is re-ingested the previous file is moved to ``archive/``
with a date stamp, e.g. ``survey_responses_2026-02-27.parquet``. If the
same table is re-ingested more than once on the same day the filename
receives a counter suffix (``_2``, ``_3``, ...).

These archived copies are kept indefinitely for auditability. They can
be safely deleted to reclaim space if no longer needed.
