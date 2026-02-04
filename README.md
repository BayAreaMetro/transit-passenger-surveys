
# Transit Passenger Surveys

A comprehensive Python package for working with standardized transit passenger survey data. This package provides:

1. **Canonical Data Models**: Pydantic-based models defining a standardized schema for transit passenger surveys
2. **Database Tools**: SQLite database with  survey responses from ~26 Bay Area operators
3. **Type Safety**: Full type hints and validation using Pydantic
4. **Codebook Utilities**: Tools to transform operator-specific data into the canonical format

## Overview

Transit agencies conduct passenger surveys with varying formats, variable names, and response categories. This package standardizes these surveys into a common format, making it easier to:

- Combine data from multiple operators
- Perform cross-agency analyses
- Build consistent reporting tools
- Ensure data quality through validation


## Workplan (Last updated: 02/03/2026)
- [x] Create placeholder installable `transit_passenger_surveys` Python package in repo
- [x] Standardized canonical onboard survey schema from `instrument-and-dictionary`
- [x] Create Hive-partitioned database with Pydantic models and version control
- [x] Implement 3-tier field validation (Required/Optional-Enriched/Optional)
- [x] Backfill existing survey data into database (186,792 records from 27 operators, 2012-2023)
- [x] Refactor initialization scripts into modular structure (`initialize_helpers/`)
- [ ] Create Python `requests` scripts to replace R code in `requests` folder
- [ ] Create new survey ingestion workflow
- [ ] Migrate `summaries` code into `transit_passenger_surveys` package

### Legacy code
All of this has been moved into `archive` folder and will be maintained there for reference as they get gradually replaced by python code in the `transit_passenger_surveys` package.

- Survey data collection and processing
    * [Instrument and Dictionary](https://github.com/BayAreaMetro/onboard-surveys/tree/master/instrument-and-dictionary): Survey questions and data collected. *[Last changed 2025]*
    * [Make Uniform](https://github.com/BayAreaMetro/onboard-surveys/tree/master/make-uniform): Process raw data from different surveys and transit agencies into the uniform format. *[Last changed 2025]*
    * [Mode Choice Targets](https://github.com/BayAreaMetro/onboard-surveys/tree/master/mode-choice-targets): Process OBS and generate inputs for mode choice targets. *[Last changed 2016]*
- Survey data analytics
    * [Summaries](https://github.com/BayAreaMetro/onboard-surveys/tree/master/summaries): High-level statistical analysis on survey data in R and Tableau. *[Last changed 2025]*
    * [Decomposition](https://github.com/BayAreaMetro/onboard-surveys/tree/master/decomposition): Route-level statistical analysis on survey data in R and Tableau. *[Last changed 2018]*
    * [Requests](https://github.com/BayAreaMetro/onboard-surveys/tree/master/requests): Analyze survey data to respond data requests from stakeholders. *[Last changed 2025]*
- Exploratory data analytics
    * [Multi-criteria Expansion](https://github.com/BayAreaMetro/onboard-surveys/tree/master/multi-criteria-expansion): Explore ways to ind an optimal set of expansion weights that best satisfy any number of criteria. *[Last changed 2015]*
    * [On-off Explore](https://github.com/BayAreaMetro/onboard-surveys/tree/master/on-off-explore): Explore the value of performing an on/off pre-survey for small operators. *[Last changed 2015]*
    * [Travel Model Priors](https://github.com/BayAreaMetro/onboard-surveys/tree/master/travel-model-priors): Process boarding and alighting data from SF Muni automated passenger counters, boarding and alighting flows from the SFCTA SF-CHAMP travel model, on-to-off surveys on SF Muni, etc. *[Last changed 2016]*

## Database Viewing

You can query the DuckDB cache or Parquet files directly using any tool of your choice. Some options:
- **DBeaver** (GUI): [https://dbeaver.io/](https://dbeaver.io/) - Works with DuckDB, Parquet, and many other formats
- **DuckDB CLI**: Native command-line interface for DuckDB files
- **Python (Polars/DuckDB)**: Read Parquet files or DuckDB tables programmatically



## Installation

```bash
# Install package in development mode
uv sync
```

## Database Setup

### Initialize Database (Single Command)

To create the database, backfill legacy data, and apply corrections:

```bash
uv run python scripts/initialize_database.py
```

This single script will:
1. Create Hive warehouse folder structure (`survey_responses/`, `survey_metadata/`, `survey_weights/`)
2. Run database migrations
3. Backfill all legacy survey data from `standardized_2025-10-13/survey_combined.csv`
   - **186,792 survey responses** from 27 operators (2012-2023)
   - 74 batches (operator/year combinations)
   - Validates all records against Pydantic schema during ingestion
4. Apply data corrections (e.g., fix ACE 2023 commuter_rail_present field)
5. Refresh DuckDB cache for fast querying

**Configuration**: The standardized data directory and CSV filename are set as constants at the top of `scripts/initialize_database.py`:
```python
STANDARDIZED_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized\standardized_2025-10-13")
CSV_FILENAME = "survey_combined.csv"
```

**Note**: The script uses version control - if data hasn't changed, it will skip writing and log "Data unchanged, reusing version X". To force a clean re-initialization, delete the Hive warehouse directory first.

### Data Processing Pipeline

The backfill process (`initialize_helpers/` modules) includes:
- **Data loading** (`backfill_loader.py`): CSV reading with schema inference
- **Transformations** (`backfill_transformations.py`): Type conversions, boolean fields, word-to-number conversion
- **Normalization** (`backfill_normalization.py`): Text standardization, sentence case, acronym handling
- **Typo fixes** (`backfill_typo_fixes.py`): 15+ field-specific value mappings and corrections
- **Validation** (`backfill_validation.py`): Pydantic schema validation with detailed error reporting
- **Ingestion** (`backfill_ingestion.py`): Write to Hive-partitioned Parquet files with versioning

### Manual Steps (Advanced)

If you need to run corrections separately:

```bash
# Fix ACE 2023 commuter rail data
uv run python -c "from initialize_helpers.fix_ace_commuter_rail import fix_ace_commuter_rail; fix_ace_commuter_rail()"

# Refresh DuckDB cache only
uv run python scripts/refresh_cache.py
```

## Database Configuration

The database uses a Hive-partitioned Parquet file structure stored at:
```
\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_transit_data_hive
(accessible via M: drive mapping)
```

**Structure**:
- `survey_responses/` - Partitioned by `operator=<name>/year=<yyyy>/data-<version>-<commit>.parquet`
- `survey_metadata/` - Survey-level information (dates, source, version tracking)
- `survey_weights/` - Response weights (boarding_weight, trip_weight)

**DuckDB Cache**: A DuckDB file at `_transit_data_hive/transit_surveys.duckdb` provides fast querying over the Parquet files.

## Quick Start

### Query Survey Data

```python
from transit_passenger_tools import db

# Read all surveys into Polars DataFrame
df = db.read_table("survey_responses")

# Filter by operator
bart_surveys = db.read_query(
    "SELECT * FROM survey_responses WHERE canonical_operator = 'BART'"
)

# Filter by year and export
bart_2024 = db.read_query(
    "SELECT * FROM survey_responses WHERE survey_year = 2024"
)
bart_2024.write_csv("bart_2024.csv")

# Get summary statistics
summary = db.read_query("""
    SELECT 
        canonical_operator,
        survey_year,
        COUNT(*) as responses,
        AVG(weight) as avg_weight
    FROM survey_responses
    GROUP BY canonical_operator, survey_year
    ORDER BY survey_year DESC, canonical_operator
""")
print(summary)
```

## Data Model Structure

The canonical data model uses a flat structure optimized for analysis:

### 3-Tier Field Classification

**Core Fields (CoreSurveyResponse)** - ~90 vendor-provided fields:
- Survey identifiers (`response_id`, `survey_id`, `original_id`)
- Trip basics (`route`, `direction`, `access_mode`, `egress_mode`, `trip_purp`, `orig_purp`, `dest_purp`)
- Demographics (`gender`, `is_hispanic`, `race`, `work_status`, `student_status`)
- Household (`persons`, `workers`, `vehicles`, `household_income`)
- Coordinates (lat/lon for origin, destination, home, work, school, board/alight locations)
- Transfer information (operator, routes, technology)
- Time and fare (`depart_hour`, `return_hour`, `day_of_the_week`, `fare_medium`, `fare_category`)

**Derived Fields (DerivedSurveyResponse)** - ~110 pipeline-calculated fields:
- Time classifications (`day_part`, `weekpart`, `time_period`)
- Tour logic (`tour_purp`, `tour_purp_case`, at_work/school flags)
- Mode presence flags (commuter_rail, heavy_rail, express_bus, ferry, light_rail)
- Distance calculations (origin-destination, board-alight, access/egress distances)
- Travel model zones (MAZ, TAZ, TAP for TM1/TM2)
- Census geographies (tract, county, PUMA GEOIDs)
- Calculated fields (`boardings`, `auto_to_workers_ratio`, `year_born_four_digit`)

**Complete Schema (SurveyResponse)** - ~200+ total fields:
- All Core + Derived fields
- Reserved for future validation/metadata fields


## Usage Examples

### Basic Queries

```python
from transit_passenger_tools import db
import polars as pl

# Get work trips by access mode
work_trips = db.read_query("""
    SELECT 
        access_mode,
        COUNT(*) as trips,
        SUM(weight) as weighted_trips
    FROM survey_responses
    WHERE dest_purp = 'Work'
    GROUP BY access_mode
    ORDER BY weighted_trips DESC
""")

# Filter by operator and year
caltrain_2023 = db.read_query("""
    SELECT * FROM survey_responses
    WHERE canonical_operator = 'Caltrain'
    AND survey_year = 2023
""")

# Get transfer patterns
transfers = db.read_query("""
    SELECT 
        transfer_from,
        transfer_to,
        COUNT(*) as count
    FROM survey_responses
    WHERE transfer_from IS NOT NULL
    AND transfer_to IS NOT NULL
    GROUP BY transfer_from, transfer_to
    ORDER BY count DESC
""")
```

### Working with Pydantic Models

```python
from transit_passenger_tools.data_models import SurveyResponse
import polars as pl

# Read data
df = db.read_table("survey_responses")

# Validate single record
record_dict = df.row(0, named=True)
try:
    response = SurveyResponse(**record_dict)
    print(f"Valid record: {response.unique_id}")
    print(f"Trip: {response.orig_purp} → {response.dest_purp}")
    print(f"Access: {response.access_mode}, Egress: {response.egress_mode}")
except ValidationError as e:
    print(f"Validation errors: {e}")

# Access optional fields safely
if response.home_lat and response.home_lon:
    print(f"Home location: ({response.home_lat}, {response.home_lon})")

if response.vehicles is not None:
    print(f"Household vehicles: {response.vehicles}")
```

### Analysis Examples

```python
# Calculate mode share by operator
mode_share = db.read_query("""
    SELECT 
        canonical_operator,
        access_mode,
        SUM(weight) as weighted_trips,
        SUM(weight) * 100.0 / SUM(SUM(weight)) OVER (PARTITION BY canonical_operator) as pct
    FROM survey_responses
    GROUP BY canonical_operator, access_mode
    ORDER BY canonical_operator, pct DESC
""")

# Income distribution for work trips
work_income = db.read_query("""
    SELECT 
        household_income,
        COUNT(*) as responses,
        SUM(weight) as weighted
    FROM survey_responses
    WHERE dest_purp = 'Work'
    AND household_income IS NOT NULL
    GROUP BY household_income
    ORDER BY 
        CASE household_income
            WHEN 'Under $10,000' THEN 1
            WHEN '$10,000 to $25,000' THEN 2
            WHEN '$25,000 to $35,000' THEN 3
            -- ... etc
        END
""")

# Transfer rates by operator
transfer_rates = db.read_query("""
    SELECT 
        canonical_operator,
        COUNT(*) as total_trips,
        SUM(CASE WHEN transfer_from IS NOT NULL THEN 1 ELSE 0 END) as transfers,
        SUM(CASE WHEN transfer_from IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as transfer_pct
    FROM survey_responses
    GROUP BY canonical_operator
    ORDER BY transfer_pct DESC
""")
```

### Export and Integration

```python
# Export to CSV for Tableau/Excel
df = db.read_table("survey_responses")
df.write_csv("all_surveys.csv")

# Export filtered subset
bart_work = db.read_query("""
    SELECT * FROM survey_responses
    WHERE canonical_operator = 'BART'
    AND dest_purp = 'Work'
""")
bart_work.write_parquet("bart_work_trips.parquet")

# Convert to pandas for additional analysis
import pandas as pd
df_pandas = df.to_pandas()

# Or work directly with polars
avg_age_by_operator = df.group_by("canonical_operator").agg([
    pl.col("approximate_age").mean().alias("avg_age"),
    pl.col("weight").sum().alias("total_weight")
])
```

## Enumerated Types

The package defines enums for categorical variables to ensure consistency:

- **AccessEgressMode**: Walk, Bike, PNR, KNR, TNC, Transit, Other, Missing
- **TripPurpose**: Home, Work, School, Shopping, Social recreation, Medical/dental, etc.
- **Gender**: Male, Female, Other, Missing
- **Race**: White, Asian, Black/African American, Hispanic/Latino, etc.
- **WorkStatus**: Full- or part-time, Non-worker, Missing
- **StudentStatus**: Full- or part-time, Non-student, Missing
- **FareCategory**: Adult, Youth, Senior, Disabled, Missing
- **SurveyType**: Brief CATI, Tablet PI, Paper-collected, Online, etc.
- **Weekpart**: Weekday, Weekend
- **Direction**: Northbound, Southbound, Eastbound, Westbound, Inbound, Outbound

## Database Management

### Schema Management with Alembic

#### What is Alembic?

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is a database migration tool that manages versioned changes to database schemas. It tracks which changes have been applied and allows you to upgrade or rollback the database structure.

**How we use it:**
- **Schema definition**: Pydantic models in [data_models.py](src/transit_passenger_tools/data_models.py) define the canonical schema with 203 fields using 3-tier validation (Required/Optional-Enriched/Optional)
- **Database DDL**: Alembic migration scripts in `migrations/versions/` contain the actual SQL DDL (`CREATE TABLE`, indices, constraints)
- **Manual migrations**: We write migrations manually (not auto-generated) because we use Pydantic models instead of SQLAlchemy ORM
- **Single source of truth**: The Pydantic models are the authoritative schema definition; Alembic migrations must be kept in sync

When the schema needs to change (new table, column, index), you:
1. Update the Pydantic model in `data_models.py`
2. Create a new Alembic migration with the corresponding DDL
3. Apply the migration to update the database

#### Common Alembic Commands

```powershell
# Apply migrations (create/update schema)
uv run alembic upgrade head

# View migration history
uv run alembic history

# Rollback one migration
uv run alembic downgrade -1
```

### Database Schema

The database contains two main tables:

- **`survey_responses`**: Core survey data (203 fields with 3-tier validation)
  - One row per on-board survey intercept
  - 186,594 responses from 26 operators (2011-2024)
  - Includes trip details, demographics, geography, fare info
  - Primary key: `unique_id`
  - NULL-friendly: 150+ optional fields support sparse data

- **`survey_weights`**: Project-specific weight schemes (planned)
  - Multiple weighting methodologies per survey response
  - Links to `survey_responses` via `unique_id`
  - Allows analysis with different weight adjustments

## Development

### Initial Setup

```powershell
# Install dependencies
uv sync

# Create database
uv run alembic upgrade head

# Inspect database structure
uv run python -c "from transit_passenger_tools import db; db.inspect_database()"
```

