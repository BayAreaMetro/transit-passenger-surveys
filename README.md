
# Transit Passenger Surveys

Tools for processing and analyzing transit on-board passenger surveys.

## Database Configuration

The canonical survey database is stored at:
```
\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized\transit_surveys.db
(accessible via M: drive mapping)
```

This path is configured in `alembic.ini`. To change the database location, update the `sqlalchemy.url` setting.

## Usage

### Query Survey Data

```python
from transit_passenger_tools import db

# Read all surveys into Polars DataFrame
df = db.read_table("survey_responses")

# Filter by operator
bart_surveys = db.read_query(
    "SELECT * FROM survey_responses WHERE operator = 'BART'"
)

# Export to CSV using Polars
bart_2024 = db.read_query(
    "SELECT * FROM survey_responses WHERE survey_year = 2024"
)
bart_2024.write_csv("bart_2024.csv")
```

### Database Management

#### What is Alembic?

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is a database migration tool that manages versioned changes to database schemas. It tracks which changes have been applied and allows you to upgrade or rollback the database structure.

**How we use it:**
- **Schema definition**: Pydantic models in [data_models.py](src/transit_passenger_tools/data_models.py) define the canonical schema with validation rules
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

- **`survey_responses`**: Core survey data (115 fields)
  - One row per on-board survey intercept
  - Includes trip details, demographics, geography, fare info
  - Primary key: `unique_id`

- **`survey_weights`**: Project-specific weight schemes
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

## Workplan (Last updated: 01/13/2026)
- [x] Create placeholder installable `transit_passenger_surveys` Python package in repo
- [x] Standardized canonical onboard survey schema from `instrument-and-dictionary`
- [x] Create SQLite database with Pydantic models and Alembic migrations
- [ ] Create Python `requests` scripts to replace R code in `requests` folder
- [ ] Backfill existing survey data into database
- [ ] Create new survey ingestion workflow
- [ ] Migrate `summaries` code into `transit_passenger_surveys` package



## Legacy code
All of this has been moved into `archive` folder and will be maintained there for reference as they get gradually replaced by python code in the `transit_passenger_surveys` package.

### Survey data collection and processing

* [Instrument and Dictionary](https://github.com/BayAreaMetro/onboard-surveys/tree/master/instrument-and-dictionary): Survey questions and data collected. *[Last changed 2025]*

* [Make Uniform](https://github.com/BayAreaMetro/onboard-surveys/tree/master/make-uniform): Process raw data from different surveys and transit agencies into the uniform format. *[Last changed 2025]*

* [Mode Choice Targets](https://github.com/BayAreaMetro/onboard-surveys/tree/master/mode-choice-targets): Process OBS and generate inputs for mode choice targets. *[Last changed 2016]*


### Survey data analytics

* [Summaries](https://github.com/BayAreaMetro/onboard-surveys/tree/master/summaries): High-level statistical analysis on survey data in R and Tableau. *[Last changed 2025]*

* [Decomposition](https://github.com/BayAreaMetro/onboard-surveys/tree/master/decomposition): Route-level statistical analysis on survey data in R and Tableau. *[Last changed 2018]*

* [Requests](https://github.com/BayAreaMetro/onboard-surveys/tree/master/requests): Analyze survey data to respond data requests from stakeholders. *[Last changed 2025]*

### Exploratory data analytics

* [Multi-criteria Expansion](https://github.com/BayAreaMetro/onboard-surveys/tree/master/multi-criteria-expansion): Explore ways to ind an optimal set of expansion weights that best satisfy any number of criteria. *[Last changed 2015]*
* [On-off Explore](https://github.com/BayAreaMetro/onboard-surveys/tree/master/on-off-explore): Explore the value of performing an on/off pre-survey for small operators. *[Last changed 2015]*

* [Travel Model Priors](https://github.com/BayAreaMetro/onboard-surveys/tree/master/travel-model-priors): Process boarding and alighting data from SF Muni automated passenger counters, boarding and alighting flows from the SFCTA SF-CHAMP travel model, on-to-off surveys on SF Muni, etc. *[Last changed 2016]*