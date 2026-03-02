# Transit Passenger Surveys

Standardized data warehouse and processing pipeline for Bay Area transit on-board passenger surveys. Combines data from 27+ operators (2012–2024) into a single Parquet-based warehouse with Pydantic-validated schemas, a 7-module derivation pipeline, and geocoding to travel-model geographies.

## Workplan (Last updated: 2026-03-02)

- [x] Installable `transit_passenger_tools` Python package
- [x] Canonical schema via Pydantic models (`CoreSurveyResponse` → `DerivedSurveyResponse` → `SurveyResponse`)
- [x] Flat Parquet data warehouse with archive versioning
- [x] Backfill 186,792 legacy records (27 operators, 2012–2023, 74 surveys) from standardized CSV
- [x] 7-module derivation pipeline (date/time, demographics, transfers, path labels, tour purpose, auto sufficiency, geocoding)
- [x] Convention-based operator ingestion (`ingestion/operators/{Operator}/{year_YYYY}/ingest.py`)
- [x] BART 2024 ingestion with full geocoding (48 geography columns)
- [x] `--rebuild` CLI for targeted and full rebuilds
- [x] Tableau Hyper export
- [x] Post-ingestion data corrections (ACE commuter rail, BART heavy rail / station names / routes)
- [x] Unit tests for all 7 pipeline modules
- [ ] **TODO**: PUMS demographic comparison generator — port legacy R function `create_PUMS_data_in_TPS_format()` to Python so new survey years automatically get PUMS population benchmarks (existing legacy PUMS rows are preserved via backfill)
- [ ] Migrate `summaries` R code into Python / Tableau
- [ ] Additional operator ingestion scripts (beyond BART 2024)
- [ ] Canonical route database -- matching could be done with GTFS but how to track-changes over time? Could maintain a canonical route table with effective date ranges, updated by operator ingestion scripts when route changes are detected. I don't know yet, this is complicated...

### Legacy Code

Archived in `archive/` for reference. Key legacy components:

| Folder | Description | Status |
|--------|-------------|--------|
| `make-uniform/` | R scripts to standardize raw survey data | Replaced by `ingestion/` |
| `summaries/` | R + Tableau statistical summaries | Not yet migrated |
| `decomposition/` | Route-level analysis in R + Tableau | Not yet migrated |
| `requests/` | Ad-hoc data request scripts (R) | Being replaced by `scripts/` |
| `multi-criteria-expansion/` | Expansion weight optimization (R) | Reference only |

## Installation

```bash
uv sync
```

## Data Warehouse

Three flat Parquet files at `DATA_ROOT` (configured in `config/pipeline.yaml`):

```
\\models.ad.mtc.ca.gov\..\_transit_data\
├── survey_responses.parquet   # ~187K+ response records, ~160 columns
├── survey_weights.parquet     # Boarding + trip weights per response
├── survey_metadata.parquet    # Survey-level metadata (field dates, source)
├── surveys_export.hyper       # Tableau Hyper with all 3 tables
└── archive/                   # Date-stamped snapshots
```

### Querying

#### Python
```python
import polars as pl
from transit_passenger_tools.database import DATA_ROOT

# Read all responses
df = pl.read_parquet(DATA_ROOT / "survey_responses.parquet")

# Filter to one operator
bart = df.filter(pl.col("survey_id").str.starts_with("BART_"))
```

See [docs/database_access.md](docs/database_access.md) for more examples.

#### Tableau
Connect directly to the Parquet files or use the `surveys_export.hyper` for optimized Tableau access.

#### SQL (DuckDB) / DBeaver

DuckDB can query Parquet files directly:

```sql
SELECT *
FROM read_parquet('\\\\models.ad.mtc.ca.gov\\..\\_transit_data\\survey_responses.parquet')
WHERE survey_id LIKE 'BART_%';
```

## Rebuilding the Database

The entry point is `ingestion/rebuild_database.py`:

```bash
# Append-only — skip operator/years already present
uv run python -m ingestion.rebuild_database

# Re-ingest specific operator/year combos (deletes then re-ingests)
uv run python -m ingestion.rebuild_database --rebuild BART/2024

# Full wipe + rebuild (requires interactive confirmation)
uv run python -m ingestion.rebuild_database --rebuild ALL

# List discovered ingestion sources and their status
uv run python -m ingestion.rebuild_database --list
```

The rebuild process:

1. **Legacy backfill** — loads `survey_combined.csv` (186,792 records), applies transformations / normalization / typo fixes, writes to Parquet in operator/year batches
2. **Operator ingestion** — discovers `ingestion/operators/{Operator}/{year_YYYY}/ingest.py` modules via convention, runs each `main()` which calls the 7-module pipeline
3. **Data corrections** — applies post-ingestion fixes (`ingestion/fixes/`)
4. **Tableau export** — writes `surveys_export.hyper`

### Adding a New Survey

1. Create `ingestion/operators/{Operator}/{year_YYYY}/preprocessing.py` — vendor-specific data cleaning
2. Create `ingestion/operators/{Operator}/{year_YYYY}/ingest.py` with a `main()` returning `(responses_df, weights_df, metadata_df)`
3. Run `uv run python -m ingestion.rebuild_database` — only the new survey is appended
4. Or `uv run python -m ingestion.rebuild_database --rebuild Operator/YYYY` to force re-ingest

## Pipeline Architecture

### Schema (Pydantic Models)

Defined in `src/transit_passenger_tools/models.py`:

| Model | Fields | Role |
|-------|--------|------|
| `CoreSurveyResponse` | ~90 | Vendor-provided fields (preprocessing must produce these) |
| `DerivedSurveyResponse` | ~70 | Pipeline-calculated fields added by the 7 modules |
| `SurveyResponse` | ~160 | Complete schema = Core + Derived |
| `SurveyMetadata` | 10 | Survey-level metadata (dates, source, operator) |
| `SurveyWeight` | 5 | Per-response boarding and trip weights |

### Derivation Pipeline

`src/transit_passenger_tools/pipeline/process_survey.py` orchestrates 7 transform modules:

| Module | Derives |
|--------|---------|
| `date_time.py` | `day_part`, `weekpart`, `day_of_the_week` from `survey_time` |
| `auto_sufficiency.py` | `autos_vs_workers`, `vehicle_numeric_cat`, `worker_numeric_cat` |
| `demographics.py` | `race` consolidation, `household_income_category` |
| `transfers.py` | `boardings`, `transfers_surveyed`, technology presence flags |
| `path_labels.py` | `path_access`, `path_egress`, `path_line_haul`, `path_label`, `transit_type` |
| `tour_purpose.py` | `tour_purp`, `tour_purp_case` |
| `geocoding.py` | 48 geography columns — MAZ/TAZ/TAP (TM1, TM2), tract/county/PUMA GEOIDs, distances |

### Backfill Modules

`ingestion/backfill/` handles loading the legacy standardized CSV:

| Module | Role |
|--------|------|
| `backfill_loader.py` | CSV reading, column renaming, type conversion |
| `backfill_transformations.py` | Boolean fields, PUMS placeholders, optional field defaults |
| `backfill_normalization.py` | Sentence case, acronym/operator name standardization |
| `backfill_typo_fixes.py` | 15+ field-specific value corrections |
| `backfill_validation.py` | Pydantic schema validation with error collection |
| `backfill_ingestion.py` | Batch writing to Parquet, metadata/weight extraction |

### Post-Ingestion Corrections

`ingestion/fixes/` applies known data quality fixes after all records are written:

- `fix_ace_2023.py` — sets `commuter_rail_present=1` for ACE (single-ride commuter rail)
- `fix_bart_2015.py` — fixes `heavy_rail_present`, airport station names, route field normalization

## Configuration

`config/pipeline.yaml` defines:

- **`data_root`**: Network path to the Parquet warehouse
- **`shapefiles`**: Paths to TM1/TM2 TAZ, MAZ, county, tract, PUMA, and station shapefiles
- **`remote_name`**: UNC fallback when `M:` drive is not mapped
- **`zone_types`**: Maps output field names → shapefile keys and column names for geocoding

## Testing

```bash
uv run pytest tests/
```

Tests cover all 7 pipeline modules with fixture-based validation.

## Project Structure

```
transit-passenger-surveys/
├── src/transit_passenger_tools/     # Installable package
│   ├── models.py                    # Pydantic schema (Core → Derived → SurveyResponse)
│   ├── codebook.py                  # Enums (AccessEgressMode, Purpose, Race, etc.)
│   ├── database.py                  # Parquet I/O, validation, Hyper export
│   ├── config.py                    # YAML config loader, shapefile path resolution
│   └── pipeline/                    # 7 derivation modules + orchestrator
├── ingestion/
│   ├── rebuild_database.py          # CLI entry point (--rebuild, --list)
│   ├── backfill/                    # Legacy CSV → Parquet (7 modules)
│   ├── fixes/                       # Post-ingestion corrections
│   └── operators/                   # Convention-based operator ingest scripts
│       └── BART/year_2024/          # Example: BART 2024 (preprocessing + ingest)
├── config/
│   ├── pipeline.yaml                # Paths, shapefiles, zone configs
│   └── pipeline.test.yaml           # Test configuration
├── tests/                           # pytest suite for pipeline modules
├── scripts/                         # Utility and exploration scripts
├── docs/                            # Additional documentation
├── archive/                         # Legacy R code (reference only)
└── pyproject.toml
```

## Development

```powershell
# Install dependencies
uv sync
```

