# Transit Passenger Survey Data Models

A comprehensive Python package for working with standardized transit passenger survey data. This package provides:

1. **Canonical Data Models**: Pydantic-based models defining a standardized schema for transit passenger surveys
2. **Codebook Utilities**: Tools to transform survey operator-specific data into the canonical format
3. **Type Safety**: Full type hints and validation using Pydantic

## Overview

Transit agencies conduct passenger surveys with varying formats, variable names, and response categories. This package standardizes these surveys into a common format, making it easier to:

- Combine data from multiple operators
- Perform cross-agency analyses
- Build consistent reporting tools
- Ensure data quality through validation

## Installation

```bash
pip install -e .
```

## Data Model Structure

The canonical data model organizes survey data into logical components:

### Core Record Structure

```python
TransitPassengerSurveyRecord
├── metadata: SurveyMetadata          # Survey administration info
├── person: PersonDemographics         # Respondent demographics
├── household: HouseholdCharacteristics # Household characteristics
├── activity_locations: ActivityLocations # Home/work/school locations
├── trip: TripCharacteristics          # Trip origin, destination, purpose
├── transit: TransitJourney            # Transit routing and transfers
└── fare: FarePayment                  # Fare payment information
```

### Key Components

**SurveyMetadata**
- Unique record ID
- Survey type (phone, paper, online)
- Weekday/weekend
- Expansion weight

**PersonDemographics**
- Gender, age, race/ethnicity
- Language proficiency
- Work and student status

**HouseholdCharacteristics**
- Household size
- Number of workers
- Number of vehicles
- Income bracket

**TripCharacteristics**
- Origin and destination locations
- Trip purposes
- Access and egress modes
- Trip timing

**TransitJourney**
- Boarding and alighting locations
- Route information
- Transfer details

## Usage Examples

### Basic Usage: Loading and Transforming Data

```python
from transit_passenger_tools import create_transformer
import pandas as pd

# Load survey data
df = pd.read_csv("ac_transit_2018_survey.csv")

# Create transformer with the data dictionary
transformer = create_transformer("Dictionary_for_Standard_Database.csv")

# Transform to canonical format
records = transformer.transform_dataframe(
    survey_name="AC Transit",
    survey_year=2018,
    df=df
)

# Access structured data
for record in records:
    print(f"Trip from {record.trip.orig_purp} to {record.trip.dest_purp}")
    print(f"Access mode: {record.trip.access_mode}")
    print(f"Route: {record.transit.route}")
```

### Working with Individual Records

```python
from transit_passenger_tools import TransitPassengerSurveyRecord, SurveyMetadata

# Access record components
record = records[0]

# Demographics
if record.person:
    print(f"Gender: {record.person.gender}")
    print(f"Work status: {record.person.work_status}")
    print(f"Interview language: {record.person.interview_language}")

# Household
if record.household:
    print(f"Household size: {record.household.persons}")
    print(f"Income: {record.household.household_income}")
    print(f"Vehicles: {record.household.vehicles}")

# Trip details
if record.trip:
    print(f"Origin: {record.trip.orig_purp}")
    print(f"Destination: {record.trip.dest_purp}")
    print(f"Access mode: {record.trip.access_mode}")
    print(f"Egress mode: {record.trip.egress_mode}")
```

### Exporting to Different Formats

```python
# Convert to dictionary
record_dict = record.model_dump()

# Convert to JSON
import json
json_data = record.model_dump_json(indent=2)

# Convert multiple records to DataFrame
import pandas as pd

data = [record.model_dump() for record in records]
df_canonical = pd.json_normalize(data)
```

### Using the Codebook Directly

```python
from transit_passenger_tools import load_codebook

# Load the codebook
codebook = load_codebook("Dictionary_for_Standard_Database.csv")

# Map individual values
generic_var, generic_resp = codebook.map_value(
    survey_name="AC Transit",
    survey_year=2018,
    survey_variable="final_suggested_access_mode",
    survey_response="Personal Bike"
)
# Returns: ("access_mode", "bike")

# Find canonical variable name
canonical_name = codebook.get_generic_variable_name(
    survey_name="ACE",
    survey_year=2019,
    survey_variable="Access_mode_final"
)
# Returns: "access_mode"

# Find survey-specific variables for canonical variable
survey_vars = codebook.get_survey_variables_for_generic(
    survey_name="AC Transit",
    survey_year=2018,
    generic_variable="access_mode"
)
# Returns: ["final_suggested_access_mode"]
```

### Filtering and Analysis

```python
# Filter by trip purpose
work_trips = [r for r in records if r.trip and r.trip.dest_purp == "work"]

# Filter by access mode
bike_access = [
    r for r in records 
    if r.trip and r.trip.access_mode == "bike"
]

# Filter by household income
high_income = [
    r for r in records
    if r.household and r.household.household_income in [
        "$100,000 to $150,000",
        "$150,000 or higher"
    ]
]

# Calculate weighted statistics
import numpy as np

weights = [r.metadata.weight for r in records if r.metadata.weight]
total_ridership = np.sum(weights)
```

### Validation and Data Quality

```python
from pydantic import ValidationError

# Pydantic automatically validates data
try:
    record = transformer.transform_row(
        survey_name="AC Transit",
        survey_year=2018,
        row=survey_row
    )
except ValidationError as e:
    print(f"Validation errors: {e}")

# Check for missing data
def check_completeness(record):
    """Check which fields are populated."""
    return {
        "has_demographics": record.person is not None,
        "has_household": record.household is not None,
        "has_trip": record.trip is not None,
        "has_origin": record.trip.origin_location is not None if record.trip else False,
        "has_destination": record.trip.destination_location is not None if record.trip else False,
    }

completeness = [check_completeness(r) for r in records]
```

## Enumerated Types

The package defines enums for categorical variables to ensure consistency:

- **AccessMode / EgressMode**: bike, walk, knr, tnc, pnr, other
- **TripPurpose**: home, work, school, shopping, etc.
- **Gender**: male, female, other, missing
- **HouseholdIncome**: Income brackets from "under $10,000" to "$150,000 or higher"
- **WorkStatus / StudentStatus**: full- or part-time, non-worker/non-student
- **FareCategory**: adult, youth, senior, disabled
- **SurveyType**: brief_cati, full_paper, tablet_pi
- **Weekpart**: WEEKDAY, WEEKEND
- **InterviewLanguage**: ENGLISH, SPANISH, CHINESE, etc.

## Data Dictionary Format

The crosswalk dictionary CSV should have this structure:

```csv
Survey_Name,Survey_year,Survey_Variable,Survey_Response,Generic_Variable,Generic_Response
AC Transit,2018,final_suggested_access_mode,Personal Bike,access_mode,bike
AC Transit,2018,final_suggested_access_mode,BIKE SHARE,access_mode,bike
ACE,2019,Access_mode_final,2,access_mode,bike
```

Where:
- **Survey_Name**: Operator name (e.g., "AC Transit", "BART", "ACE")
- **Survey_year**: Year of the survey
- **Survey_Variable**: Survey-specific variable name
- **Survey_Response**: Survey-specific response value
- **Generic_Variable**: Canonical variable name
- **Generic_Response**: Canonical response value (or "NONCATEGORICAL" for continuous variables)

## Contributing

When adding new surveys:

1. Add mappings to the Dictionary_for_Standard_Database.csv
2. Ensure survey-specific variables map to existing canonical variables
3. Add new canonical variables to data_models.py if needed
4. Update enums if new response categories are needed

## License

See LICENSE file for details.
