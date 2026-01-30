"""Database utilities for transit passenger survey data lake.

This package provides a modular interface for interacting with the survey data lake:
- helpers: Low-level utilities (git, hashing, connections)
- validation: Schema enforcement and integrity checking
- manager: High-level operations (ingestion, views, migrations)

Example usage:
    from transit_passenger_tools.database import (
        ingest_survey_batch,
        query,
        create_views,
    )
"""

import polars as pl

# Re-export public helpers
from .helpers import (
    DATA_LAKE_ROOT,
    DUCKDB_PATH,
    LOCK_FILE,
    check_git_clean,
    compute_dataframe_hash,
    connect,
    get_git_commit,
    get_latest_metadata,
    get_next_version,
    query,
    write_session,
)

# Re-export public manager functions
from .manager import (
    create_views,
    get_row_count,
    get_table_info,
    ingest_survey_batch,
    ingest_survey_metadata,
    ingest_survey_weights,
    inspect_database,
    run_migrations,
)

# Re-export public validation functions
from .validation import (
    enforce_dataframe_types,
    validate_dataframe_schema,
    validate_referential_integrity,
)

# Schema version - increment when making breaking changes to data_models.py
# When incrementing, you MUST run migration script before ingesting new data
SCHEMA_VERSION = 1  # v1: Initial schema

# Python type to Polars type mapping (shared constant)
python_to_polars = {
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    str: pl.Utf8,
}

__all__ = [
    "DATA_LAKE_ROOT",
    "DUCKDB_PATH",
    "LOCK_FILE",
    "SCHEMA_VERSION",
    "check_git_clean",
    "compute_dataframe_hash",
    "connect",
    "create_views",
    "enforce_dataframe_types",
    "get_git_commit",
    "get_latest_metadata",
    "get_next_version",
    "get_row_count",
    "get_table_info",
    "ingest_survey_batch",
    "ingest_survey_metadata",
    "ingest_survey_weights",
    "inspect_database",
    "python_to_polars",
    "query",
    "run_migrations",
    "validate_dataframe_schema",
    "validate_referential_integrity",
    "write_session",
]
