"""Database utilities for transit passenger survey data lake."""

import logging
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl
from pydantic import ValidationError

from transit_passenger_tools.data_models import SurveyMetadata, SurveyResponse

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized\data_lake"
)
DUCKDB_PATH = DATA_LAKE_ROOT / "surveys.duckdb"
LOCK_FILE = DATA_LAKE_ROOT / ".surveys.lock"
LOCK_FILE = DATA_LAKE_ROOT / ".surveys.lock"


def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection to surveys database."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def query(sql: str) -> pl.DataFrame:
    """Execute SQL query and return Polars DataFrame."""
    conn = connect(read_only=True)
    try:
        return conn.execute(sql).pl()
    finally:
        conn.close()


@contextmanager
def write_session():
    """Context manager for safe write operations with file-based locking."""
    lock_acquired = False
    try:
        if LOCK_FILE.exists():
            raise RuntimeError("Write lock already held by another process")

        LOCK_FILE.touch()
        lock_acquired = True

        conn = connect(read_only=False)
        try:
            yield conn
        finally:
            conn.close()
    finally:
        if lock_acquired and LOCK_FILE.exists():
            LOCK_FILE.unlink()


def ingest_survey_batch(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str,
    validate: bool = True,
) -> Path:
    """Write survey batch to hive-partitioned Parquet: operator={op}/year={year}/data.parquet"""
    if validate:
        sample_size = min(100, len(df))
        for i, row_dict in enumerate(df.head(sample_size).iter_rows(named=True)):
            try:
                SurveyResponse(**row_dict)
            except ValidationError as e:
                raise ValueError(f"Validation failed on row {i}: {e}") from e
        logger.info(f"Validated {sample_size} rows")

    partition_dir = DATA_LAKE_ROOT / "survey_responses" / f"operator={canonical_operator}" / f"year={survey_year}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / "data.parquet"
    df.write_parquet(output_path, compression="zstd", statistics=True)

    logger.info(f"Wrote {len(df)} records to {output_path.relative_to(DATA_LAKE_ROOT)}")
    return output_path


def ingest_survey_metadata(df: pl.DataFrame, validate: bool = True) -> Path:
    """Write or append survey metadata to metadata.parquet, deduplicating by (survey_year, canonical_operator)."""
    if validate:
        for i, row_dict in enumerate(df.iter_rows(named=True)):
            try:
                SurveyMetadata(**row_dict)
            except ValidationError as e:
                raise ValueError(f"Validation failed on metadata row {i}: {e}") from e
        logger.info(f"Validated {len(df)} metadata rows")

    metadata_dir = DATA_LAKE_ROOT / "survey_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    output_path = metadata_dir / "metadata.parquet"

    if output_path.exists():
        combined = pl.concat([pl.read_parquet(output_path), df])
        combined = combined.unique(subset=["survey_year", "canonical_operator"], keep="last")
        combined.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info(f"Appended {len(df)} metadata records")
    else:
        df.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info(f"Wrote {len(df)} metadata records")

    return output_path


def get_table_info(view_name: str) -> pl.DataFrame:
    """Get schema information for view or table."""
    return query(f"DESCRIBE {view_name}")


def get_row_count(view_name: str) -> int:
    """Get row count for view or table."""
    return query(f"SELECT COUNT(*) as count FROM {view_name}")["count"][0]


def inspect_database() -> None:
    """Print summary of data lake views/tables."""
    conn = connect(read_only=True)
    try:
        tables = conn.execute("SHOW TABLES").pl()
        logger.info(f"Available views/tables: {len(tables)}")

        for table_name in tables["name"]:
            count = get_row_count(table_name)
            logger.info(f"{table_name}: {count:,} rows")
    finally:
        conn.close()

