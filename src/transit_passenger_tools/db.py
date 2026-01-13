"""
Database utilities for transit passenger survey data.

This module provides connection management, Polars integration, and export
functions for the SQLite survey database.
"""

import logging
import sqlite3
import sys
from configparser import ConfigParser
from pathlib import Path
from typing import Optional

import polars as pl
from pydantic import ValidationError

from transit_passenger_tools.data_models import SurveyResponse, SurveyWeight

logger = logging.getLogger(__name__)

# Configure logger if no handlers exist
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _get_db_path_from_alembic() -> Path:
    """Read database path from alembic.ini configuration."""
    config = ConfigParser()
    alembic_ini = Path(__file__).parent.parent.parent / "alembic.ini"
    config.read(alembic_ini)
    url = config.get("alembic", "sqlalchemy.url")
    # Extract path from sqlite:/// URL
    if url.startswith("sqlite:///"):
        path_str = url[len("sqlite:///"):]
        # Handle escaped backslashes from INI file (UNC paths)
        # ConfigParser reads \\\\server as \\server, so just use as-is
        return Path(path_str)
    msg = f"Unexpected database URL format: {url}"
    raise ValueError(msg)


# Default database path (read from alembic.ini)
DEFAULT_DB_PATH = _get_db_path_from_alembic()


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """
    Get a SQLite database connection.

    Args:
        db_path: Path to SQLite database file. Defaults to transit_surveys.db
                 in the current directory.

    Returns:
        SQLite connection object.
    """
    path = db_path or DEFAULT_DB_PATH
    return sqlite3.connect(str(path))


def read_query(query: str, db_path: Optional[Path] = None) -> pl.DataFrame:
    """
    Execute a SQL query and return results as a Polars DataFrame.

    Args:
        query: SQL query string to execute.
        db_path: Path to SQLite database file.

    Returns:
        Polars DataFrame with query results.

    Example:
        >>> df = read_query("SELECT * FROM survey_responses WHERE survey_year = 2024")
        >>> print(df.shape)
    """
    conn = get_connection(db_path)
    try:
        return pl.read_database(query, connection=conn)
    finally:
        conn.close()


def read_table(
    table_name: str,
    where: Optional[str] = None,
    limit: Optional[int] = None,
    db_path: Optional[Path] = None,
) -> pl.DataFrame:
    """
    Read an entire table or filtered subset as a Polars DataFrame.

    Args:
        table_name: Name of the table to read.
        where: Optional WHERE clause (without the WHERE keyword).
        limit: Optional row limit.
        db_path: Path to SQLite database file.

    Returns:
        Polars DataFrame with table data.

    Example:
        >>> df = read_table("survey_responses", where="operator = 'BART'", limit=1000)
    """
    query = f"SELECT * FROM {table_name}"  # noqa: S608
    if where:
        query += f" WHERE {where}"
    if limit:
        query += f" LIMIT {limit}"

    return read_query(query, db_path)


def insert_dataframe(
    df: pl.DataFrame,
    table_name: str,
    if_exists: str = "append",
    validate: bool = True,
    model: Optional[type[SurveyResponse | SurveyWeight]] = None,
    db_path: Optional[Path] = None,
) -> int:
    """
    Insert a Polars DataFrame into a database table.

    Args:
        df: Polars DataFrame to insert.
        table_name: Name of the target table.
        if_exists: How to behave if table exists ('fail', 'replace', 'append').
        validate: Whether to validate rows against Pydantic model before insert.
        model: Pydantic model class for validation (required if validate=True).
        db_path: Path to SQLite database file.

    Returns:
        Number of rows inserted.

    Raises:
        ValidationError: If validate=True and data doesn't match schema.

    Example:
        >>> from transit_passenger_tools.data_models import SurveyResponse
        >>> df = pl.DataFrame({...})
        >>> insert_dataframe(df, "survey_responses", validate=True, model=SurveyResponse)
    """
    if validate and model is None:
        msg = "model parameter required when validate=True"
        raise ValueError(msg)

    if validate:
        # Validate a sample of rows (for performance, validate first 100 rows)
        sample_size = min(100, len(df))
        for row_dict in df.head(sample_size).iter_rows(named=True):
            try:
                model(**row_dict)
            except ValidationError as e:
                msg = f"Validation failed on row: {e}"
                raise ValidationError(msg) from e

    conn = get_connection(db_path)
    try:
        df.write_database(table_name, connection=conn, if_table_exists=if_exists)
        return len(df)
    finally:
        conn.close()


def get_table_info(table_name: str, db_path: Optional[Path] = None) -> pl.DataFrame:
    """
    Get schema information for a table.

    Args:
        table_name: Name of the table.
        db_path: Path to SQLite database file.

    Returns:
        DataFrame with column information (name, type, nullable, etc.).
    """
    query = f"PRAGMA table_info({table_name})"  # noqa: S608
    return read_query(query, db_path)


def get_row_count(table_name: str, db_path: Optional[Path] = None) -> int:
    """
    Get the number of rows in a table.

    Args:
        table_name: Name of the table.
        db_path: Path to SQLite database file.

    Returns:
        Number of rows in the table.
    """
    query = f"SELECT COUNT(*) as count FROM {table_name}"  # noqa: S608
    result = read_query(query, db_path)
    return result["count"][0]


def inspect_database(db_path: Optional[Path] = None) -> None:
    """
    Log a summary of the database structure and contents.

    Args:
        db_path: Path to SQLite database file.

    Example:
        >>> from transit_passenger_tools import db
        >>> db.inspect_database()
    """
    # List all tables
    tables = read_query('SELECT name FROM sqlite_master WHERE type="table"', db_path)
    logger.info("=" * 60)
    logger.info("Database Tables")
    logger.info("=" * 60)
    logger.info("\n%s", tables)
    logger.info("")

    # For each non-alembic table, show schema and row count
    for table_name in tables["name"]:
        if table_name == "alembic_version":
            continue

        logger.info("\nTable: %s", table_name)
        logger.info("-" * 60)

        schema = get_table_info(table_name, db_path)
        row_count = get_row_count(table_name, db_path)

        logger.info("Columns: %d", len(schema))
        logger.info("Rows: %d", row_count)

        # Show first few columns as sample
        if len(schema) > 0:
            logger.info("\nFirst 5 columns:")
            logger.info("\n%s", schema.select(["name", "type", "notnull"]).head(5))
        logger.info("")

