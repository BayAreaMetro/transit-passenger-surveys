"""Export functions for creating Tableau-friendly cache files.

This module provides functionality to export the DuckDB cache into formats
that are easier for Tableau users to work with, including:
- Tableau .hyper extracts (native format)
- Single joined parquet files

The exports include joined data from survey_responses, survey_weights (pivoted),
and survey_metadata for complete analysis.
"""

import logging
import math
from pathlib import Path

import duckdb
import polars as pl
from tableauhyperapi import (
    NULLABLE,
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)

from .helpers import DUCKDB_PATH, HIVE_ROOT

logger = logging.getLogger(__name__)


def create_joined_dataset_query() -> str:
    """Generate SQL query to create fully joined dataset with pivoted weights.

    Joins survey_responses with survey_metadata and pivots survey_weights so that
    each weight_scheme becomes its own column pair (boarding_weight, trip_weight).

    Returns:
        SQL query string for DuckDB to execute
    """
    return """
        WITH pivoted_weights AS (
            -- Pivot weight schemes into columns
            -- Each weight scheme gets boarding_weight and trip_weight columns
            SELECT
                response_id,
                MAX(CASE WHEN weight_scheme = 'expansion' THEN boarding_weight END) AS weight_expansion_boarding,
                MAX(CASE WHEN weight_scheme = 'expansion' THEN trip_weight END) AS weight_expansion_trip,
                MAX(CASE WHEN weight_scheme = 'onboard' THEN boarding_weight END) AS weight_onboard_boarding,
                MAX(CASE WHEN weight_scheme = 'onboard' THEN trip_weight END) AS weight_onboard_trip,
                MAX(CASE WHEN weight_scheme = 'linked' THEN boarding_weight END) AS weight_linked_boarding,
                MAX(CASE WHEN weight_scheme = 'linked' THEN trip_weight END) AS weight_linked_trip
            FROM survey_weights
            GROUP BY response_id
        )
        SELECT
            sr.*,
            -- Pivoted weights (NULL if no weights for this response)
            pw.weight_expansion_boarding,
            pw.weight_expansion_trip,
            pw.weight_onboard_boarding,
            pw.weight_onboard_trip,
            pw.weight_linked_boarding,
            pw.weight_linked_trip,
            -- Survey metadata fields (NULL if metadata not available)
            sm.survey_name,
            sm.source,
            sm.field_start,
            sm.field_end,
            sm.inflation_year,
            sm.processing_notes
        FROM survey_responses sr
        LEFT JOIN pivoted_weights pw ON sr.response_id = pw.response_id
        LEFT JOIN survey_metadata sm ON sr.survey_id = sm.survey_id
    """  # noqa: E501


def export_to_parquet(
    output_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Export joined dataset to a single parquet file.

    Args:
        output_path: Path where the parquet file should be saved
        conn: Optional DuckDB connection. If None, creates read-only connection.

    Returns:
        Number of rows exported

    Raises:
        RuntimeError: If required tables don't exist in DuckDB
    """
    output_path = Path(output_path)
    close_conn = False

    if conn is None:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        close_conn = True

    try:
        logger.info("Exporting joined dataset to parquet: %s", output_path)

        # Check if required tables exist
        tables_result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {row[0] for row in tables_result}

        if "survey_responses" not in tables:
            msg = "survey_responses table not found. Run cache refresh first."
            raise RuntimeError(msg)

        # Get the joined dataset
        query = create_joined_dataset_query()
        conn.execute(query)

        # Write to parquet
        conn.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")

        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM ({query}) t").fetchone()[0]  # type: ignore  # noqa: PGH003, S608

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info("  Exported %s rows to parquet (%.2f MB)", f"{row_count:,}", file_size_mb)

        return row_count

    finally:
        if close_conn:
            conn.close()


def export_to_hyper(
    output_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Export relational database structure to a Tableau .hyper file.

    Creates a proper relational Hyper file with three separate tables:
    - survey_responses (with all response fields)
    - survey_weights (with weight_scheme, boarding_weight, trip_weight)
    - survey_metadata (with survey-level information)

    Tableau users can create relationships between tables using:
    - survey_responses.response_id = survey_weights.response_id
    - survey_responses.survey_id = survey_metadata.survey_id

    Args:
        output_path: Path where the .hyper file should be saved
        conn: Optional DuckDB connection. If None, creates read-only connection.

    Returns:
        Total number of survey_responses rows exported

    Raises:
        RuntimeError: If required tables don't exist in DuckDB
    """
    output_path = Path(output_path)
    close_conn = False

    if conn is None:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        close_conn = True

    try:
        logger.info("Exporting relational database to Tableau Hyper: %s", output_path)

        # Check if required tables exist
        tables_result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {row[0] for row in tables_result}

        if "survey_responses" not in tables:
            msg = "survey_responses table not found. Run cache refresh first."
            raise RuntimeError(msg)

        # Create Hyper file with multiple tables
        with (
            HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper,
            Connection(
                endpoint=hyper.endpoint,
                database=str(output_path),
                create_mode=CreateMode.CREATE_AND_REPLACE,
            ) as hyper_conn,
        ):
            # Export survey_responses table
            responses_df = conn.execute("SELECT * FROM survey_responses").pl()
            responses_count = len(responses_df)
            logger.info("  Exporting survey_responses: %s rows", f"{responses_count:,}")

            responses_table = _create_hyper_table_definition("survey_responses", responses_df)
            hyper_conn.catalog.create_table(responses_table)
            _insert_polars_to_hyper_chunked(
                hyper_conn, responses_table.table_name, responses_df, chunk_size=50000
            )

            # Export survey_weights table (if exists)
            if "survey_weights" in tables:
                weights_df = conn.execute("SELECT * FROM survey_weights").pl()
                logger.info("  Exporting survey_weights: %s rows", f"{len(weights_df):,}")

                weights_table = _create_hyper_table_definition("survey_weights", weights_df)
                hyper_conn.catalog.create_table(weights_table)
                _insert_polars_to_hyper_chunked(
                    hyper_conn, weights_table.table_name, weights_df, chunk_size=100000
                )

            # Export survey_metadata table (if exists)
            if "survey_metadata" in tables:
                metadata_df = conn.execute("SELECT * FROM survey_metadata").pl()
                logger.info("  Exporting survey_metadata: %s rows", f"{len(metadata_df):,}")

                metadata_table = _create_hyper_table_definition("survey_metadata", metadata_df)
                hyper_conn.catalog.create_table(metadata_table)
                _insert_polars_to_hyper(hyper_conn, metadata_table.table_name, metadata_df)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            "  Exported %s responses to Hyper (%.2f MB)", f"{responses_count:,}", file_size_mb
        )

        return responses_count

    finally:
        if close_conn:
            conn.close()


def _create_hyper_table_definition(table_name: str, df: pl.DataFrame) -> TableDefinition:  # noqa: C901
    """Create Tableau Hyper table definition from Polars DataFrame schema.

    Args:
        table_name: Name for the table in Hyper file
        df: Polars DataFrame with data to export

    Returns:
        TableDefinition for Hyper API
    """
    columns = []
    for col_name, dtype in zip(df.columns, df.dtypes, strict=False):
        # Map Polars dtype to Hyper SqlType
        # Check dtype properties rather than exact type equality
        if dtype in (pl.Utf8, pl.String):
            sql_type = SqlType.text()
        elif dtype == pl.Int64:
            sql_type = SqlType.big_int()
        elif dtype == pl.Int32:
            sql_type = SqlType.int()
        elif dtype in (pl.Int16, pl.Int8):
            sql_type = SqlType.small_int()
        elif dtype in (pl.Float64, pl.Float32):
            sql_type = SqlType.double()
        elif dtype == pl.Boolean:
            sql_type = SqlType.bool()
        elif dtype == pl.Date:
            sql_type = SqlType.date()
        elif isinstance(dtype, pl.Datetime):
            sql_type = SqlType.timestamp()
        elif dtype == pl.Time:
            sql_type = SqlType.time()
        else:
            # Default to TEXT for unknown types
            sql_type = SqlType.text()

        columns.append(TableDefinition.Column(col_name, sql_type, NULLABLE))

    return TableDefinition(
        table_name=TableName("public", table_name),
        columns=columns,
    )


def _insert_polars_to_hyper(
    connection: Connection,
    table_name: TableName,
    df: pl.DataFrame,
) -> None:
    """Insert Polars DataFrame into Hyper table (all rows at once).

    Args:
        connection: Active Hyper connection
        table_name: Target table name
        df: DataFrame to insert
    """
    # Convert DataFrame to list of tuples for insertion
    # Handle None/null values appropriately (but keep date/datetime as objects)
    data = []
    for row in df.iter_rows():
        clean_row = tuple(
            None if (isinstance(val, float) and math.isnan(val)) else val  # NaN check
            for val in row
        )
        data.append(clean_row)

    with Inserter(connection, table_name) as inserter:
        inserter.add_rows(data)
        inserter.execute()


def _insert_polars_to_hyper_chunked(
    connection: Connection,
    table_name: TableName,
    df: pl.DataFrame,
    chunk_size: int = 50000,
) -> None:
    """Insert Polars DataFrame into Hyper table in chunks for large datasets.

    Args:
        connection: Active Hyper connection
        table_name: Target table name
        df: DataFrame to insert
        chunk_size: Number of rows to insert per batch
    """
    row_count = len(df)
    for i in range(0, row_count, chunk_size):
        chunk = df.slice(i, min(chunk_size, row_count - i))
        _insert_polars_to_hyper(connection, table_name, chunk)

        if (i + chunk_size) % 100000 == 0:
            logger.info("    Inserted %s rows...", f"{i + chunk_size:,}")


def export_cache(
    export_formats: list[str] | None = None,
    output_dir: Path | str | None = None,
) -> dict[str, Path]:
    """Export DuckDB cache to Tableau-friendly formats.

    This is the main entry point for cache exports. It creates both .hyper
    and parquet exports by default.

    Args:
        export_formats: List of formats to export. Options: ["hyper", "parquet"].
                       Defaults to both if None.
        output_dir: Directory where exports should be saved. Defaults to HIVE_ROOT
                   (same location as surveys.duckdb).

    Returns:
        Dictionary mapping format name to output file path

    Raises:
        ValueError: If invalid format specified
        RuntimeError: If DuckDB cache doesn't exist or is missing required tables
    """
    if export_formats is None:
        export_formats = ["hyper", "parquet"]

    output_dir = HIVE_ROOT if output_dir is None else Path(output_dir)

    # Validate formats
    valid_formats = {"hyper", "parquet"}
    invalid = set(export_formats) - valid_formats
    if invalid:
        msg = f"Invalid export formats: {invalid}. Valid options: {valid_formats}"
        raise ValueError(msg)

    # Check DuckDB exists
    if not DUCKDB_PATH.exists():
        msg = f"DuckDB cache not found at {DUCKDB_PATH}. Run refresh_cache.py first."
        raise RuntimeError(msg)

    logger.info("Exporting cache to %s formats: %s", len(export_formats), export_formats)

    output_paths = {}

    # Create single read-only connection for all exports
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        if "parquet" in export_formats:
            parquet_path = output_dir / "surveys_export.parquet"
            export_to_parquet(parquet_path, conn)
            output_paths["parquet"] = parquet_path

        if "hyper" in export_formats:
            hyper_path = output_dir / "surveys_export.hyper"
            export_to_hyper(hyper_path, conn)
            output_paths["hyper"] = hyper_path

    logger.info("Cache export completed successfully")
    return output_paths
