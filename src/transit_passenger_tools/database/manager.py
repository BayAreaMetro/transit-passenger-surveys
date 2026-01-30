"""High-level database operations for survey data ingestion and management."""

# ruff: noqa: S608

import logging
from pathlib import Path

import duckdb
import polars as pl
from pydantic import ValidationError

from transit_passenger_tools.schemas.models import (
    SurveyMetadata,
    SurveyResponse,
    SurveyWeight,
)

from .helpers import (
    DATA_LAKE_ROOT,
    check_git_clean,
    compute_dataframe_hash,
    connect,
    get_git_commit,
    get_latest_metadata,
    get_next_version,
    query,
    write_session,
)
from .validation import (
    _check_duplicate_data,
    _check_schema_compatibility,
    enforce_dataframe_types,
    validate_dataframe_schema,
)

logger = logging.getLogger(__name__)

# Schema version - increment when making breaking changes to data_models.py
# When incrementing, you MUST run migration script before ingesting new data
SCHEMA_VERSION = 1  # v1: Initial schema


def ingest_survey_batch(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str,
    validate: bool = True,
    refresh_views: bool = True,
    require_clean_git: bool = True,
) -> tuple[Path, int, str, str]:
    """Write survey batch to hive-partitioned Parquet.

    Args:
        df: DataFrame to ingest
        survey_year: Survey year
        canonical_operator: Canonical operator name
        validate: Whether to validate data against schema
        refresh_views: Whether to refresh DuckDB views after ingestion
        require_clean_git: Whether to require no uncommitted git changes

    Returns:
        Tuple of (output_path, version, commit_id, data_hash)

    Raises:
        ValueError: If schema version mismatch detected (migration required)
    """
    if require_clean_git:
        check_git_clean(strict=True)

    # Check schema compatibility
    _check_schema_compatibility(canonical_operator, survey_year, df.schema, SCHEMA_VERSION)

    # Check for duplicate data
    data_hash = compute_dataframe_hash(df)
    duplicate_check = _check_duplicate_data(
        canonical_operator, survey_year, data_hash, get_latest_metadata, DATA_LAKE_ROOT
    )
    if duplicate_check:
        return duplicate_check

    # Enforce types and validate
    df = enforce_dataframe_types(df)
    validate_dataframe_schema(df, strict=True)

    if validate:
        sample_size = min(100, len(df))
        for i, row_dict in enumerate(df.head(sample_size).iter_rows(named=True)):
            try:
                SurveyResponse(**row_dict)
            except ValidationError as e:
                msg = f"Validation failed on row {i}: {e}"
                raise ValueError(msg) from e
        logger.info("Validated %d rows", sample_size)

    # Write data
    partition_dir = (
        DATA_LAKE_ROOT
        / "survey_responses"
        / f"operator={canonical_operator}"
        / f"year={survey_year}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    version = get_next_version(partition_dir)
    commit_id = get_git_commit()
    output_path = partition_dir / f"data-{version}-{commit_id}.parquet"
    df.write_parquet(output_path, compression="zstd", statistics=True)

    logger.info(
        "Data changed (hash %s...), wrote %d records to %s (version %d, commit %s)",
        data_hash[:8],
        len(df),
        output_path.relative_to(DATA_LAKE_ROOT),
        version,
        commit_id,
    )

    if refresh_views:
        logger.info("Refreshing views to match current schema...")
        create_views()
        logger.info("Views refreshed successfully")

    return output_path, version, commit_id, data_hash


def ingest_survey_metadata(
    df: pl.DataFrame, validate: bool = True, refresh_views: bool = True
) -> Path:
    """Write or append survey metadata to metadata.parquet.

    Args:
        df: DataFrame to ingest
        validate: Whether to validate data against schema
        refresh_views: Whether to refresh DuckDB views after ingestion

    Deduplicates by (survey_year, canonical_operator, data_version).
    """
    if validate:
        for i, row_dict in enumerate(df.iter_rows(named=True)):
            try:
                SurveyMetadata(**row_dict)
            except ValidationError as e:
                msg = f"Validation failed on metadata row {i}: {e}"
                raise ValueError(msg) from e
        logger.info("Validated %d metadata rows", len(df))

    metadata_dir = DATA_LAKE_ROOT / "survey_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    output_path = metadata_dir / "metadata.parquet"

    if output_path.exists():
        combined = pl.concat([pl.read_parquet(output_path), df])
        combined = combined.unique(
            subset=["survey_year", "canonical_operator", "data_version"], keep="last"
        )
        combined.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info("Appended %d metadata records", len(df))
    else:
        df.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info("Wrote %d metadata records", len(df))

    if refresh_views:
        create_views()

    return output_path


def ingest_survey_weights(
    df: pl.DataFrame, validate: bool = True, refresh_views: bool = True
) -> Path:
    """Write or append survey weights to weights.parquet.

    Args:
        df: DataFrame to ingest
        validate: Whether to validate data against schema
        refresh_views: Whether to refresh DuckDB views after ingestion

    Deduplicates by (unique_id, weight_scheme).
    """
    if validate:
        for i, row_dict in enumerate(df.iter_rows(named=True)):
            try:
                SurveyWeight(**row_dict)
            except ValidationError as e:
                msg = f"Validation failed on weight row {i}: {e}"
                raise ValueError(msg) from e
        logger.info("Validated %d weight rows", len(df))

    weights_dir = DATA_LAKE_ROOT / "survey_weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    output_path = weights_dir / "weights.parquet"

    if output_path.exists():
        combined = pl.concat([pl.read_parquet(output_path), df])
        combined = combined.unique(subset=["response_id", "weight_scheme"], keep="last")
        combined.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info("Appended %d weight records", len(df))
    else:
        df.write_parquet(output_path, compression="zstd", statistics=True)
        logger.info("Wrote %d weight records", len(df))

    if refresh_views:
        create_views()

    return output_path


def get_table_info(view_name: str) -> pl.DataFrame:
    """Get schema information for view or table."""
    # Note: view_name should be validated/trusted input only
    return query(f"DESCRIBE {view_name}")


def get_row_count(view_name: str) -> int:
    """Get row count for view or table."""
    return query(f"SELECT COUNT(*) as count FROM {view_name}")["count"][0]


def inspect_database() -> None:
    """Print summary of data lake views/tables."""
    conn = connect(read_only=True)
    try:
        tables = conn.execute("SHOW TABLES").pl()
        logger.info("Available views/tables: %d", len(tables))

        for table_name in tables["name"]:
            count = get_row_count(table_name)
            logger.info("%s: %s rows", table_name, f"{count:,}")
    finally:
        conn.close()


# ========== Schema Management ==========


def run_migrations() -> None:
    """Run SQL migration files from migrations directory.

    Executes .sql files in alphanumeric order, records each in schema_migrations
    table, and skips already-applied migrations.
    """
    migrations_dir = Path(__file__).parent / "migrations"

    if not migrations_dir.exists():
        logger.warning("Migrations directory not found: %s", migrations_dir)
        return

    with write_session() as conn:
        # Get already-applied migrations
        try:
            applied = conn.execute("SELECT name FROM schema_migrations ORDER BY name").fetchall()
            applied_names = {row[0] for row in applied}
        except duckdb.CatalogException:
            # Table doesn't exist yet, no migrations applied
            applied_names = set()

        # Find and sort migration files
        migration_files = sorted(migrations_dir.glob("*.sql"))

        if not migration_files:
            logger.info("No migration files found")
            return

        # Execute unapplied migrations
        for migration_file in migration_files:
            migration_name = migration_file.name

            if migration_name in applied_names:
                logger.debug("Skipping already-applied migration: %s", migration_name)
                continue

            logger.info("Applying migration: %s", migration_name)

            # Read and execute SQL
            sql = migration_file.read_text(encoding="utf-8")
            conn.execute(sql)

            # Record migration
            conn.execute(
                "INSERT INTO schema_migrations (name, applied_at) VALUES (?, CURRENT_TIMESTAMP)",
                [migration_name],
            )

            logger.info("  Applied: %s", migration_name)


def create_views() -> None:
    """Create or recreate DuckDB views over Parquet files in data lake.

    Views use SELECT * to handle schema evolution but vendor-specific raw
    survey fields are preserved in parquet files for future reference.
    """
    logger.info("Creating DuckDB views over Parquet files...")

    with write_session() as conn:
        # Create view for survey_responses (latest version only)
        survey_responses_pattern = f"{DATA_LAKE_ROOT}/survey_responses/**/data-*.parquet"
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW survey_responses AS
            WITH versioned_data AS (
                SELECT *,
                       CAST(regexp_extract(filename, 'data-(\\d+)-', 1) AS INTEGER) as _version
                FROM read_parquet(
                    '{survey_responses_pattern}',
                    hive_partitioning = true,
                    union_by_name = true,
                    filename = true
                )
            ),
            latest_versions AS (
                SELECT operator, year, MAX(_version) as max_version
                FROM versioned_data
                GROUP BY operator, year
            )
            SELECT versioned_data.* EXCLUDE (filename, _version)
            FROM versioned_data
            JOIN latest_versions
                ON versioned_data.operator = latest_versions.operator
                AND versioned_data.year = latest_versions.year
                AND versioned_data._version = latest_versions.max_version
        """
        )
        logger.info("  Created view: survey_responses")

        # Create view for all versions (power users)
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW survey_responses_all_versions AS
            SELECT *,
                   CAST(regexp_extract(filename, 'data-(\\d+)-', 1) AS INTEGER) as data_version,
                   regexp_extract(filename, 'data-\\d+-(\\w+)\\.parquet', 1) as data_commit
            FROM read_parquet(
                '{survey_responses_pattern}',
                hive_partitioning = true,
                union_by_name = true,
                filename = true
            )
        """
        )
        logger.info("  Created view: survey_responses_all_versions")

        # Create view for survey_metadata (only if file exists)
        metadata_file = DATA_LAKE_ROOT / "survey_metadata" / "metadata.parquet"
        if metadata_file.exists():
            conn.execute(
                f"""
                CREATE OR REPLACE VIEW survey_metadata AS
                SELECT * FROM read_parquet(
                    '{metadata_file}',
                    union_by_name = true
                )
            """
            )
            logger.info("  Created view: survey_metadata")
        else:
            logger.warning("  Skipped survey_metadata view (file does not exist yet)")

        # Create view for survey_weights (only if file exists)
        weights_file = DATA_LAKE_ROOT / "survey_weights" / "weights.parquet"
        if weights_file.exists():
            conn.execute(
                f"""
                CREATE OR REPLACE VIEW survey_weights AS
                SELECT * FROM read_parquet(
                    '{weights_file}',
                    union_by_name = true
                )
            """
            )
            logger.info("  Created view: survey_weights")
        else:
            logger.warning("  Skipped survey_weights view (file does not exist yet)")

        # Verify views work
        count = conn.execute("SELECT COUNT(*) FROM survey_responses").fetchone()[0]  # type: ignore[index]
        logger.info("  Verified: survey_responses has %s rows", f"{count:,}")

        all_versions_count = conn.execute(
            "SELECT COUNT(*) FROM survey_responses_all_versions"
        ).fetchone()[0]  # type: ignore[index]

        logger.info(
            "  Verified: survey_responses_all_versions has %s rows", f"{all_versions_count:,}"
        )

        if metadata_file.exists():
            meta_count = conn.execute("SELECT COUNT(*) FROM survey_metadata").fetchone()[0]  # type: ignore[index]
            logger.info("  Verified: survey_metadata has %s rows", f"{meta_count:,}")

        if weights_file.exists():
            weights_count = conn.execute("SELECT COUNT(*) FROM survey_weights").fetchone()[0]  # type: ignore[index]
            logger.info("  Verified: survey_weights has %s rows", f"{weights_count:,}")
