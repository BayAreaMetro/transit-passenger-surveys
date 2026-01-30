"""Database utilities for transit passenger survey data lake."""

# ruff: noqa: S608

import hashlib
import io
import logging
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import get_args, get_origin

import duckdb
import polars as pl
import yaml
from pydantic import ValidationError

try:
    from git import Repo
    from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError

    HAS_GITPYTHON = True
except ImportError:
    HAS_GITPYTHON = False

from transit_passenger_tools.schemas.models import SurveyMetadata, SurveyResponse, SurveyWeight

# Schema version - increment when making breaking changes to data_models.py
# When incrementing, you MUST run migration script before ingesting new data
SCHEMA_VERSION = 1  # v1: Initial schema

logger = logging.getLogger(__name__)

# Load data lake root from config (avoid triggering shapefile validation)
_config_path = Path(os.getenv("PIPELINE_CONFIG", "config/pipeline.yaml"))
with _config_path.open() as f:
    DATA_LAKE_ROOT = Path(yaml.safe_load(f)["data_lake_root"])

DUCKDB_PATH = DATA_LAKE_ROOT / "surveys.duckdb"
LOCK_FILE = DATA_LAKE_ROOT / ".surveys.lock"


def get_git_commit() -> str:
    """Get current git commit SHA (short form)."""
    if not HAS_GITPYTHON:
        logger.warning("GitPython not installed, using 'unknown'")
        return "unknown"

    try:
        repo_root = Path(__file__).parent.parent.parent
        repo = Repo(repo_root, search_parent_directories=True)
        return repo.head.commit.hexsha[:7]
    except InvalidGitRepositoryError:
        logger.warning("Not in a git repository, using 'unknown'")
        return "unknown"
    except (NoSuchPathError, GitCommandError) as e:
        logger.warning("Could not determine git commit: %s, using 'unknown'", e)
        return "unknown"


def check_git_clean(strict: bool = True) -> bool:
    """Check if git repository has uncommitted changes.

    Args:
        strict: If True, raise error on uncommitted changes. If False, just warn.

    Returns:
        True if repository is clean, False otherwise

    Raises:
        ValueError: If strict=True and repository has uncommitted changes
    """
    if not HAS_GITPYTHON:
        logger.warning("GitPython not installed, cannot verify clean repository")
        return True

    try:
        repo_root = Path(__file__).parent.parent.parent
        repo = Repo(repo_root, search_parent_directories=True)

        if repo.is_dirty(untracked_files=True):
            uncommitted = [item.a_path for item in repo.index.diff(None) if item.a_path]
            untracked = repo.untracked_files

            msg = "Repository has uncommitted changes. Commit changes before ingesting data.\n"
            if uncommitted:
                msg += f"  Modified: {', '.join(uncommitted[:5])}\n"
            if untracked:
                msg += f"  Untracked: {', '.join(untracked[:5])}\n"

            if strict:
                raise ValueError(msg)
            logger.warning(msg)
            return False

    except InvalidGitRepositoryError:
        logger.warning("Not in a git repository, cannot verify clean state")
        return True
    except (NoSuchPathError, GitCommandError) as e:
        logger.warning("Could not check git status: %s", e)
        return True
    else:
        return True


def get_next_version(partition_dir: Path) -> int:
    """Get next version number by scanning existing files in partition."""
    if not partition_dir.exists():
        return 1

    existing_files = list(partition_dir.glob("data-*.parquet"))
    if not existing_files:
        return 1

    # Extract version numbers from filenames like data-1-abc123.parquet
    versions = []
    for file in existing_files:
        match = re.match(r"data-(\d+)-\w+\.parquet", file.name)
        if match:
            versions.append(int(match.group(1)))

    return max(versions, default=0) + 1


def compute_dataframe_hash(df: pl.DataFrame) -> str:
    """Compute deterministic SHA256 hash of DataFrame content.

    Sorts columns alphabetically and rows by all columns to ensure
    identical data produces identical hash regardless of order.
    """
    # Sort columns alphabetically for deterministic ordering
    sorted_df = df.select(sorted(df.columns))

    # Sort rows by all columns for deterministic ordering
    sorted_df = sorted_df.sort(sorted_df.columns)

    # Serialize to parquet bytes and hash
    buffer = io.BytesIO()
    sorted_df.write_parquet(buffer, compression="zstd")
    parquet_bytes = buffer.getvalue()
    hash_obj = hashlib.sha256(parquet_bytes)

    return hash_obj.hexdigest()


def get_latest_metadata(canonical_operator: str, survey_year: int) -> dict[str, str | int] | None:
    """Get latest version metadata for an operator/year combination.

    Returns:
        Dict with keys 'version', 'commit', 'hash', or None if not found.
    """
    try:
        conn = connect(read_only=True)
        try:
            result = conn.execute(
                """
                SELECT data_version, data_commit, data_hash
                FROM survey_metadata
                WHERE canonical_operator = ?
                AND survey_year = ?
                ORDER BY data_version DESC
                LIMIT 1
                """,
                [canonical_operator, survey_year],
            ).pl()
        finally:
            conn.close()

        if len(result) == 0:
            return None

        row = result.row(0, named=True)
        return {
            "version": row["data_version"],
            "commit": row["data_commit"],
            "hash": row["data_hash"],
        }
    except (duckdb.CatalogException, duckdb.Error):
        # Table might not exist yet or no data
        return None


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


def enforce_dataframe_types(df: pl.DataFrame) -> pl.DataFrame:
    """Cast DataFrame columns to match Pydantic schema types.

    This ensures that Parquet files have correct data types even if CSV
    inference gets them wrong (e.g., lat/lon as VARCHAR instead of Float64).
    """
    # Map Python types to Polars types
    python_to_polars = {
        int: pl.Int64,
        float: pl.Float64,
        bool: pl.Boolean,
        str: pl.Utf8,
    }

    type_map = {}

    for field_name, field_info in SurveyResponse.model_fields.items():
        if field_name not in df.columns:
            continue

        # Get the annotation (type hint)
        annotation = field_info.annotation

        # Handle Optional/Union types using typing.get_origin and get_args
        origin = get_origin(annotation)
        if origin is not None:
            # This is a generic type (Union, Optional, etc.)
            args = get_args(annotation)
            # Filter out None to get the actual type
            non_none_types = [t for t in args if t is not type(None)]
            if non_none_types:
                annotation = non_none_types[0]

        # Map Python type to Polars type
        if annotation in python_to_polars:
            type_map[field_name] = python_to_polars[annotation]
        else:
            # Skip types we don't handle (date, datetime, enums, etc.)
            # These are handled automatically by Polars or are already correct
            pass

    # Cast columns that need type correction
    cast_exprs = []
    for col in df.columns:
        if col in type_map and df[col].dtype != type_map[col]:
            logger.info("Casting %s from %s to %s", col, df[col].dtype, type_map[col])
            cast_exprs.append(pl.col(col).cast(type_map[col], strict=False).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    return df.select(cast_exprs) if cast_exprs else df


def validate_dataframe_schema(df: pl.DataFrame, strict: bool = True) -> None:
    """Validate DataFrame schema matches expected Pydantic schema types.

    Args:
        df: DataFrame to validate
        strict: If True, raise error on type mismatches. If False, just warn.

    Raises:
        ValueError: If strict=True and schema validation fails
    """
    python_to_polars = {
        int: pl.Int64,
        float: pl.Float64,
        bool: pl.Boolean,
        str: pl.Utf8,
    }

    mismatches = []

    for field_name, field_info in SurveyResponse.model_fields.items():
        if field_name not in df.columns:
            continue

        annotation = field_info.annotation

        # Handle Optional/Union types
        origin = get_origin(annotation)
        if origin is not None:
            args = get_args(annotation)
            non_none_types = [t for t in args if t is not type(None)]
            if non_none_types:
                annotation = non_none_types[0]

        # Check if we can map this type
        if annotation not in python_to_polars:
            # Skip types we don't validate (date, datetime, enums)
            continue

        # Check type matches
        expected_type = python_to_polars[annotation]
        actual_type = df[field_name].dtype

        if actual_type != expected_type:
            mismatches.append(
                f"  {field_name}: expected {expected_type}, got {actual_type}"
            )

    if mismatches:
        msg = "Schema validation failed. Type mismatches:\n" + "\n".join(mismatches)
        if strict:
            raise ValueError(msg)
        logger.warning(msg)


@contextmanager
def write_session() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager for safe write operations with file-based locking."""
    lock_acquired = False
    try:
        if LOCK_FILE.exists():
            msg = "Write lock already held by another process"
            raise RuntimeError(msg)

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


def _check_schema_compatibility(
    canonical_operator: str, survey_year: int, new_schema: dict
) -> None:
    """Check if new data schema is compatible with existing data."""
    partition_dir = (
        DATA_LAKE_ROOT
        / "survey_responses"
        / f"operator={canonical_operator}"
        / f"year={survey_year}"
    )

    if not partition_dir.exists():
        return

    existing_files = list(partition_dir.glob("*.parquet"))
    if not existing_files:
        return

    existing_schema = pl.read_parquet_schema(existing_files[0])
    schema_issues = []

    # Check for column renames
    if "hispanic" in existing_schema and "is_hispanic" in new_schema:
        schema_issues.append(
            "Column rename detected: 'hispanic' → 'is_hispanic'. "
            "Run migration script: scripts/migrate_schema_v1_to_v2.py"
        )

    # Check for type changes
    common_cols = set(existing_schema.keys()) & set(new_schema.keys())
    schema_issues.extend(
        f"Type mismatch for '{col}': existing={existing_schema[col]}, new={new_schema[col]}"
        for col in common_cols
        if existing_schema[col] != new_schema[col]
    )

    if schema_issues:
        error_msg = (
            f"\n\nSCHEMA VERSION MISMATCH for {canonical_operator}/{survey_year}:\n"
            + "\n".join(f"  - {issue}" for issue in schema_issues)
            + f"\n\nCurrent schema version: {SCHEMA_VERSION}\n"
            + "\nMigration required. See docs/schema_migration.md\n"
        )
        raise ValueError(error_msg)


def _check_duplicate_data(
    canonical_operator: str, survey_year: int, data_hash: str
) -> tuple[Path, int, str, str] | None:
    """Check if identical data already exists. Returns existing version info if found."""
    existing_metadata = get_latest_metadata(canonical_operator, survey_year)
    if not existing_metadata or existing_metadata["hash"] != data_hash:
        return None

    existing_version = int(existing_metadata["version"])
    existing_commit = str(existing_metadata["commit"])
    partition_dir = (
        DATA_LAKE_ROOT
        / "survey_responses"
        / f"operator={canonical_operator}"
        / f"year={survey_year}"
    )
    existing_path = partition_dir / f"data-{existing_version}-{existing_commit}.parquet"

    logger.info(
        "Data unchanged (hash %s...), reusing version %d for %s/%d",
        data_hash[:8],
        existing_version,
        canonical_operator,
        survey_year,
    )
    return existing_path, existing_version, existing_commit, data_hash


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
    _check_schema_compatibility(canonical_operator, survey_year, df.schema)

    # Check for duplicate data
    data_hash = compute_dataframe_hash(df)
    duplicate_check = _check_duplicate_data(canonical_operator, survey_year, data_hash)
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
    df: pl.DataFrame,
    validate: bool = True,
    refresh_views: bool = True
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
    df: pl.DataFrame,
    validate: bool = True,
    refresh_views: bool = True
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


def validate_referential_integrity() -> dict[str, int]:
    """Validate foreign key relationships in the data lake.

    Checks:
    1. All SurveyWeight.response_id values reference existing SurveyResponse.response_id
    2. All SurveyResponse.survey_id values reference existing SurveyMetadata.survey_id

    Returns:
        Dict with validation results: {
            'orphaned_weights': count,
            'orphaned_responses': count
        }

    Raises:
        ValueError: If referential integrity violations are found
    """
    conn = connect(read_only=True)
    try:
        results = {}
        errors = []

        # Check 1: Weights must reference existing responses
        orphaned_weights = conn.execute(
            """
            SELECT COUNT(*)
            FROM survey_weights w
            LEFT JOIN survey_responses r ON w.response_id = r.response_id
            WHERE r.response_id IS NULL
        """
        ).fetchone()[0]  # type: ignore[index]

        results["orphaned_weights"] = orphaned_weights

        if orphaned_weights > 0:
            sample = conn.execute(
                """
                SELECT w.response_id
                FROM survey_weights w
                LEFT JOIN survey_responses r ON w.response_id = r.response_id
                WHERE r.response_id IS NULL
                LIMIT 5
            """
            ).fetchall()
            sample_ids = [row[0] for row in sample]
            errors.append(
                f"{orphaned_weights} weight records reference non-existent responses. "
                f"Sample IDs: {', '.join(sample_ids)}"
            )

        # Check 2: Responses must have metadata (via survey_id foreign key)
        orphaned_responses = conn.execute(
            """
            SELECT COUNT(*)
            FROM survey_responses r
            LEFT JOIN survey_metadata m ON r.survey_id = m.survey_id
            WHERE m.survey_id IS NULL
        """
        ).fetchone()[0]  # type: ignore[index]

        results["orphaned_responses"] = orphaned_responses

        if orphaned_responses > 0:
            sample = conn.execute(
                """
                SELECT DISTINCT r.survey_id
                FROM survey_responses r
                LEFT JOIN survey_metadata m ON r.survey_id = m.survey_id
                WHERE m.survey_id IS NULL
                LIMIT 5
            """
            ).fetchall()
            sample_ids = [row[0] for row in sample]
            errors.append(
                f"{orphaned_responses} response records lack metadata. "
                f"Missing survey_id: {', '.join(sample_ids)}"
            )

        if errors:
            msg = "Referential integrity violations found:\n  - " + "\n  - ".join(errors)
            raise ValueError(msg)

        logger.info("✓ Referential integrity validation passed")
        return results
    finally:
        conn.close()
