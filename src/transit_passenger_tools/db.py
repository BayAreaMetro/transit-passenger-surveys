"""Database utilities for transit passenger survey data lake."""

import hashlib
import io
import logging
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl
from pydantic import ValidationError

try:
    from git import Repo
    from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError
    HAS_GITPYTHON = True
except ImportError:
    HAS_GITPYTHON = False

from transit_passenger_tools.data_models import SurveyMetadata, SurveyResponse

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized\data_lake"
)
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


def get_latest_metadata(
    canonical_operator: str, survey_year: int
) -> dict[str, str | int] | None:
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
                [canonical_operator, survey_year]
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


def ingest_survey_batch(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str,
    validate: bool = True,
) -> tuple[Path, int, str, str]:
    """Write survey batch to hive-partitioned Parquet.

    Returns:
        Tuple of (output_path, version, commit_id, data_hash)
    """
    # Compute hash of incoming data for deduplication
    data_hash = compute_dataframe_hash(df)

    # Check if identical data already exists
    existing_metadata = get_latest_metadata(canonical_operator, survey_year)
    if existing_metadata and existing_metadata["hash"] == data_hash:
        # Data unchanged - reuse existing version
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

    # Data has changed or doesn't exist - proceed with validation and write
    if validate:
        sample_size = min(100, len(df))
        for i, row_dict in enumerate(df.head(sample_size).iter_rows(named=True)):
            try:
                SurveyResponse(**row_dict)
            except ValidationError as e:
                msg = f"Validation failed on row {i}: {e}"
                raise ValueError(msg) from e
        logger.info("Validated %d rows", sample_size)

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
    return output_path, version, commit_id, data_hash


def ingest_survey_metadata(df: pl.DataFrame, validate: bool = True) -> Path:
    """Write or append survey metadata to metadata.parquet.

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

    return output_path


def get_table_info(view_name: str) -> pl.DataFrame:
    """Get schema information for view or table."""
    # Note: view_name should be validated/trusted input only
    return query(f"DESCRIBE {view_name}")


def get_row_count(view_name: str) -> int:
    """Get row count for view or table."""
    return query(f"SELECT COUNT(*) as count FROM {view_name}")["count"][0]  # noqa: S608


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

