"""Database helper utilities for git, hashing, versioning, and connections."""

# ruff: noqa: S608

import hashlib
import io
import logging
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import yaml

try:
    from git import Repo
    from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError

    HAS_GITPYTHON = True
except ImportError:
    HAS_GITPYTHON = False

logger = logging.getLogger(__name__)

# Load Hive warehouse root from config
_config_path = Path(os.getenv("PIPELINE_CONFIG", "config/pipeline.yaml"))
with _config_path.open() as f:
    HIVE_ROOT = Path(yaml.safe_load(f)["hive_root"])

DUCKDB_PATH = HIVE_ROOT / "surveys.duckdb"
LOCK_FILE = HIVE_ROOT / ".surveys.lock"


def get_git_commit() -> str:
    """Get current git commit SHA (short form)."""
    if not HAS_GITPYTHON:
        logger.warning("GitPython not installed, using 'unknown'")
        return "unknown"

    try:
        repo_root = Path(__file__).parent.parent.parent.parent
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
        repo_root = Path(__file__).parent.parent.parent.parent
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


def check_cache_freshness() -> dict[str, bool | str]:
    """Check if DuckDB cache is stale compared to Parquet files.

    Compares last_synced_at timestamp from cache_metadata table against
    newest file modification time in the Hive warehouse.

    Returns:
        Dict with keys 'is_fresh' (bool) and 'message' (str)
    """
    try:
        conn = connect(read_only=True)
        try:
            # Get last sync timestamp
            result = conn.execute(
                "SELECT MIN(last_synced_at) as oldest_sync FROM cache_metadata"
            ).fetchone()

            if not result or result[0] is None:
                return {
                    "is_fresh": False,
                    "message": "Cache has never been synced. Run sync_to_duckdb_cache().",
                }

            last_sync = result[0]

            # Find newest Parquet file in Hive warehouse
            newest_mtime = 0.0
            newest_file = None

            for parquet_file in HIVE_ROOT.rglob("*.parquet"):
                mtime = parquet_file.stat().st_mtime
                if mtime > newest_mtime:
                    newest_mtime = mtime
                    newest_file = parquet_file

            if newest_file is None:
                return {
                    "is_fresh": True,
                    "message": "No Parquet files found in Hive warehouse.",
                }

            # Compare timestamps (convert mtime to datetime for comparison)
            newest_file_time = datetime.fromtimestamp(newest_mtime, tz=UTC)

            # Parse last_sync if it's a string
            if isinstance(last_sync, str):
                last_sync = datetime.fromisoformat(last_sync)

            if newest_file_time > last_sync:
                relative_path = newest_file.relative_to(HIVE_ROOT)
                return {
                    "is_fresh": False,
                    "message": f"Cache is stale. Newest file: {relative_path} "
                    f"modified at {newest_file_time.isoformat()}. "
                    f"Last sync: {last_sync}. Run sync_to_duckdb_cache().",
                }

            return {
                "is_fresh": True,
                "message": f"Cache is fresh (last synced: {last_sync}).",
            }

        finally:
            conn.close()

    except duckdb.CatalogException:
        return {
            "is_fresh": False,
            "message": "Cache metadata table doesn't exist. Run sync_to_duckdb_cache().",
        }
    except (OSError, ValueError) as e:
        logger.warning("Could not check cache freshness: %s", e)
        return {
            "is_fresh": False,
            "message": f"Error checking cache: {e}",
        }


def query_parquet(sql: str) -> pl.DataFrame:
    """Execute SQL query directly against Parquet files (bypassing DuckDB cache).

    This is useful for development or when you need to query fresh data
    without syncing the cache. Creates temporary views over Parquet files.

    Args:
        sql: SQL query to execute (can reference survey_responses, etc.)

    Returns:
        Polars DataFrame with query results
    """
    conn = duckdb.connect(":memory:")
    try:
        # Create temporary views over Parquet files
        survey_responses_pattern = f"{HIVE_ROOT}/survey_responses/**/data-*.parquet"
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

        # Create metadata view if file exists
        metadata_file = HIVE_ROOT / "survey_metadata" / "metadata.parquet"
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

        # Create weights view if file exists
        weights_file = HIVE_ROOT / "survey_weights" / "weights.parquet"
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

        # Execute user query
        return conn.execute(sql).pl()

    finally:
        conn.close()

