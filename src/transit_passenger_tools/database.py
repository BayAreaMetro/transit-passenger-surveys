"""Database utilities for transit passenger survey data.

Flat-Parquet storage layer: ingestion, archiving, schema validation,
referential integrity checks, and Tableau Hyper export.

Quick reference::

    from transit_passenger_tools.database import DATA_ROOT
    from transit_passenger_tools.database import ingest_survey_responses
    from transit_passenger_tools.database import export_to_hyper
"""

import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import get_args, get_origin

import polars as pl
from pydantic import ValidationError
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

from transit_passenger_tools.codebook import POLARS_TO_HYPER, PYTHON_TO_POLARS
from transit_passenger_tools.config import get_config
from transit_passenger_tools.models import (
    SurveyMetadata,
    SurveyResponse,
    SurveyWeight,
)

logger = logging.getLogger(__name__)

# ========== Config ==========

DATA_ROOT = Path(get_config().data_root)


# ========== Query helpers ==========


def get_ingested_operator_years() -> set[tuple[str, int]]:
    """Return ``{(canonical_operator, survey_year)}`` pairs already in the warehouse."""
    metadata_path = DATA_ROOT / "survey_metadata.parquet"
    if not metadata_path.exists():
        return set()
    md = pl.read_parquet(metadata_path, columns=["canonical_operator", "survey_year"])
    return set(md.iter_rows())


# ========== Archiving ==========


def archive_file(path: Path) -> Path:
    """Archive a file by moving it to the archive/ subfolder with a date stamp.

    Renames e.g. ``survey_responses.parquet`` to
    ``archive/survey_responses_2026-02-27.parquet``. If a same-day archive
    already exists, appends a counter (``_2``, ``_3``, etc.).

    Returns:
        The new path of the archived file.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    if not path.exists():
        msg = f"Cannot archive: file does not exist: {path}"
        raise FileNotFoundError(msg)

    archive_dir = path.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    stem = path.stem
    suffix = path.suffix
    today = datetime.now(UTC).date().isoformat()

    candidate = archive_dir / f"{stem}_{today}{suffix}"
    counter = 2
    while candidate.exists():
        candidate = archive_dir / f"{stem}_{today}_{counter}{suffix}"
        counter += 1

    path.rename(candidate)
    logger.info("Archived %s -> %s", path.name, candidate.name)
    return candidate


# ========== Schema validation ==========


def _resolve_annotation(annotation: type) -> type:
    """Unwrap ``Optional[T]`` / ``Union[T, None]`` and resolve Enum subclasses.

    Returns the innermost concrete type suitable for ``PYTHON_TO_POLARS``
    lookup — e.g. ``Direction | None`` → ``str``.
    """
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        non_none = [t for t in args if t is not type(None)]
        if non_none:
            annotation = non_none[0]
    # Resolve str/int Enum subclasses to their primitive base type
    # (skip types already in the map — e.g. bool, which is a subclass of int)
    if isinstance(annotation, type) and annotation not in PYTHON_TO_POLARS:
        for base in PYTHON_TO_POLARS:
            if isinstance(base, type) and issubclass(annotation, base):
                return base
    return annotation


def enforce_dataframe_types(
    df: pl.DataFrame, *, strict: bool = True,
) -> pl.DataFrame:
    """Cast DataFrame columns to match Pydantic schema types and validate.

    After casting, verifies that every column matches the expected type.
    With *strict=True* (default) a ``ValueError`` is raised on mismatch;
    otherwise a warning is logged.
    """
    type_map: dict[str, pl.DataType] = {}
    for name, info in SurveyResponse.model_fields.items():
        if name not in df.columns:
            continue
        ann = _resolve_annotation(info.annotation) # pyright: ignore[reportArgumentType]
        if ann in PYTHON_TO_POLARS:
            type_map[name] = PYTHON_TO_POLARS[ann]

    cast_exprs = []
    for col in df.columns:
        if col in type_map and df[col].dtype != type_map[col]:
            logger.info("Casting %s from %s to %s", col, df[col].dtype, type_map[col])
            cast_exprs.append(pl.col(col).cast(type_map[col], strict=False).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    df = df.select(cast_exprs) if cast_exprs else df

    # Post-cast validation
    mismatches = [
        f"  {col}: expected {type_map[col]}, got {df[col].dtype}"
        for col in type_map
        if col in df.columns and df[col].dtype != type_map[col]
    ]
    if mismatches:
        msg = "Schema validation failed. Type mismatches:\n" + "\n".join(mismatches)
        if strict:
            raise ValueError(msg)
        logger.warning(msg)

    return df


def validate_referential_integrity(
    responses: pl.DataFrame,
    weights: pl.DataFrame | None = None,
    metadata: pl.DataFrame | None = None,
) -> dict[str, int]:
    """Validate foreign key relationships between DataFrames.

    Checks:
    1. All weight ``response_id`` values exist in responses
    2. All response ``survey_id`` values exist in metadata

    Parameters:
        responses: Survey responses DataFrame (must have ``response_id``).
        weights: Survey weights DataFrame.  Skipped when ``None``.
        metadata: Survey metadata DataFrame.  Skipped when ``None``.

    Returns:
        Dict with counts: ``{'orphaned_weights': int, 'orphaned_responses': int}``

    Raises:
        ValueError: If referential integrity violations are found.
    """
    results: dict[str, int] = {}
    errors: list[str] = []

    # Check 1: Weights must reference existing responses
    if weights is not None:
        orphaned = weights.join(responses, on="response_id", how="anti")
        results["orphaned_weights"] = len(orphaned)

        if len(orphaned) > 0:
            sample_ids = orphaned["response_id"].head(5).to_list()
            errors.append(
                f"{len(orphaned)} weight records reference non-existent responses. "
                f"Sample IDs: {', '.join(str(x) for x in sample_ids)}"
            )
    else:
        results["orphaned_weights"] = 0

    # Check 2: Responses must have metadata (via survey_id foreign key)
    if metadata is not None:
        if "survey_id" in responses.columns and "survey_id" in metadata.columns:
            orphaned = responses.join(metadata, on="survey_id", how="anti")
            results["orphaned_responses"] = len(orphaned)

            if len(orphaned) > 0:
                sample_ids = orphaned["survey_id"].unique().head(5).to_list()
                errors.append(
                    f"{len(orphaned)} response records lack metadata. "
                    f"Missing survey_id: {', '.join(str(x) for x in sample_ids)}"
                )
        else:
            results["orphaned_responses"] = 0
    else:
        results["orphaned_responses"] = 0

    if errors:
        msg = "Referential integrity violations found:\n  - " + "\n  - ".join(errors)
        raise ValueError(msg)

    logger.info("Referential integrity validation passed")
    return results


def validate_warehouse_integrity() -> dict[str, int]:
    """Read the warehouse Parquet files and validate referential integrity.

    Convenience wrapper around :func:`validate_referential_integrity` that
    loads ``survey_responses``, ``survey_weights``, and ``survey_metadata``
    from ``DATA_ROOT``.

    Returns:
        Dict with counts (see :func:`validate_referential_integrity`).

    Raises:
        FileNotFoundError: If ``survey_responses.parquet`` does not exist.
        ValueError: If referential integrity violations are found.
    """
    responses_path = DATA_ROOT / "survey_responses.parquet"
    if not responses_path.exists():
        msg = "survey_responses.parquet not found"
        raise FileNotFoundError(msg)

    responses = pl.read_parquet(responses_path)

    weights_path = DATA_ROOT / "survey_weights.parquet"
    weights = pl.read_parquet(weights_path) if weights_path.exists() else None

    metadata_path = DATA_ROOT / "survey_metadata.parquet"
    metadata = pl.read_parquet(metadata_path) if metadata_path.exists() else None

    return validate_referential_integrity(responses, weights, metadata)


# ========== Ingestion ==========


def _validate_rows(
    df: pl.DataFrame,
    model: type,
    *,
    sample_size: int | None = None,
) -> None:
    """Validate DataFrame rows against a Pydantic model."""
    subset = df.head(min(sample_size, len(df))) if sample_size else df
    for i, row_dict in enumerate(subset.iter_rows(named=True)):
        try:
            model(**row_dict)
        except ValidationError as e:
            msg = f"Validation failed on row {i}: {e}"
            raise ValueError(msg) from e
    logger.info("Validated %d rows against %s", len(subset), model.__name__)


def _upsert_parquet(
    path: Path,
    df: pl.DataFrame,
    dedup_keys: list[str],
    *,
    label: str = "records",
    archive: bool = True,
) -> Path:
    """Archive existing file (if any), concat new rows, dedup, and write.

    When *archive* is ``False`` the existing file is read, concatenated
    with *df*, and overwritten in-place — no archive snapshot is created.
    Use ``archive=False`` during bulk rebuilds to avoid creating an archive
    for every intermediate state.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        if archive:
            archive_file(path)
            archive_dir = path.parent / "archive"
            latest = max(
                archive_dir.glob(f"{path.stem}_*.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            existing = pl.read_parquet(latest)
        else:
            existing = pl.read_parquet(path)

        combined = pl.concat([existing, df], how="diagonal_relaxed")
        combined = combined.unique(subset=dedup_keys, keep="last")
        combined.write_parquet(path, compression="zstd", statistics=True)
        logger.info("Appended %d new %s (%d total)", len(df), label, len(combined))
    else:
        df.write_parquet(path, compression="zstd", statistics=True)
        logger.info("Wrote %d %s to %s", len(df), label, path.name)

    return path


def ingest_survey_responses(
    df: pl.DataFrame,
    survey_year: int,
    canonical_operator: str,
    validate: bool = True,
    *,
    archive: bool = True,
) -> Path:
    """Write survey responses to flat Parquet storage.

    Archives existing data, concatenates new rows, and deduplicates
    by ``response_id`` (keeping the latest).  Pass ``archive=False``
    during bulk rebuilds to avoid creating per-batch archive snapshots.

    Raises:
        ValueError: If schema version mismatch detected (migration required)
    """
    # Schema-compatibility check against existing data
    responses_path = DATA_ROOT / "survey_responses.parquet"
    if responses_path.exists():
        existing_schema = pl.read_parquet_schema(responses_path)
        schema_issues: list[str] = []
        if "hispanic" in existing_schema and "is_hispanic" in df.schema:
            schema_issues.append(
                "Column rename detected: 'hispanic' -> 'is_hispanic'. "
                "Run migration script: scripts/migrate_schema_v1_to_v2.py"
            )
        schema_issues.extend(
            f"Type mismatch for '{col}': "
            f"existing={existing_schema[col]}, new={df.schema[col]}"
            for col in set(existing_schema) & set(df.schema)
            if existing_schema[col] != df.schema[col]
            and df.schema[col] != pl.Null  # all-null columns are compatible
        )
        if schema_issues:
            raise ValueError(
                f"\n\nSCHEMA VERSION MISMATCH for {canonical_operator}/{survey_year}:\n"
                + "\n".join(f"  - {issue}" for issue in schema_issues)
                + "\n\nMigration required. See docs/schema_migration.md\n"
            )

    # Normalize types after the schema check so all-null columns (Polars
    # ``Null`` dtype) and enum-backed fields are cast to their declared types
    # before writing to parquet.
    df = enforce_dataframe_types(df)

    if validate:
        _validate_rows(df, SurveyResponse, sample_size=100)

    output_path = _upsert_parquet(
        DATA_ROOT / "survey_responses.parquet", df, ["response_id"],
        archive=archive,
    )
    return output_path


def ingest_survey_metadata(
    df: pl.DataFrame, validate: bool = True, *, archive: bool = True,
) -> Path:
    """Write or append survey metadata. Deduplicates by (survey_year, canonical_operator)."""
    if validate:
        _validate_rows(df, SurveyMetadata)
    return _upsert_parquet(
        DATA_ROOT / "survey_metadata.parquet",
        df,
        ["survey_year", "canonical_operator"],
        label="metadata records",
        archive=archive,
    )


def ingest_survey_weights(
    df: pl.DataFrame, validate: bool = True, *, archive: bool = True,
) -> Path:
    """Write or append survey weights. Deduplicates by (response_id, weight_scheme)."""
    if validate:
        _validate_rows(df, SurveyWeight)
    return _upsert_parquet(
        DATA_ROOT / "survey_weights.parquet",
        df,
        ["response_id", "weight_scheme"],
        label="weight records",
        archive=archive,
    )


def ingest(
    responses: pl.DataFrame,
    weights: pl.DataFrame,
    metadata: pl.DataFrame,
    *,
    validate: bool = True,
    archive: bool = True,
) -> dict[str, Path]:
    """Ingest all three tables for an operator/year and validate integrity.

    When *validate* is ``True`` (default), **all three tables are validated
    before any data is written** so a validation failure never leaves the
    warehouse in a partially-ingested state.

    Write order: metadata → responses → weights → referential integrity.

    Returns:
        Mapping of table name to output path.

    Raises:
        ValueError: If validation or referential integrity checks fail.
    """
    operator = metadata["canonical_operator"][0]
    year = metadata["survey_year"][0]

    # Validate everything up-front so a failure never partially writes.
    if validate:
        responses = enforce_dataframe_types(responses)
        _validate_rows(metadata, SurveyMetadata)
        _validate_rows(weights, SurveyWeight)
        _validate_rows(responses, SurveyResponse)
        validate_referential_integrity(responses, weights, metadata)

    paths: dict[str, Path] = {}
    paths["metadata"] = ingest_survey_metadata(metadata, validate=False, archive=archive)
    paths["responses"] = ingest_survey_responses(
        responses,
        survey_year=year,
        canonical_operator=operator,
        validate=False,
        archive=archive,
    )
    paths["weights"] = ingest_survey_weights(weights, validate=False, archive=archive)

    return paths


# ========== Tableau Hyper export ==========


def export_to_hyper(
    output_path: Path | str,
    tables: dict[str, pl.DataFrame] | None = None,
) -> int:
    """Export DataFrames (or the standard warehouse parquets) to a Tableau .hyper file.

    When *tables* is ``None`` the three well-known warehouse files
    (``survey_responses``, ``survey_weights``, ``survey_metadata``) are read
    from ``DATA_ROOT`` and exported.  When *tables* is a ``dict`` mapping
    table names to DataFrames those tables are exported instead.

    Returns:
        Total number of rows exported across all tables.

    Raises:
        FileNotFoundError: If *tables* is ``None`` and
            ``survey_responses.parquet`` does not exist.
    """
    output_path = Path(output_path)

    if tables is None:
        responses_path = DATA_ROOT / "survey_responses.parquet"
        if not responses_path.exists():
            msg = f"survey_responses.parquet not found at {responses_path}"
            raise FileNotFoundError(msg)

        tables = {"survey_responses": pl.read_parquet(responses_path)}

        weights_path = DATA_ROOT / "survey_weights.parquet"
        if weights_path.exists():
            tables["survey_weights"] = pl.read_parquet(weights_path)

        metadata_path = DATA_ROOT / "survey_metadata.parquet"
        if metadata_path.exists():
            tables["survey_metadata"] = pl.read_parquet(metadata_path)

    logger.info("Exporting %d table(s) to Tableau Hyper: %s", len(tables), output_path)

    total_rows = 0
    with (
        HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper,
        Connection(
            endpoint=hyper.endpoint,
            database=str(output_path),
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as conn,
    ):
        for tbl_name, dataframe in tables.items():
            # Build table definition via dict lookup
            columns = []
            for col_name, dtype in zip(dataframe.columns, dataframe.dtypes, strict=False):
                if isinstance(dtype, pl.Datetime):
                    sql_type = SqlType.timestamp()
                else:
                    sql_type = POLARS_TO_HYPER.get(dtype, SqlType.text())
                columns.append(TableDefinition.Column(col_name, sql_type, NULLABLE))
            table_def = TableDefinition(
                table_name=TableName("public", tbl_name), columns=columns,
            )

            conn.catalog.create_table(table_def)

            # Insert in 50k-row chunks
            for offset in range(0, len(dataframe), 50_000):
                chunk = dataframe.slice(offset, min(50_000, len(dataframe) - offset))
                data = [
                    tuple(
                        None if (isinstance(v, float) and math.isnan(v)) else v
                        for v in row
                    )
                    for row in chunk.iter_rows()
                ]
                with Inserter(conn, table_def.table_name) as inserter:
                    inserter.add_rows(data)
                    inserter.execute()

            logger.info("  %s: %s rows", tbl_name, f"{len(dataframe):,}")
            total_rows += len(dataframe)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info("  Exported %s rows total (%.2f MB)", f"{total_rows:,}", file_size_mb)

    return total_rows
