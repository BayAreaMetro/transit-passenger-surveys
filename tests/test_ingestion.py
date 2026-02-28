"""Tests for ingestion schema compatibility and archive behavior."""

from datetime import UTC, datetime

import polars as pl
import pytest

from transit_passenger_tools import database


@pytest.fixture
def temp_hive(monkeypatch, tmp_path):
    """Create a temporary data directory."""
    test_hive = tmp_path / "test_hive"
    test_hive.mkdir()
    (test_hive / "archive").mkdir()

    # Patch DATA_ROOT in the database module
    monkeypatch.setattr(database, "DATA_ROOT", test_hive)

    return test_hive


@pytest.fixture
def old_schema_data():
    """Create DataFrame with old schema (hispanic: str)."""
    return pl.DataFrame(
        {
            "response_id": pl.Series(["1", "2", "3"], dtype=pl.Utf8),
            "survey_id": ["TEST_2020", "TEST_2020", "TEST_2020"],
            "original_id": ["R001", "R002", "R003"],
            "survey_name": ["Test Survey", "Test Survey", "Test Survey"],
            "survey_year": [2020, 2020, 2020],
            "canonical_operator": ["TEST", "TEST", "TEST"],
            "hispanic": [
                "HISPANIC/LATINO OR OF SPANISH ORIGIN",
                "NOT HISPANIC/LATINO OR OF SPANISH ORIGIN",
                None,
            ],
            "approximate_age": [25, 35, 45],
            "persons": [2, 3, 1],
            "workers": [1, 2, 1],
            "vehicles": [1, 2, 0],
        }
    )


@pytest.fixture
def new_schema_data():
    """Create DataFrame with new schema (is_hispanic: bool)."""
    return pl.DataFrame(
        {
            "response_id": pl.Series(["4", "5", "6"], dtype=pl.Utf8),
            "survey_id": ["TEST_2020", "TEST_2020", "TEST_2020"],
            "original_id": ["R004", "R005", "R006"],
            "survey_name": ["Test Survey", "Test Survey", "Test Survey"],
            "survey_year": [2020, 2020, 2020],
            "canonical_operator": ["TEST", "TEST", "TEST"],
            "is_hispanic": [True, False, None],
            "approximate_age": [28, 38, 48],
            "persons": [2, 3, 1],
            "workers": [1, 2, 1],
            "vehicles": [1, 2, 0],
        }
    )


def test_schema_mismatch_column_rename_detected(temp_hive, old_schema_data, new_schema_data):
    """Test that column rename (hispanic -> is_hispanic) is detected."""
    # Write old schema data as the existing survey_responses file
    old_schema_data.write_parquet(temp_hive / "survey_responses.parquet")

    # Attempt to ingest new schema data - should fail
    with pytest.raises(ValueError, match=r"SCHEMA VERSION MISMATCH"):
        database.ingest_survey_responses(
            df=new_schema_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )


def test_schema_mismatch_error_message_content(temp_hive, old_schema_data, new_schema_data):
    """Test that error message contains helpful information."""
    # Write old schema data
    old_schema_data.write_parquet(temp_hive / "survey_responses.parquet")

    # Capture error
    with pytest.raises(ValueError, match="SCHEMA VERSION MISMATCH") as exc_info:
        database.ingest_survey_responses(
            df=new_schema_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )

    error_msg = str(exc_info.value)

    # Verify error message contains key information
    assert "SCHEMA VERSION MISMATCH" in error_msg
    assert "TEST/2020" in error_msg
    assert "hispanic" in error_msg
    assert "is_hispanic" in error_msg
    assert "migration" in error_msg.lower()


def test_schema_mismatch_type_change_detected(temp_hive):
    """Test that type changes for same column are detected.

    Uses a non-model column (``extra_metric``) so that
    ``enforce_dataframe_types`` does not normalise types before
    the schema-compatibility check runs.
    """
    old_data = pl.DataFrame(
        {
            "response_id": ["1", "2"],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "extra_metric": [1.0, 2.0],  # Float64
        }
    )

    new_data = pl.DataFrame(
        {
            "response_id": ["3", "4"],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "extra_metric": ["a", "b"],  # Utf8 - different from Float64
        }
    )

    # Write old schema
    old_data.write_parquet(temp_hive / "survey_responses.parquet")

    # Attempt to ingest different type
    with pytest.raises(ValueError, match=r"Type mismatch.*extra_metric"):
        database.ingest_survey_responses(
            df=new_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )


def test_schema_compatible_ingestion_succeeds(temp_hive):
    """Test that ingestion succeeds when schemas match."""
    # Create compatible data (same schema)
    first_batch = pl.DataFrame(
        {
            "response_id": ["1", "2"],
            "survey_id": ["TEST_2020", "TEST_2020"],
            "original_id": ["R001", "R002"],
            "survey_name": ["Test Survey", "Test Survey"],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "is_hispanic": [True, False],
            "approximate_age": [25, 35],
            "persons": [2, 3],
            "workers": [1, 2],
            "vehicles": [1, 2],
        }
    )

    second_batch = pl.DataFrame(
        {
            "response_id": ["3", "4"],
            "survey_id": ["TEST_2020", "TEST_2020"],
            "original_id": ["R003", "R004"],
            "survey_name": ["Test Survey", "Test Survey"],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "is_hispanic": [None, True],
            "approximate_age": [45, 55],
            "persons": [1, 2],
            "workers": [1, 1],
            "vehicles": [0, 1],
        }
    )

    # Write first batch as existing data
    first_batch.write_parquet(temp_hive / "survey_responses.parquet")

    # Ingest second batch - should succeed (same schema)
    try:
        database.ingest_survey_responses(
            df=second_batch,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Schema validation incorrectly rejected compatible schema: {e}")
        raise


def test_new_data_no_existing_file(temp_hive, new_schema_data):  # noqa: ARG001
    """Test that ingestion into fresh directory (no existing file) works."""
    try:
        database.ingest_survey_responses(
            df=new_schema_data,
            survey_year=2025,
            canonical_operator="NEWOP",
            validate=False,
        )
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Schema check incorrectly triggered for new data: {e}")
        raise


def test_schema_check_multiple_issues(temp_hive):
    """Test that multiple schema issues are all reported."""
    # Old schema with multiple columns
    old_data = pl.DataFrame(
        {
            "response_id": ["1"],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "hispanic": ["HISPANIC/LATINO OR OF SPANISH ORIGIN"],
            "gender": ["Male"],
            "age": [25],
        }
    )

    # New schema with renamed column and type change
    new_data = pl.DataFrame(
        {
            "response_id": ["2"],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [True],  # Renamed
            "gender": pl.Series([1], dtype=pl.Int32),  # Type changed
            "age": [35],  # Same name, should check type
        }
    )

    # Write old schema
    old_data.write_parquet(temp_hive / "survey_responses.parquet")

    # Attempt ingestion
    with pytest.raises(ValueError, match="SCHEMA VERSION MISMATCH") as exc_info:
        database.ingest_survey_responses(
            df=new_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )

    error_msg = str(exc_info.value)

    # Should report column rename
    assert "hispanic" in error_msg
    assert "is_hispanic" in error_msg

    # Should report type mismatch for gender
    assert "gender" in error_msg
    assert "Type mismatch" in error_msg or "type mismatch" in error_msg.lower()


def test_schema_check_with_additional_columns(temp_hive):
    """Test that adding new columns (non-breaking change) is allowed."""
    # Old schema
    old_data = pl.DataFrame(
        {
            "response_id": ["1"],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [True],
        }
    )

    # New schema with additional column
    new_data = pl.DataFrame(
        {
            "response_id": ["2"],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [False],
            "new_field": ["extra data"],  # New column added
        }
    )

    # Write old schema
    old_data.write_parquet(temp_hive / "survey_responses.parquet")

    # Should succeed - adding columns is non-breaking
    try:
        database.ingest_survey_responses(
            df=new_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,
        )
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Adding new columns should not trigger schema mismatch: {e}")
        raise


def test_archive_file_creates_dated_copy(temp_hive):
    """Test that archive_file moves the file with a date stamp."""
    test_file = temp_hive / "test.parquet"
    test_file.write_bytes(b"data")

    archived = database.archive_file(test_file)

    assert not test_file.exists()
    assert archived.exists()
    assert archived.parent.name == "archive"
    assert "test_" in archived.name


def test_archive_file_collision_handling(temp_hive):
    """Test same-day collision handling with counter suffix."""
    archive_dir = temp_hive / "archive"
    archive_dir.mkdir(exist_ok=True)

    today = datetime.now(UTC).date().isoformat()

    # Create an existing archive to cause a collision
    existing = archive_dir / f"test_{today}.parquet"
    existing.write_bytes(b"old")

    # Now archive a new file
    test_file = temp_hive / "test.parquet"
    test_file.write_bytes(b"new")

    archived = database.archive_file(test_file)

    assert archived.name == f"test_{today}_2.parquet"
    assert archived.exists()
    assert not test_file.exists()


def test_archive_file_nonexistent_raises(temp_hive):
    """Test that archiving a nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        database.archive_file(temp_hive / "does_not_exist.parquet")
