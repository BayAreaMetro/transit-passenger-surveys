"""Tests for schema migration breaking change detection."""

import polars as pl
import pytest

from transit_passenger_tools import db


@pytest.fixture
def temp_data_lake(monkeypatch, tmp_path):
    """Create a temporary data lake directory."""
    test_lake = tmp_path / "test_data_lake"
    test_lake.mkdir()

    # Patch DATA_LAKE_ROOT to use temp directory
    monkeypatch.setattr(db, "DATA_LAKE_ROOT", test_lake)

    return test_lake


@pytest.fixture
def old_schema_data():
    """Create DataFrame with old schema (hispanic: str)."""
    return pl.DataFrame(
        {
            "response_id": [1, 2, 3],
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
            "response_id": [4, 5, 6],
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


def test_schema_mismatch_column_rename_detected(temp_data_lake, old_schema_data, new_schema_data):
    """Test that column rename (hispanic → is_hispanic) is detected."""
    # Write old schema data to partition
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    old_file = partition_dir / "data-1-test.parquet"
    old_schema_data.write_parquet(old_file)

    # Attempt to ingest new schema data - should fail
    with pytest.raises(ValueError, match=r"SCHEMA VERSION MISMATCH"):
        db.ingest_survey_batch(
            df=new_schema_data,
            survey_year=2020,
            canonical_operator="TEST",
            validate=False,  # Skip pydantic validation for this test
        )


def test_schema_mismatch_error_message_content(temp_data_lake, old_schema_data, new_schema_data):
    """Test that error message contains helpful information."""
    # Write old schema data
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    old_file = partition_dir / "data-1-test.parquet"
    old_schema_data.write_parquet(old_file)

    # Capture error
    with pytest.raises(ValueError, match="SCHEMA VERSION MISMATCH") as exc_info:
        db.ingest_survey_batch(
            df=new_schema_data, survey_year=2020, canonical_operator="TEST", validate=False
        )

    error_msg = str(exc_info.value)

    # Verify error message contains key information
    assert "SCHEMA VERSION MISMATCH" in error_msg
    assert "TEST/2020" in error_msg
    assert "hispanic" in error_msg
    assert "is_hispanic" in error_msg
    assert "migration" in error_msg.lower()
    assert "scripts/migrate_schema_v1_to_v2.py" in error_msg
    assert f"Current schema version: {db.SCHEMA_VERSION}" in error_msg


def test_schema_mismatch_type_change_detected(temp_data_lake):
    """Test that type changes for same column are detected."""
    # Create data with different types for same column
    old_data = pl.DataFrame(
        {
            "response_id": [1, 2],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "gender": ["Male", "Female"],  # String type
        }
    )

    new_data = pl.DataFrame(
        {
            "response_id": [3, 4],
            "survey_year": [2020, 2020],
            "canonical_operator": ["TEST", "TEST"],
            "gender": pl.Series([1, 2], dtype=pl.Int32),  # Int type
        }
    )

    # Write old schema
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    old_data.write_parquet(partition_dir / "data-1-test.parquet")

    # Attempt to ingest different type
    with pytest.raises(ValueError, match=r"Type mismatch.*gender"):
        db.ingest_survey_batch(
            df=new_data, survey_year=2020, canonical_operator="TEST", validate=False
        )


def test_schema_compatible_ingestion_succeeds(temp_data_lake):
    """Test that ingestion succeeds when schemas match."""
    # Create compatible data (same schema)
    first_batch = pl.DataFrame(
        {
            "response_id": [1, 2],
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
            "response_id": [3, 4],
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

    # Write first batch
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    first_batch.write_parquet(partition_dir / "data-1-test.parquet")

    # Ingest second batch - should succeed (same schema)
    try:
        db.ingest_survey_batch(
            df=second_batch, survey_year=2020, canonical_operator="TEST", validate=False
        )
        # If we get here without exception, test passes
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Schema validation incorrectly rejected compatible schema: {e}")
        raise


@pytest.mark.usefixtures("temp_data_lake")
def test_new_partition_no_schema_check(new_schema_data):
    """Test that new partitions (no existing data) don't trigger schema checks."""
    # Ingest into fresh partition (no existing files)
    try:
        db.ingest_survey_batch(
            df=new_schema_data,
            survey_year=2025,  # New year
            canonical_operator="NEWOP",  # New operator
            validate=False,
        )
        # Should succeed without schema checks
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Schema check incorrectly triggered for new partition: {e}")
        raise


def test_schema_check_multiple_issues(temp_data_lake):
    """Test that multiple schema issues are all reported."""
    # Old schema with multiple columns
    old_data = pl.DataFrame(
        {
            "response_id": [1],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "hispanic": ["HISPANIC/LATINO OR OF SPANISH ORIGIN"],
            "gender": ["Male"],
            "age": [25],  # Different column name
        }
    )

    # New schema with renamed column and type change
    new_data = pl.DataFrame(
        {
            "response_id": [2],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [True],  # Renamed
            "gender": pl.Series([1], dtype=pl.Int32),  # Type changed
            "age": [35],  # Same name, should check type
        }
    )

    # Write old schema
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    old_data.write_parquet(partition_dir / "data-1-test.parquet")

    # Attempt ingestion
    with pytest.raises(ValueError, match="SCHEMA VERSION MISMATCH") as exc_info:
        db.ingest_survey_batch(
            df=new_data, survey_year=2020, canonical_operator="TEST", validate=False
        )

    error_msg = str(exc_info.value)

    # Should report column rename
    assert "hispanic" in error_msg
    assert "is_hispanic" in error_msg

    # Should report type mismatch for gender
    assert "gender" in error_msg
    assert "Type mismatch" in error_msg or "type mismatch" in error_msg.lower()


def test_schema_check_with_additional_columns(temp_data_lake):
    """Test that adding new columns (non-breaking change) is allowed."""
    # Old schema
    old_data = pl.DataFrame(
        {
            "response_id": [1],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [True],
        }
    )

    # New schema with additional column
    new_data = pl.DataFrame(
        {
            "response_id": [2],
            "survey_year": [2020],
            "canonical_operator": ["TEST"],
            "is_hispanic": [False],
            "new_field": ["extra data"],  # New column added
        }
    )

    # Write old schema
    partition_dir = temp_data_lake / "survey_responses" / "operator=TEST" / "year=2020"
    partition_dir.mkdir(parents=True)
    old_data.write_parquet(partition_dir / "data-1-test.parquet")

    # Should succeed - adding columns is non-breaking
    try:
        db.ingest_survey_batch(
            df=new_data, survey_year=2020, canonical_operator="TEST", validate=False
        )
    except ValueError as e:
        if "SCHEMA VERSION MISMATCH" in str(e):
            pytest.fail(f"Adding new columns should not trigger schema mismatch: {e}")
        raise
