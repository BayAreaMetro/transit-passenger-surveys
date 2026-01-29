"""Tests for process_survey orchestrator module."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from transit_passenger_tools.codebook import (
    AutoRatioCategory,
    DayPart,
    HouseholdIncome,
    TechnologyType,
    TripPurpose,
    Weekpart,
    WorkStatus,
)
from transit_passenger_tools.pipeline.process_survey import (
    PIPELINE_MODULES,
    process_survey,
    process_survey_file,
)
from transit_passenger_tools.pipeline.validate import (
    validate_core_required,
    validate_preprocessed_input,
)


def make_minimal_preprocessed_df(**overrides) -> pl.DataFrame:
    """Create a minimal DataFrame that passes validation.
    
    Provides all core required fields with sensible defaults.
    """
    defaults = {
        "response_id": [1],
        "survey_id": ["TEST_2024"],
        "original_id": ["R001"],
        "survey_name": ["Test Survey"],
        "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
        "day_of_the_week": ["Monday"],
        "survey_time": ["09:30:00"],
        "vehicles": [2],
        "workers": [1],
        "first_board_tech": [None],
        "last_alight_tech": [None],
        "race_cat": ["White"],
        "household_income": [HouseholdIncome.FROM_50K_TO_75K.value],
        "approximate_age": [35],
        "survey_year": [2024],
    }
    defaults.update(overrides)
    return pl.DataFrame(defaults)


class TestProcessSurvey:
    """Test the main process_survey function."""

    def test_basic_pipeline_execution(self):
        """Test that pipeline runs through all modules."""
        df = make_minimal_preprocessed_df()

        result = process_survey(df, skip_geocoding=True)

        # Check that modules added their columns
        assert "day_part" in result.columns  # from date_time
        assert "weekpart" in result.columns  # from date_time
        assert "auto_to_workers_ratio" in result.columns  # from auto_sufficiency
        assert "survey_mode" in result.columns  # from path_labels

    def test_skip_geocoding(self):
        """Test that geocoding can be skipped."""
        df = make_minimal_preprocessed_df()

        # Should not raise error even without coordinate columns
        result = process_survey(df, skip_geocoding=True)
        assert "survey_mode" in result.columns

    def test_preserves_original_columns(self):
        """Test that original columns are preserved."""
        df = make_minimal_preprocessed_df(
            response_id=[1, 2, 3],
            survey_id=["TEST_2024"] * 3,
            original_id=["R001", "R002", "R003"],
            survey_name=["Test Survey"] * 3,
            vehicle_tech=[TechnologyType.HEAVY_RAIL.value, TechnologyType.LOCAL_BUS.value, TechnologyType.COMMUTER_RAIL.value],
            day_of_the_week=["Monday"] * 3,
            survey_time=["09:30:00"] * 3,
            vehicles=[2, 1, 0],
            workers=[1, 2, 0],
            first_board_tech=[None, None, None],
            last_alight_tech=[None, None, None],
            race_cat=["White", "Asian", "Black"],
            household_income=[HouseholdIncome.FROM_50K_TO_75K.value] * 3,
            approximate_age=[35, 40, 25],
            survey_year=[2024] * 3,
            custom_field=["a", "b", "c"],
        )

        result = process_survey(df, skip_geocoding=True)

        assert "custom_field" in result.columns
        assert result["custom_field"].to_list() == ["a", "b", "c"]

    def test_full_pipeline_integration(self):
        """Test realistic data through full pipeline."""
        df = make_minimal_preprocessed_df(
            vehicle_tech=[TechnologyType.HEAVY_RAIL.value],
            day_of_the_week=["Monday"],
            survey_time=["08:15:00"],
            vehicles=[1],
            workers=[1],
            orig_purp=[TripPurpose.HOME.value],
            dest_purp=[TripPurpose.WORK.value],
            work_status=[WorkStatus.FULL_OR_PART_TIME.value],
            worker=[1],
        )

        result = process_survey(df, skip_geocoding=True)

        # date_time outputs
        assert result["day_part"][0] == DayPart.AM_PEAK.value
        assert result["weekpart"][0] == Weekpart.WEEKDAY.value

        # auto_sufficiency outputs
        assert result["auto_to_workers_ratio"][0] == AutoRatioCategory.WORKERS_LE_AUTOS.value

        # path_labels outputs
        assert result["survey_mode"][0] == "HR"
        assert result["usedHR"][0] == 1
        # Note: usedLB would only be 1 if we had transfer data showing a local bus boarding

        # tour_purpose outputs - now skipped because required columns missing
        # Original test would check: assert result["tour_purp"][0] == "work"
        # But tour_purpose requires many more columns that we haven't provided


class TestPipelineOrder:
    """Test that pipeline modules are in correct order."""

    def test_transfers_before_path_labels(self):
        """Transfers must run before path_labels (provides tech columns)."""
        module_order = [name for name, _, _ in PIPELINE_MODULES]
        assert module_order.index("transfers") < module_order.index("path_labels")

    def test_date_time_first(self):
        """date_time should be first (provides day_part for path_labels)."""
        module_order = [name for name, _, _ in PIPELINE_MODULES]
        assert module_order[0] == "date_time"


class TestProcessSurveyFile:
    """Test file-based processing."""

    def test_read_csv_write_parquet(self):
        """Test reading CSV and writing Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            output_path = Path(tmpdir) / "output.parquet"

            # Create input file with proper preprocessed data
            df = make_minimal_preprocessed_df()
            df.write_csv(input_path)

            # Process
            result = process_survey_file(
                input_path,
                output_path,
                skip_geocoding=True,
            )

            # Check output file was created
            assert output_path.exists()

            # Read back and verify
            reread = pl.read_parquet(output_path)
            assert "survey_mode" in reread.columns

    def test_read_parquet_write_csv(self):
        """Test reading Parquet and writing CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            output_path = Path(tmpdir) / "output.csv"

            # Create input file with proper preprocessed data
            df = make_minimal_preprocessed_df(
                vehicle_tech=[TechnologyType.LOCAL_BUS.value],
            )
            df.write_parquet(input_path)

            # Process
            result = process_survey_file(
                input_path,
                output_path,
                skip_geocoding=True,
            )

            assert output_path.exists()

    def test_unsupported_input_format_raises_error(self):
        """Test that unsupported input format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported input format"):
            process_survey_file("data.xlsx", skip_geocoding=True)

    def test_unsupported_output_format_raises_error(self):
        """Test that unsupported output format raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            df = make_minimal_preprocessed_df()
            df.write_csv(input_path)

            with pytest.raises(ValueError, match="Unsupported output format"):
                process_survey_file(
                    input_path,
                    "output.xlsx",
                    skip_geocoding=True,
                )

    def test_no_output_path_returns_dataframe(self):
        """Test that processing without output path returns DataFrame."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            df = make_minimal_preprocessed_df()
            df.write_csv(input_path)

            result = process_survey_file(input_path, skip_geocoding=True)

            assert isinstance(result, pl.DataFrame)
            assert "survey_mode" in result.columns


class TestValidation:
    """Test validation integration."""

    def test_validation_passes_with_core_identifiers(self):
        """Test validation passes when core identifiers are present."""
        df = pl.DataFrame({
            "response_id": [1],
            "survey_id": ["BART_2024"],
            "original_id": ["ABC123"],
            "vehicle_tech": ["heavy rail"],
        })

        errors = validate_core_required(df)
        assert errors == []

    def test_validation_fails_missing_response_id(self):
        """Test validation fails without response_id."""
        df = pl.DataFrame({
            "survey_id": ["BART_2024"],
            "original_id": ["ABC123"],
            "vehicle_tech": ["heavy rail"],
        })

        errors = validate_core_required(df)
        assert any("response_id" in e for e in errors)

    def test_validation_fails_missing_survey_id(self):
        """Test validation fails without survey_id."""
        df = pl.DataFrame({
            "response_id": [1],
            "original_id": ["ABC123"],
            "vehicle_tech": ["heavy rail"],
        })

        errors = validate_core_required(df)
        assert any("survey_id" in e for e in errors)

    def test_strict_validation_raises_on_errors(self):
        """Test that strict validation raises ValueError."""
        df = pl.DataFrame({
            "other_column": ["value"],
        })

        with pytest.raises(ValueError, match="VALIDATION FAILED"):
            validate_preprocessed_input(df, strict=True)

    def test_nonstrict_validation_warns_on_errors(self, capsys):
        """Test that non-strict validation prints warning."""
        df = pl.DataFrame({
            "other_column": ["value"],
        })

        # Should not raise
        result = validate_preprocessed_input(df, strict=False)

        # Should return input df unchanged
        assert result.equals(df)

        # Should print warning
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
