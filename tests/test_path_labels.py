"""Tests for path_labels transformation module."""

import polars as pl

from transit_passenger_tools.pipeline.path_labels import (
    derive_path_labels,
)


class TestTechShortCodes:
    """Test technology short code mapping."""

    def test_survey_mode_mapping(self):
        """Test vehicle_tech maps to survey_mode short code."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus", "Heavy Rail", "Commuter Rail"],
                "first_board_tech": [None, None, None],
                "last_alight_tech": [None, None, None],
            }
        )
        result = derive_path_labels(df)
        assert result["survey_mode"].to_list() == ["LB", "HR", "CR"]

    def test_first_board_mode_mapping(self):
        """Test first_board_tech maps to first_board_mode."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["first_board_mode"][0] == "LB"

    def test_last_alight_mode_mapping(self):
        """Test last_alight_tech maps to last_alight_mode."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": [None],
                "last_alight_tech": ["Express Bus"],
            }
        )
        result = derive_path_labels(df)
        assert result["last_alight_mode"][0] == "EB"

    def test_null_tech_maps_to_null(self):
        """Test null technology maps to null mode."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail", None],
                "first_board_tech": [None, None],
                "last_alight_tech": [None, None],
            }
        )
        result = derive_path_labels(df)
        assert result["survey_mode"][0] == "HR"
        assert result["survey_mode"][1] is None


class TestUsageFlags:
    """Test technology usage flag calculation."""

    def test_single_tech_used(self):
        """Test usage flag when only one tech used."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": [None],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["usedHR"][0] == 1
        assert result["usedLB"][0] == 0
        assert result["usedTotal"][0] == 1

    def test_multiple_tech_used(self):
        """Test usage flags when multiple techs used."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": ["Express Bus"],
            }
        )
        result = derive_path_labels(df)
        assert result["usedHR"][0] == 1
        assert result["usedLB"][0] == 1
        assert result["usedEB"][0] == 1
        assert result["usedTotal"][0] == 3

    def test_same_tech_multiple_legs(self):
        """Test that same tech in multiple legs counts once."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": ["Local Bus"],
            }
        )
        result = derive_path_labels(df)
        assert result["usedLB"][0] == 1
        assert result["usedTotal"][0] == 1


class TestBestMode:
    """Test BEST_MODE hierarchy calculation."""

    def test_hierarchy_single_mode(self):
        """Test BEST_MODE with single technology."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"],
                "first_board_tech": [None],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["BEST_MODE"][0] == "LB"

    def test_hierarchy_lb_over_none(self):
        """Test LB is default BEST_MODE."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"],
                "first_board_tech": [None],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["BEST_MODE"][0] == "LB"

    def test_hierarchy_hr_over_lb(self):
        """Test HR beats LB in hierarchy."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["BEST_MODE"][0] == "HR"

    def test_hierarchy_cr_over_hr(self):
        """Test CR beats HR in hierarchy."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": ["Commuter Rail"],
                "last_alight_tech": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["BEST_MODE"][0] == "CR"

    def test_hierarchy_cr_is_highest(self):
        """Test CR is highest in hierarchy."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Commuter Rail"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": ["Heavy Rail"],
            }
        )
        result = derive_path_labels(df)
        assert result["BEST_MODE"][0] == "CR"


class TestTransferType:
    """Test TRANSFER_TYPE calculation."""

    def test_no_transfers(self):
        """Test NO_TRANSFERS when boardings = 1."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": [None],
                "last_alight_tech": [None],
                "boardings": [1],
            }
        )
        result = derive_path_labels(df)
        assert result["TRANSFER_TYPE"][0] == "NO_TRANSFERS"
        assert result["nTransfers"][0] == 0

    def test_lb_hr_transfer(self):
        """Test LB_HR transfer type."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Heavy Rail"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": [None],
                "boardings": [2],
            }
        )
        result = derive_path_labels(df)
        assert result["TRANSFER_TYPE"][0] == "LB_HR"
        assert result["nTransfers"][0] == 1

    def test_same_mode_transfer(self):
        """Test same-mode transfer (LB_LB)."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"],
                "first_board_tech": ["Local Bus"],
                "last_alight_tech": ["Local Bus"],
                "boardings": [3],
            }
        )
        result = derive_path_labels(df)
        assert result["TRANSFER_TYPE"][0] == "LB_LB"

    def test_eb_cr_transfer(self):
        """Test EB_CR transfer type."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Commuter Rail"],
                "first_board_tech": ["Express Bus"],
                "last_alight_tech": [None],
                "boardings": [2],
            }
        )
        result = derive_path_labels(df)
        assert result["TRANSFER_TYPE"][0] == "EB_CR"


class TestPeriod:
    """Test period code mapping."""

    def test_all_periods(self):
        """Test all day_part values map correctly."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"] * 5,
                "first_board_tech": [None] * 5,
                "last_alight_tech": [None] * 5,
                "day_part": ["EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING"],
            }
        )
        result = derive_path_labels(df)
        assert result["period"].to_list() == ["EA", "AM", "MD", "PM", "EV"]

    def test_null_day_part(self):
        """Test null day_part maps to null period."""
        df = pl.DataFrame(
            {
                "vehicle_tech": ["Local Bus"],
                "first_board_tech": [None],
                "last_alight_tech": [None],
                "day_part": [None],
            }
        )
        result = derive_path_labels(df)
        assert result["period"][0] is None


class TestTransformPreservesData:
    """Test that transform preserves original data."""

    def test_preserves_original_columns(self):
        """Test all original columns preserved."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "survey_name": ["BART", "BART"],
                "vehicle_tech": ["Heavy Rail", "Heavy Rail"],
                "first_board_tech": [None, None],
                "last_alight_tech": [None, None],
                "boardings": [1, 2],
            }
        )
        result = derive_path_labels(df)
        assert "id" in result.columns
        assert "survey_name" in result.columns
        assert result["id"].to_list() == [1, 2]
