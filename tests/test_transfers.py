"""Tests for transfers transformation module."""

import polars as pl
import pytest

from transit_passenger_tools.codebook import TechnologyType
from transit_passenger_tools.pipeline.transfers import derive_transfer_fields


class TestTechnologyAssignment:
    """Test technology assignment to transfer legs."""

    def test_no_transfers_single_boarding(self):
        """Test no transfers results in 1 boarding."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_route_before_survey_board": [None],
                "second_before_route_before_survey_board": [None],
                "third_before_route_before_survey_board": [None],
                "first_after_route_after_survey_alight": [None],
                "second_after_route_after_survey_alight": [None],
                "third_after_route_after_survey_alight": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["boardings"][0] == 1


class TestTechnologyFlags:
    """Test technology presence flag calculation."""

    def test_heavy_rail_present(self):
        """Test heavy rail flag set when vehicle_tech is heavy rail."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["heavy_rail_present"][0] is True
        assert result["local_bus_present"][0] is False

    def test_multiple_technologies_present(self):
        """Test multiple technology flags when transfers use different modes."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [TechnologyType.LIGHT_RAIL.value],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["heavy_rail_present"][0] is True
        assert result["local_bus_present"][0] is True
        assert result["light_rail_present"][0] is True
        assert result["ferry_present"][0] is False


class TestBoardingsCalculation:
    """Test boardings calculation logic."""

    def test_boardings_no_transfers(self):
        """Test boardings = 1 when no transfers."""
        df = pl.DataFrame(
            {
                "survey_name": ["Caltrain"],
                "survey_year": [2024],
                "vehicle_tech": ["commuter rail"],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["boardings"][0] == 1

    def test_boardings_one_transfer_before(self):
        """Test boardings = 2 with one transfer before."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["boardings"][0] == 2

    def test_boardings_transfers_before_and_after(self):
        """Test boardings with transfers before and after."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": ["express bus"],
                "third_before_technology": [None],
                "first_after_technology": [TechnologyType.LIGHT_RAIL.value],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["boardings"][0] == 4  # 2 before + 1 survey + 1 after

    def test_boardings_all_six_transfers(self):
        """Test maximum boardings with all 6 transfer legs used."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": ["express bus"],
                "third_before_technology": ["ferry"],
                "first_after_technology": [TechnologyType.LIGHT_RAIL.value],
                "second_after_technology": ["commuter rail"],
                "third_after_technology": [TechnologyType.LOCAL_BUS.value],
            }
        )
        result = derive_transfer_fields(df)
        assert result["boardings"][0] == 7  # 3 before + 1 survey + 3 after


class TestTransferFromTo:
    """Test transfer_from and transfer_to derivation."""

    def test_transfer_from_last_before_operator(self):
        """Test transfer_from is last (closest) before operator."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_operator": ["AC TRANSIT"],
                "second_before_operator": ["SAMTRANS"],
                "third_before_operator": ["VTA"],
                "first_after_operator": [None],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": [TechnologyType.LOCAL_BUS.value],
                "third_before_technology": [TechnologyType.LOCAL_BUS.value],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["transfer_from"][0] == "VTA"  # Third (last) before

    def test_transfer_to_first_after_operator(self):
        """Test transfer_to is first after operator."""
        df = pl.DataFrame(
            {
                "survey_name": ["Caltrain"],
                "survey_year": [2024],
                "vehicle_tech": ["commuter rail"],
                "first_before_operator": [None],
                "second_before_operator": [None],
                "third_before_operator": [None],
                "first_after_operator": ["VTA"],
                "second_after_operator": ["AC TRANSIT"],
                "third_after_operator": [None],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [TechnologyType.LIGHT_RAIL.value],
                "second_after_technology": [TechnologyType.LOCAL_BUS.value],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["transfer_to"][0] == "VTA"  # First after


class TestFirstBoardLastAlight:
    """Test first_board_tech and last_alight_tech derivation."""

    def test_first_board_tech_no_transfers_before(self):
        """Test first_board_tech is vehicle_tech when no transfers before."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["first_board_tech"][0] == TechnologyType.HEAVY_RAIL.value

    def test_first_board_tech_with_transfer_before(self):
        """Test first_board_tech is earliest technology when transfers before."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [TechnologyType.LOCAL_BUS.value],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["first_board_tech"][0] == TechnologyType.LOCAL_BUS.value

    def test_last_alight_tech_with_transfers_after(self):
        """Test last_alight_tech is latest technology when transfers after."""
        df = pl.DataFrame(
            {
                "survey_name": ["Caltrain"],
                "survey_year": [2024],
                "vehicle_tech": ["commuter rail"],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [TechnologyType.LIGHT_RAIL.value],
                "second_after_technology": [TechnologyType.LOCAL_BUS.value],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["last_alight_tech"][0] == TechnologyType.LOCAL_BUS.value


class TestNullTechnologyHandling:
    """Test that null technologies remain null (not converted to 'Missing' string)."""

    def test_null_technologies_remain_null(self):
        """Test null values in technology columns remain null."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_technology": [None],
                "second_before_technology": [None],
                "third_before_technology": [None],
                "first_after_technology": [None],
                "second_after_technology": [None],
                "third_after_technology": [None],
            }
        )
        result = derive_transfer_fields(df)
        assert result["first_before_technology"][0] is None
        assert result["second_before_technology"][0] is None
        assert result["third_before_technology"][0] is None
        assert result["first_after_technology"][0] is None
        assert result["second_after_technology"][0] is None
        assert result["third_after_technology"][0] is None


class TestRouteToTechnologyMapping:
    """Test route-to-technology mapping via canonical route crosswalk."""

    def test_unmapped_route_raises_error(self):
        """Test that unmapped routes raise ValueError with helpful message."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_route_before_survey_board": ["INVALID_ROUTE_123"],
                "first_before_operator": ["NONEXISTENT_OPERATOR"],
            }
        )

        with pytest.raises(ValueError, match="Cannot map routes to technology for first_before"):
            derive_transfer_fields(df)

    def test_valid_route_mapping(self):
        """Test that valid routes from canonical crosswalk are mapped correctly."""
        # Using actual data from canonical_route_crosswalk.csv
        df = pl.DataFrame(
            {
                "survey_name": ["AC Transit"],
                "survey_year": [2018],
                "vehicle_tech": [TechnologyType.LOCAL_BUS.value],
                "first_after_route_after_survey_alight": [
                    "AC Transit 12 - Dtn. Oakland /Dtn. Berkeley/4th St. Harrison"
                ],
                "first_after_operator": ["AC TRANSIT"],
            }
        )

        result = derive_transfer_fields(df)
        # Technology value is normalized to match enum
        assert result["first_after_technology"][0] == TechnologyType.LOCAL_BUS.value
        assert result["boardings"][0] == 2

    def test_multiple_transfer_routes_mapped(self):
        """Test that multiple transfer routes are all mapped correctly."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_route_before_survey_board": [
                    "AC Transit 1 - San Leandro Bart- Dtn. Oakland"
                ],
                "first_before_operator": ["AC TRANSIT"],
                "first_after_route_after_survey_alight": [
                    "AC Transit 12 - Dtn. Oakland /Dtn. Berkeley/4th St. Harrison"
                ],
                "first_after_operator": ["AC TRANSIT"],
            }
        )

        result = derive_transfer_fields(df)
        # Technology values are normalized to match enum
        assert result["first_before_technology"][0] == TechnologyType.LOCAL_BUS.value
        assert result["first_after_technology"][0] == TechnologyType.LOCAL_BUS.value
        assert result["boardings"][0] == 3
        assert result["local_bus_present"][0] is True
        assert result["heavy_rail_present"][0] is True

    def test_null_operator_skips_lookup(self):
        """Test that null operator/route values don't trigger lookups."""
        df = pl.DataFrame(
            {
                "survey_name": ["BART"],
                "survey_year": [2024],
                "vehicle_tech": [TechnologyType.HEAVY_RAIL.value],
                "first_before_route_before_survey_board": [None],
                "first_before_operator": [None],
            }
        )

        result = derive_transfer_fields(df)

        # Technology should be null
        assert result["first_before_technology"][0] is None
        assert result["boardings"][0] == 1
