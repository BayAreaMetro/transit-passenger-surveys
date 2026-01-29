"""Tests for demographics transformation module."""

import polars as pl
import pytest

from transit_passenger_tools.codebook import Race
from transit_passenger_tools.pipeline.demographics import process_race


def make_race_df(**overrides):
    """Helper to create test DataFrames with proper null handling for race fields."""
    defaults = {
        "race_dmy_ind": [0],
        "race_dmy_asn": [0],
        "race_dmy_blk": [0],
        "race_dmy_hwi": [0],
        "race_dmy_wht": [0],
        "race_dmy_mdl_estn": [0],
    }
    defaults.update(overrides)

    df = pl.DataFrame(defaults)

    # Add race_other_string and race_cat as nulls if not provided
    if "race_other_string" not in df.columns:
        df = df.with_columns([pl.lit(None).cast(pl.Utf8).alias("race_other_string")])
    if "race_cat" not in df.columns:
        df = df.with_columns([pl.lit(None).cast(pl.Utf8).alias("race_cat")])

    return df


class TestRaceProcessing:
    """Test race processing logic."""

    @pytest.mark.parametrize(
        "overrides,expected_race,test_id",
        [
            ({"race_dmy_asn": [1]}, Race.ASIAN, "single_race_asian"),
            ({"race_dmy_blk": [1]}, Race.BLACK, "single_race_black"),
            ({"race_dmy_wht": [1]}, Race.WHITE, "single_race_white"),
            ({"race_dmy_mdl_estn": [1]}, Race.WHITE, "single_middle_eastern_maps_to_white"),
            ({"race_dmy_wht": [1], "race_dmy_mdl_estn": [1]}, Race.WHITE, "white_plus_middle_eastern"),
            ({"race_dmy_asn": [1], "race_dmy_blk": [1]}, Race.OTHER, "multi_racial_asian_black"),
            ({"race_dmy_ind": [1]}, Race.OTHER, "native_american_only"),
            ({"race_dmy_hwi": [1]}, Race.OTHER, "pacific_islander_only"),
            ({"race_dmy_ind": [1], "race_dmy_asn": [1]}, Race.OTHER, "native_american_in_combination"),
            ({"race_dmy_hwi": [1], "race_dmy_blk": [1]}, Race.OTHER, "pacific_islander_in_combination"),
            ({"race_other_string": ["Human"]}, Race.OTHER, "other_string_only"),
            ({}, Race.MISSING, "no_race_specified"),
        ],
    )
    def test_race_categorization(self, overrides, expected_race, test_id):
        """Test race categorization logic with various input combinations."""
        df = make_race_df(**overrides)
        result = process_race(df)
        assert result["race"][0] == expected_race.value


class TestNullHandling:
    """Test handling of null/missing values in race fields."""

    def test_null_race_dmy_fields(self):
        """Test null values in race_dmy fields treated as 0."""
        df = make_race_df(
            race_dmy_asn=[None],
            race_dmy_blk=[None],
            race_dmy_ind=[None],
            race_dmy_hwi=[None],
            race_dmy_wht=[1],
            race_dmy_mdl_estn=[None]
        )
        result = process_race(df)
        assert result["race"][0] == Race.WHITE.value

    def test_short_race_other_string_ignored(self):
        """Test race_other_string <= 2 chars is ignored."""
        df = make_race_df(race_other_string=["xy"])  # 2 chars - too short
        result = process_race(df)
        assert result["race"][0] == Race.MISSING.value


class TestNullRaceHandling:
    """Test that race_other_string and race_cat handle actual nulls correctly."""

    def test_null_race_other_string_not_counted(self):
        """Test that null race_other_string doesn't count toward OTHER category."""
        df = make_race_df()  # Creates with null race_other_string and race_cat
        result = process_race(df)
        # With no race data, should be MISSING (not OTHER)
        assert result["race"][0] == Race.MISSING.value

    def test_null_race_cat_not_counted(self):
        """Test that null race_cat doesn't affect race categorization."""
        df = make_race_df(race_dmy_asn=[1])  # One race selected
        result = process_race(df)
        # Should be ASIAN (null race_cat doesn't interfere)
        assert result["race"][0] == Race.ASIAN.value

    def test_missing_columns_created_as_null(self):
        """Test that process_race handles missing race_other_string and race_cat columns."""
        # Don't provide race_other_string or race_cat at all
        df = pl.DataFrame({
            "race_dmy_ind": [0],
            "race_dmy_asn": [1],
            "race_dmy_blk": [0],
            "race_dmy_hwi": [0],
            "race_dmy_wht": [0],
            "race_dmy_mdl_estn": [0],
        })
        result = process_race(df)
        # Should process correctly and return ASIAN
        assert result["race"][0] == Race.ASIAN.value


# TODO: Add language and fare medium tests once full transform() is called in pipeline

