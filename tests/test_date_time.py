"""Unit tests for date_time transformation module.

The date_time transform expects preprocessed data with:
- day_of_week: String like "Monday", "Tuesday", etc.
- survey_time: String time like "08:30:00" or "08:30"

It standardizes:
- day_of_week: Canonical day field
- time_period: Time period (EARLY AM, AM PEAK, MIDDAY, PM PEAK, EVENING)
"""

import polars as pl
import pytest

from transit_passenger_tools.codebook import DayOfWeek, DayPart
from transit_passenger_tools.pipeline import date_time


class TestDayFieldStandardization:
    """Test day field standardization."""

    def test_preserves_day_of_week(self):
        """Test that canonical day_of_week is preserved."""
        df = pl.DataFrame(
            {
                "day_of_week": [
                    DayOfWeek.MONDAY.value,
                    DayOfWeek.TUESDAY.value,
                    DayOfWeek.WEDNESDAY.value,
                    DayOfWeek.THURSDAY.value,
                    DayOfWeek.FRIDAY.value,
                ],
                "survey_time": ["12:00"] * 5,
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["day_of_week"].to_list() == df["day_of_week"].to_list()

    def test_legacy_day_of_the_week_renamed(self):
        """Test that legacy day_of_the_week input is renamed."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [
                    DayOfWeek.SATURDAY.value,
                    DayOfWeek.SUNDAY.value,
                ],
                "survey_time": ["12:00"] * 2,
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert "day_of_week" in result.columns
        assert "day_of_the_week" not in result.columns
        assert result["day_of_week"].to_list() == [DayOfWeek.SATURDAY.value, DayOfWeek.SUNDAY.value]


class TestTimePeriodDerivation:
    """Test time_period derivation from survey_time."""

    def test_early_am_hours(self):
        """Test that hours 3-5 map to EARLY_AM."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value] * 3,
                "survey_time": ["03:00:00", "04:30:00", "05:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(tp == DayPart.EARLY_AM.value for tp in result["time_period"].to_list())

    def test_am_peak_hours(self):
        """Test that hours 6-9 map to AM_PEAK."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value] * 4,
                "survey_time": ["06:00:00", "07:15:00", "08:30:00", "09:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(tp == DayPart.AM_PEAK.value for tp in result["time_period"].to_list())

    def test_midday_hours(self):
        """Test that hours 10-14 map to MIDDAY."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value] * 5,
                "survey_time": ["10:00:00", "11:30:00", "12:00:00", "13:15:00", "14:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(tp == DayPart.MIDDAY.value for tp in result["time_period"].to_list())

    def test_pm_peak_hours(self):
        """Test that hours 15-18 map to PM_PEAK."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value] * 4,
                "survey_time": ["15:00:00", "16:30:00", "17:45:00", "18:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(tp == DayPart.PM_PEAK.value for tp in result["time_period"].to_list())

    def test_evening_hours(self):
        """Test that hours 19-23 and 0-2 map to EVENING."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value] * 6,
                "survey_time": [
                    "19:00:00",
                    "20:30:00",
                    "22:00:00",
                    "23:59:00",
                    "00:30:00",
                    "02:59:00",
                ],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(tp == DayPart.EVENING.value for tp in result["time_period"].to_list())

    def test_null_time_produces_null_time_period(self):
        """Test that null survey_time produces null time_period."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value, DayOfWeek.TUESDAY.value],
                "survey_time": ["12:00:00", None],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] == DayPart.MIDDAY.value
        assert result["time_period"][1] is None

    def test_existing_time_period_is_preserved(self):
        """Test that source-provided time_period wins over derived time."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value],
                "survey_time": ["08:30:00"],
                "time_period": [DayPart.PM_PEAK.value],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] == DayPart.PM_PEAK.value

    def test_legacy_day_part_backfills_time_period(self):
        """Test that legacy day_part is promoted to time_period."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value],
                "day_part": [DayPart.AM_PEAK.value],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] == DayPart.AM_PEAK.value


class TestValidation:
    """Test input validation."""

    def test_missing_day_of_week_raises_error(self):
        """Test that missing day_of_week column raises ValueError."""
        df = pl.DataFrame(
            {
                "survey_time": ["12:00:00"],
            }
        )

        with pytest.raises(ValueError, match="day_of_week"):
            date_time.derive_temporal_fields(df)

    def test_missing_survey_time_handled_gracefully(self):
        """Test that missing survey_time column is handled (time_period will be null)."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] is None


class TestTimeFormatHandling:
    """Test various time format inputs."""

    def test_hhmm_format(self):
        """Test HH:MM format without seconds."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value],
                "survey_time": ["08:30"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] == DayPart.AM_PEAK.value

    def test_hhmmss_format(self):
        """Test HH:MM:SS format."""
        df = pl.DataFrame(
            {
                "day_of_week": [DayOfWeek.MONDAY.value],
                "survey_time": ["08:30:45"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["time_period"][0] == DayPart.AM_PEAK.value
