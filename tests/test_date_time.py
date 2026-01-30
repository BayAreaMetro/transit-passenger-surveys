"""Unit tests for date_time transformation module.

The date_time transform expects preprocessed data with:
- day_of_the_week: String like "Monday", "Tuesday", etc.
- survey_time: String time like "08:30:00" or "08:30"

It derives:
- weekpart: WEEKDAY or WEEKEND based on day_of_the_week
- day_part: Time period (EARLY AM, AM PEAK, MIDDAY, PM PEAK, EVENING) based on survey_time
"""

import polars as pl
import pytest

from transit_passenger_tools.pipeline import date_time
from transit_passenger_tools.schemas.codebook import DayOfWeek, DayPart, Weekpart


class TestWeekpartDerivation:
    """Test weekpart derivation from day_of_the_week."""

    def test_weekdays_map_to_weekday(self):
        """Test that Monday-Friday map to WEEKDAY."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [
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

        assert all(wp == Weekpart.WEEKDAY.value for wp in result["weekpart"].to_list())

    def test_weekends_map_to_weekend(self):
        """Test that Saturday-Sunday map to WEEKEND."""
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

        assert all(wp == Weekpart.WEEKEND.value for wp in result["weekpart"].to_list())

    def test_null_day_produces_null_weekpart(self):
        """Test that null day_of_the_week produces null weekpart."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value, None],
                "survey_time": ["12:00", "12:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["weekpart"][0] == Weekpart.WEEKDAY.value
        assert result["weekpart"][1] is None


class TestDayPartDerivation:
    """Test day_part derivation from survey_time."""

    def test_early_am_hours(self):
        """Test that hours 3-5 map to EARLY_AM."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value] * 3,
                "survey_time": ["03:00:00", "04:30:00", "05:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(dp == DayPart.EARLY_AM.value for dp in result["day_part"].to_list())

    def test_am_peak_hours(self):
        """Test that hours 6-9 map to AM_PEAK."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value] * 4,
                "survey_time": ["06:00:00", "07:15:00", "08:30:00", "09:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(dp == DayPart.AM_PEAK.value for dp in result["day_part"].to_list())

    def test_midday_hours(self):
        """Test that hours 10-14 map to MIDDAY."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value] * 5,
                "survey_time": ["10:00:00", "11:30:00", "12:00:00", "13:15:00", "14:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(dp == DayPart.MIDDAY.value for dp in result["day_part"].to_list())

    def test_pm_peak_hours(self):
        """Test that hours 15-18 map to PM_PEAK."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value] * 4,
                "survey_time": ["15:00:00", "16:30:00", "17:45:00", "18:59:00"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert all(dp == DayPart.PM_PEAK.value for dp in result["day_part"].to_list())

    def test_evening_hours(self):
        """Test that hours 19-23 and 0-2 map to EVENING."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value] * 6,
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

        assert all(dp == DayPart.EVENING.value for dp in result["day_part"].to_list())

    def test_null_time_produces_null_day_part(self):
        """Test that null survey_time produces null day_part."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value, DayOfWeek.TUESDAY.value],
                "survey_time": ["12:00:00", None],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["day_part"][0] == DayPart.MIDDAY.value
        assert result["day_part"][1] is None


class TestValidation:
    """Test input validation."""

    def test_missing_day_of_the_week_raises_error(self):
        """Test that missing day_of_the_week column raises ValueError."""
        df = pl.DataFrame(
            {
                "survey_time": ["12:00:00"],
            }
        )

        with pytest.raises(ValueError, match="day_of_the_week"):
            date_time.derive_temporal_fields(df)

    def test_missing_survey_time_handled_gracefully(self):
        """Test that missing survey_time column is handled (day_part will be null)."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value],
            }
        )

        result = date_time.derive_temporal_fields(df)

        # weekpart should still be derived
        assert result["weekpart"][0] == Weekpart.WEEKDAY.value
        # day_part should be null since no survey_time
        assert result["day_part"][0] is None


class TestTimeFormatHandling:
    """Test various time format inputs."""

    def test_hhmm_format(self):
        """Test HH:MM format without seconds."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value],
                "survey_time": ["08:30"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["day_part"][0] == DayPart.AM_PEAK.value

    def test_hhmmss_format(self):
        """Test HH:MM:SS format."""
        df = pl.DataFrame(
            {
                "day_of_the_week": [DayOfWeek.MONDAY.value],
                "survey_time": ["08:30:45"],
            }
        )

        result = date_time.derive_temporal_fields(df)

        assert result["day_part"][0] == DayPart.AM_PEAK.value
