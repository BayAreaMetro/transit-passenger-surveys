"""Unit tests for auto_sufficiency transformation module."""

import polars as pl
import pytest

from transit_passenger_tools.pipeline import auto_sufficiency
from transit_passenger_tools.schemas.codebook import AutoRatioCategory


def test_text_to_numeric_mapping():
    """Test that text values are correctly mapped to numbers."""
    df = pl.DataFrame(
        {
            "vehicles": ["zero", "one", "two", "three", "four or more"],
            "workers": ["zero", "one", "two", "three", "four"],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert result["vehicle_numeric"].to_list() == [0, 1, 2, 3, 4]
    assert result["worker_numeric"].to_list() == [0, 1, 2, 3, 4]


def test_numeric_values_preserved():
    """Test that already-numeric values are preserved."""
    df = pl.DataFrame(
        {
            "vehicles": [0, 1, 2, 3, 4],
            "workers": [0, 1, 2, 3, 4],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert result["vehicle_numeric"].to_list() == [0, 1, 2, 3, 4]
    assert result["worker_numeric"].to_list() == [0, 1, 2, 3, 4]


def test_auto_suff_zero_autos():
    """Test that zero vehicles categorizes as 'Zero autos'."""
    df = pl.DataFrame(
        {
            "vehicles": [0, 0, 0],
            "workers": [0, 1, 2],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert all(result["auto_to_workers_ratio"] == AutoRatioCategory.ZERO_AUTOS.value)


def test_auto_suff_workers_greater():
    """Test 'Autos < workers' category."""
    df = pl.DataFrame(
        {
            "vehicles": [1, 1, 2],
            "workers": [2, 3, 3],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert all(result["auto_to_workers_ratio"] == AutoRatioCategory.WORKERS_GT_AUTOS.value)


def test_auto_suff_workers_less_or_equal():
    """Test 'Autos >= workers' category."""
    df = pl.DataFrame(
        {
            "vehicles": [2, 3, 2],
            "workers": [1, 2, 2],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert all(result["auto_to_workers_ratio"] == AutoRatioCategory.WORKERS_LE_AUTOS.value)


def test_invalid_vehicle_value_raises_error():
    """Test that unrecognized vehicle values raise ValueError."""
    df = pl.DataFrame(
        {
            "vehicles": ["invalid_value", "two"],
            "workers": ["one", "two"],
        }
    )

    with pytest.raises(ValueError, match="Unrecognized vehicle values"):
        auto_sufficiency.derive_auto_sufficiency(df)


def test_invalid_worker_value_raises_error():
    """Test that unrecognized worker values raise ValueError."""
    df = pl.DataFrame(
        {
            "vehicles": ["one", "two"],
            "workers": ["invalid_value", "two"],
        }
    )

    with pytest.raises(ValueError, match="Unrecognized worker values"):
        auto_sufficiency.derive_auto_sufficiency(df)


def test_null_values_handled():
    """Test that null values are handled gracefully."""
    df = pl.DataFrame(
        {
            "vehicles": ["one", None, "two"],
            "workers": [None, "one", "two"],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    # First row: vehicles=1, workers=null -> no category
    assert result["auto_to_workers_ratio"][0] is None

    # Second row: vehicles=null, workers=1 -> no category
    assert result["auto_to_workers_ratio"][1] is None

    # Third row: vehicles=2, workers=2 -> Autos >= workers
    assert result["auto_to_workers_ratio"][2] == AutoRatioCategory.WORKERS_LE_AUTOS.value


def test_case_insensitive_text_mapping():
    """Test that text mapping is case-insensitive."""
    df = pl.DataFrame(
        {
            "vehicles": ["Zero", "ONE", "Two"],
            "workers": ["ZERO", "one", "TWO"],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    assert result["vehicle_numeric"].to_list() == [0, 1, 2]
    assert result["worker_numeric"].to_list() == [0, 1, 2]


def test_edge_case_zero_workers_multiple_vehicles():
    """Test edge case: 0 workers but vehicles > 0."""
    df = pl.DataFrame(
        {
            "vehicles": [2],
            "workers": [0],
        }
    )

    result = auto_sufficiency.derive_auto_sufficiency(df)

    # Should not match any category (workers must be > 0 for non-zero categories)
    assert result["auto_to_workers_ratio"][0] is None
