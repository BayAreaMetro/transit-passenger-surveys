"""Tests for the warehouse export helpers (read_responses, CSV, Hyper)."""

import polars as pl
import pytest

from transit_passenger_tools import database


@pytest.fixture
def temp_hive(monkeypatch, tmp_path):
    """Create a temporary data directory and patch DATA_ROOT to it."""
    test_hive = tmp_path / "test_hive"
    test_hive.mkdir()
    monkeypatch.setattr(database, "DATA_ROOT", test_hive)
    return test_hive


@pytest.fixture
def warehouse(temp_hive):
    """Write small responses + weights parquets into the temp warehouse.

    Responses: 3 rows across BART/2020, BART/2024, ACE/2020. Weights: response
    "1" has two schemes (so "latest" selection is observable), "2" has one
    scheme, "3" has none (so the left join must preserve it with null weights).
    """
    responses = pl.DataFrame(
        {
            "response_id": ["1", "2", "3"],
            "survey_id": ["BART_2020", "BART_2024", "ACE_2020"],
            "survey_year": [2020, 2024, 2020],
            "canonical_operator": ["BART", "BART", "ACE"],
        }
    )
    responses.write_parquet(temp_hive / "survey_responses.parquet")

    weights = pl.DataFrame(
        {
            "response_id": ["1", "1", "2"],
            "weight_scheme": ["old", "new", "old"],
            "boarding_weight": [10.0, 20.0, 30.0],
            "trip_weight": [1.0, 2.0, 3.0],
            "description": [None, None, None],
        }
    )
    weights.write_parquet(temp_hive / "survey_weights.parquet")
    return temp_hive


# ========== read_responses ==========


def test_read_responses_unfiltered(warehouse):  # noqa: ARG001
    """Without filters, all responses are returned."""
    assert database.read_responses().height == 3


def test_read_responses_filters(warehouse):  # noqa: ARG001
    """Operator (case-insensitive), year, and survey_id each narrow the rows."""
    assert database.read_responses(operator="bart").height == 2
    assert database.read_responses(year=2020).height == 2
    assert database.read_responses(operator="BART", year=2024).height == 1
    assert database.read_responses(survey_id="ACE_2020")["response_id"][0] == "3"
    assert database.read_responses(operator="NOPE").is_empty()


def test_read_responses_missing_raises(temp_hive):  # noqa: ARG001
    """A missing responses table is a hard error."""
    with pytest.raises(FileNotFoundError):
        database.read_responses()


# ========== export_to_csv ==========


def test_combined_left_join_preserves_all_responses(warehouse, tmp_path):  # noqa: ARG001
    """Combined CSV joins weight columns and keeps every response row."""
    out = tmp_path / "combined.csv"
    rows = database.export_to_csv(out)

    assert rows == 3  # left join preserves all responses
    df = pl.read_csv(out)
    assert df.height == 3
    assert {"boarding_weight", "trip_weight", "weight_scheme"} <= set(df.columns)
    # Response "3" has no weight -> null after the left join.
    row3 = df.filter(pl.col("response_id") == 3)
    assert row3["boarding_weight"][0] is None


def test_default_keeps_latest_weight_per_response(warehouse, tmp_path):  # noqa: ARG001
    """With no scheme specified, the last weight row per response wins."""
    out = tmp_path / "combined.csv"
    database.export_to_csv(out)

    df = pl.read_csv(out)
    row1 = df.filter(pl.col("response_id") == 1)
    assert row1["weight_scheme"][0] == "new"
    assert row1["boarding_weight"][0] == 20.0


def test_weight_scheme_filters(warehouse, tmp_path):  # noqa: ARG001
    """Specifying a scheme joins only that scheme's weights."""
    out = tmp_path / "combined.csv"
    database.export_to_csv(out, weight_scheme="old")

    df = pl.read_csv(out)
    row1 = df.filter(pl.col("response_id") == 1)
    assert row1["weight_scheme"][0] == "old"
    assert row1["boarding_weight"][0] == 10.0
    # "2" also has the "old" scheme; "3" has none -> null.
    assert df.filter(pl.col("response_id") == 3)["boarding_weight"][0] is None


def test_include_weights_false_exports_responses_only(warehouse, tmp_path):  # noqa: ARG001
    """With weights excluded, no weight columns are written."""
    out = tmp_path / "responses.csv"
    database.export_to_csv(out, include_weights=False)

    df = pl.read_csv(out)
    assert df.height == 3
    assert "boarding_weight" not in df.columns
    assert "weight_scheme" not in df.columns


def test_missing_weights_parquet_exports_responses_only(temp_hive, tmp_path):
    """No weights file -> responses are still exported without weight columns."""
    pl.DataFrame({"response_id": ["1"], "survey_year": [2020]}).write_parquet(
        temp_hive / "survey_responses.parquet"
    )
    out = tmp_path / "combined.csv"
    rows = database.export_to_csv(out)

    assert rows == 1
    assert "boarding_weight" not in pl.read_csv(out).columns


def test_csv_filter_by_operator_and_year(warehouse, tmp_path):  # noqa: ARG001
    """Operator + year together narrow the CSV to a single survey."""
    out = tmp_path / "combined.csv"
    rows = database.export_to_csv(out, operator="bart", year=2024)

    assert rows == 1
    assert pl.read_csv(out)["response_id"][0] == 2


def test_csv_filter_no_match_writes_empty(warehouse, tmp_path):  # noqa: ARG001
    """A filter matching nothing writes a header-only CSV and returns 0."""
    out = tmp_path / "combined.csv"
    rows = database.export_to_csv(out, operator="NOPE")

    assert rows == 0
    assert pl.read_csv(out).height == 0


# ========== export_to_hyper ==========


def test_hyper_exports_all_tables(warehouse, tmp_path):  # noqa: ARG001
    """Unfiltered Hyper export writes a file with responses + all weight rows."""
    out = tmp_path / "export.hyper"
    total = database.export_to_hyper(out)

    assert out.exists()
    assert total == 6  # 3 responses + 3 weight rows


def test_hyper_filter_restricts_weights_for_integrity(warehouse, tmp_path):  # noqa: ARG001
    """Filtering responses also restricts weights to the surviving responses."""
    out = tmp_path / "export.hyper"
    # operator BART -> responses {1, 2}; weights for 1 (old,new) + 2 (old) = 3.
    total = database.export_to_hyper(out, operator="BART")

    assert total == 2 + 3


def test_hyper_weight_scheme_filters(warehouse, tmp_path):  # noqa: ARG001
    """weight_scheme keeps only that scheme in the weights table."""
    out = tmp_path / "export.hyper"
    # scheme "old" -> weight rows for 1 and 2 = 2; all 3 responses.
    total = database.export_to_hyper(out, weight_scheme="old")

    assert total == 3 + 2


def test_hyper_missing_responses_raises(temp_hive, tmp_path):  # noqa: ARG001
    """A missing responses table is a hard error for Hyper too."""
    with pytest.raises(FileNotFoundError):
        database.export_to_hyper(tmp_path / "export.hyper")


# ========== available_weight_schemes ==========


def test_available_weight_schemes(warehouse):  # noqa: ARG001
    """available_weight_schemes returns the distinct schemes, sorted."""
    assert database.available_weight_schemes() == ["new", "old"]


def test_available_weight_schemes_empty_without_file(temp_hive):  # noqa: ARG001
    """No weights file -> empty scheme list (no error)."""
    assert database.available_weight_schemes() == []
