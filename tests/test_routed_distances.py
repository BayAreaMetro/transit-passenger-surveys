"""Tests for OSRM routed distance pipeline module."""

import polars as pl
import pytest
import requests

from transit_passenger_tools import config as config_mod
from transit_passenger_tools.pipeline.routed_distances import (
    _METERS_TO_MILES,
    _MODE_TO_PROFILE,
    FIELD_DEPENDENCIES,
    IMPLAUSIBLE_MILES,
    _create_osrm_client,
    _route_leg,
    add_implausible_flags,
    compute_routed_distances,
)

OSRM_URL = "http://model3-c.ad.mtc.ca.gov:5000"

# Known Bay Area coordinates
SF_LAT, SF_LON = 37.7749, -122.4194
OAK_LAT, OAK_LON = 37.8044, -122.2712
DALY_CITY_LAT, DALY_CITY_LON = 37.6879, -122.4702
FREMONT_LAT, FREMONT_LON = 37.5485, -121.9886


def _osrm_reachable() -> bool:
    """Check if the MTC OSRM server is reachable."""
    try:
        r = requests.get(
            f"{OSRM_URL}/route/v1/driving/{SF_LON},{SF_LAT};{OAK_LON},{OAK_LAT}",
            timeout=5,
        )
    except (requests.RequestException, OSError):
        return False
    else:
        return r.status_code == 200


requires_osrm = pytest.mark.skipif(
    not _osrm_reachable(),
    reason="MTC OSRM server not reachable",
)


# ========== Unit tests (no server needed) ==========


class TestFieldDependencies:
    """Verify field dependency declarations."""

    def test_output_fields(self):
        """Output fields include routed distance, geometry, and flag columns."""
        assert "distance_orig_first_board_routed" in FIELD_DEPENDENCIES.outputs
        assert "distance_last_alight_dest_routed" in FIELD_DEPENDENCIES.outputs
        assert "geometry_orig_first_board_routed" in FIELD_DEPENDENCIES.outputs
        assert "geometry_last_alight_dest_routed" in FIELD_DEPENDENCIES.outputs
        assert "access_leg_implausible" in FIELD_DEPENDENCIES.outputs
        assert "egress_leg_implausible" in FIELD_DEPENDENCIES.outputs


class TestImplausibleFlags:
    """Verify the walk-implausibility flagging."""

    def test_long_walk_and_bike_are_flagged(self):
        """Walk/Bike legs over their ceilings flag; other modes never do."""
        walk_max = IMPLAUSIBLE_MILES["Walk"]
        bike_max = IMPLAUSIBLE_MILES["Bike"]
        df = pl.DataFrame({
            "access_mode": ["Walk", "Walk", "PNR", "Bike"],
            "egress_mode": ["Bike", "Bike", "Walk", None],
            "distance_orig_first_board_routed": [
                walk_max + 1, 0.4, 30.0, bike_max + 1,  # walk hi, walk lo, PNR, bike hi
            ],
            "distance_last_alight_dest_routed": [
                bike_max + 2, bike_max - 1, 0.2, 8.0,  # bike hi, bike ok, walk lo, null
            ],
        })

        out = add_implausible_flags(df)

        # access: long walk flagged; short walk no; long PNR no (drive); long bike yes.
        assert out["access_leg_implausible"].to_list() == [True, False, False, True]
        # egress: long bike yes; sub-ceiling bike no; short walk no; null mode no.
        assert out["egress_leg_implausible"].to_list() == [True, False, False, False]

    def test_missing_mode_column_is_safe(self):
        """With no mode columns present, flags default to False (no error)."""
        df = pl.DataFrame({
            "distance_orig_first_board_routed": [50.0],
            "distance_last_alight_dest_routed": [50.0],
        })
        out = add_implausible_flags(df)
        assert out["access_leg_implausible"].to_list() == [False]
        assert out["egress_leg_implausible"].to_list() == [False]

    def test_input_fields_include_coordinates(self):
        """Input fields include all eight coordinate columns."""
        for col in ["orig_lat", "orig_lon", "first_board_lat", "first_board_lon"]:
            assert col in FIELD_DEPENDENCIES.inputs

    def test_mode_columns_are_optional(self):
        """Access and egress mode are optional inputs."""
        assert "access_mode" in FIELD_DEPENDENCIES.optional_inputs
        assert "egress_mode" in FIELD_DEPENDENCIES.optional_inputs


class TestModeToProfile:
    """Verify AccessEgressMode → OSRM profile mapping."""

    def test_walk_maps_to_foot(self):
        """Walk mode uses OSRM foot profile."""
        assert _MODE_TO_PROFILE["Walk"] == "foot"

    def test_bike_maps_to_bicycle(self):
        """Bike mode uses OSRM bicycle profile."""
        assert _MODE_TO_PROFILE["Bike"] == "bicycle"

    def test_drive_modes_map_to_driving(self):
        """PNR, KNR, and TNC all use OSRM driving profile."""
        for mode in ("PNR", "KNR", "TNC"):
            assert _MODE_TO_PROFILE[mode] == "driving"

    def test_none_falls_back_to_driving(self):
        """None mode falls back to OSRM driving profile."""
        assert _MODE_TO_PROFILE[None] == "driving"


class TestComputeRoutedDistancesSkip:
    """Test that compute_routed_distances skips when URL is None."""

    def test_null_url_returns_null_columns(self, monkeypatch):
        """When osrm_server_url is None, output columns should be all-null."""
        original = config_mod.get_config()
        monkeypatch.setattr(original, "osrm_server_url", None)

        df = pl.DataFrame({
            "orig_lat": [SF_LAT],
            "orig_lon": [SF_LON],
            "first_board_lat": [OAK_LAT],
            "first_board_lon": [OAK_LON],
            "last_alight_lat": [OAK_LAT],
            "last_alight_lon": [OAK_LON],
            "dest_lat": [DALY_CITY_LAT],
            "dest_lon": [DALY_CITY_LON],
        })

        result = compute_routed_distances(df)

        assert "distance_orig_first_board_routed" in result.columns
        assert "distance_last_alight_dest_routed" in result.columns
        assert "geometry_orig_first_board_routed" in result.columns
        assert "geometry_last_alight_dest_routed" in result.columns
        assert result["distance_orig_first_board_routed"].null_count() == 1
        assert result["distance_last_alight_dest_routed"].null_count() == 1
        assert result["geometry_orig_first_board_routed"].null_count() == 1
        assert result["geometry_last_alight_dest_routed"].null_count() == 1


class TestRouteLegMissingColumns:
    """Test _route_leg when coordinate columns are missing."""

    def test_missing_columns_returns_nulls(self):
        """Missing coordinate columns produce all-null output."""
        df = pl.DataFrame({"response_id": ["A", "B"]})
        dist, geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col=None,
            dist_col="test_dist",
            geom_col="test_geom",
            server_url="http://unused",
        )
        assert dist.null_count() == 2
        assert dist.name == "test_dist"
        assert geom.null_count() == 2
        assert geom.name == "test_geom"


# ========== Integration tests (require OSRM server) ==========


@requires_osrm
class TestOSRMClient:
    """Test OSRM client creation and basic routing."""

    def test_create_client(self):
        """Client has correct base_url and profile."""
        client = _create_osrm_client(OSRM_URL, "driving")
        assert client.base_url == OSRM_URL
        assert client.profile == "driving"

    def test_driving_route(self):
        """Driving route returns a plausible SF-Oakland distance."""
        client = _create_osrm_client(OSRM_URL, "driving")
        result = client.Route([(SF_LON, SF_LAT), (OAK_LON, OAK_LAT)])
        dist_m = result["routes"][0]["distance"]
        dist_miles = dist_m * _METERS_TO_MILES
        # SF → Oakland driving: ~11 miles
        assert 8 < dist_miles < 16

    def test_foot_route(self):
        """Foot route returns a plausible SF-Oakland distance."""
        client = _create_osrm_client(OSRM_URL, "foot")
        result = client.Route([(SF_LON, SF_LAT), (OAK_LON, OAK_LAT)])
        dist_m = result["routes"][0]["distance"]
        dist_miles = dist_m * _METERS_TO_MILES
        assert 8 < dist_miles < 16

    def test_bicycle_route(self):
        """Bicycle route returns a plausible SF-Oakland distance."""
        client = _create_osrm_client(OSRM_URL, "bicycle")
        result = client.Route([(SF_LON, SF_LAT), (OAK_LON, OAK_LAT)])
        dist_m = result["routes"][0]["distance"]
        dist_miles = dist_m * _METERS_TO_MILES
        assert 8 < dist_miles < 16


@requires_osrm
class TestRouteLeg:
    """Test _route_leg end-to-end against OSRM."""

    def test_single_row(self):
        """Single valid row returns a non-null distance."""
        df = pl.DataFrame({
            "orig_lat": [SF_LAT],
            "orig_lon": [SF_LON],
            "dest_lat": [OAK_LAT],
            "dest_lon": [OAK_LON],
        })
        dist, geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col=None,
            dist_col="test_dist",
            geom_col="test_geom",
            server_url=OSRM_URL,
        )
        assert dist.null_count() == 0
        assert 8 < dist[0] < 16  # miles
        # Geometry is a non-empty encoded polyline string
        assert geom.null_count() == 0
        assert isinstance(geom[0], str)
        assert len(geom[0]) > 0

    def test_multiple_rows(self):
        """Multiple rows all return plausible Bay Area distances."""
        df = pl.DataFrame({
            "orig_lat": [SF_LAT, DALY_CITY_LAT],
            "orig_lon": [SF_LON, DALY_CITY_LON],
            "dest_lat": [OAK_LAT, FREMONT_LAT],
            "dest_lon": [OAK_LON, FREMONT_LON],
        })
        dist, geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col=None,
            dist_col="test_dist",
            geom_col="test_geom",
            server_url=OSRM_URL,
        )
        assert dist.null_count() == 0
        assert len(dist) == 2
        # Both should be reasonable Bay Area distances
        assert all(1 < d < 50 for d in dist.to_list())
        # Both rows have geometry
        assert geom.null_count() == 0

    def test_null_coordinates_produce_null(self):
        """Rows with null coordinates get null distance."""
        df = pl.DataFrame({
            "orig_lat": [SF_LAT, None],
            "orig_lon": [SF_LON, -122.0],
            "dest_lat": [OAK_LAT, OAK_LAT],
            "dest_lon": [OAK_LON, OAK_LON],
        })
        dist, geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col=None,
            dist_col="test_dist",
            geom_col="test_geom",
            server_url=OSRM_URL,
        )
        assert dist[0] is not None  # valid row
        assert dist[1] is None  # null coordinate row
        assert geom[0] is not None  # valid row has geometry
        assert geom[1] is None  # null coordinate row

    def test_mode_column_selects_profile(self):
        """Walk and drive should produce different routed distances."""
        df = pl.DataFrame({
            "orig_lat": [SF_LAT, SF_LAT],
            "orig_lon": [SF_LON, SF_LON],
            "dest_lat": [OAK_LAT, OAK_LAT],
            "dest_lon": [OAK_LON, OAK_LON],
            "access_mode": ["Walk", "PNR"],
        })
        dist, _geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col="access_mode",
            dist_col="test_dist",
            geom_col="test_geom",
            server_url=OSRM_URL,
        )
        assert dist.null_count() == 0
        walk_dist = dist[0]
        drive_dist = dist[1]
        # Distances should differ (different networks/profiles)
        assert walk_dist != drive_dist

    def test_null_mode_uses_driving_fallback(self):
        """Null access_mode should fall back to driving, not error."""
        df = pl.DataFrame({
            "orig_lat": [SF_LAT],
            "orig_lon": [SF_LON],
            "dest_lat": [OAK_LAT],
            "dest_lon": [OAK_LON],
            "access_mode": [None],
        })
        dist, _geom = _route_leg(
            df,
            from_lat="orig_lat",
            from_lon="orig_lon",
            to_lat="dest_lat",
            to_lon="dest_lon",
            mode_col="access_mode",
            dist_col="test_dist",
            geom_col="test_geom",
            server_url=OSRM_URL,
        )
        assert dist.null_count() == 0
        assert 8 < dist[0] < 16


@requires_osrm
class TestComputeRoutedDistances:
    """Test full compute_routed_distances function."""

    def test_both_legs_computed(self, monkeypatch):
        """Both access and egress legs return plausible distances."""
        original = config_mod.get_config()
        monkeypatch.setattr(original, "osrm_server_url", OSRM_URL)

        df = pl.DataFrame({
            "orig_lat": [SF_LAT],
            "orig_lon": [SF_LON],
            "first_board_lat": [OAK_LAT],
            "first_board_lon": [OAK_LON],
            "last_alight_lat": [DALY_CITY_LAT],
            "last_alight_lon": [DALY_CITY_LON],
            "dest_lat": [FREMONT_LAT],
            "dest_lon": [FREMONT_LON],
            "access_mode": ["Walk"],
            "egress_mode": ["PNR"],
        })

        result = compute_routed_distances(df)

        assert "distance_orig_first_board_routed" in result.columns
        assert "distance_last_alight_dest_routed" in result.columns

        access_dist = result["distance_orig_first_board_routed"][0]
        egress_dist = result["distance_last_alight_dest_routed"][0]

        assert access_dist is not None
        assert egress_dist is not None
        # SF→Oakland access ~9-12 mi, Daly City→Fremont egress ~25-35 mi
        assert 5 < access_dist < 20
        assert 15 < egress_dist < 45

        # Geometry columns are populated with encoded polyline strings
        assert "geometry_orig_first_board_routed" in result.columns
        assert "geometry_last_alight_dest_routed" in result.columns
        assert isinstance(result["geometry_orig_first_board_routed"][0], str)
        assert isinstance(result["geometry_last_alight_dest_routed"][0], str)

    def test_preserves_original_columns(self, monkeypatch):
        """Original DataFrame columns are preserved in output."""
        original = config_mod.get_config()
        monkeypatch.setattr(original, "osrm_server_url", OSRM_URL)

        df = pl.DataFrame({
            "response_id": ["R001"],
            "orig_lat": [SF_LAT],
            "orig_lon": [SF_LON],
            "first_board_lat": [OAK_LAT],
            "first_board_lon": [OAK_LON],
            "last_alight_lat": [OAK_LAT],
            "last_alight_lon": [OAK_LON],
            "dest_lat": [DALY_CITY_LAT],
            "dest_lon": [DALY_CITY_LON],
        })

        result = compute_routed_distances(df)

        assert "response_id" in result.columns
        assert result["response_id"][0] == "R001"
        assert len(result) == 1
