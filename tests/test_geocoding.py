"""Tests for geocoding transformation module."""

import polars as pl

from transit_passenger_tools.pipeline.geocoding import (
    assign_zones_and_distances,
    haversine_km,
)
from transit_passenger_tools.utils.config import get_config


class TestHaversineFunction:
    """Test Haversine distance calculation."""

    def test_same_point_zero_distance(self):
        """Test that same point returns zero distance."""
        dist = haversine_km(37.7749, -122.4194, 37.7749, -122.4194)
        assert dist == 0.0

    def test_known_distance_sf_to_oakland(self):
        """Test known distance between SF and Oakland."""
        # SF City Hall to Oakland City Hall ~ 13 km
        sf_lat, sf_lon = 37.7793, -122.4193
        oak_lat, oak_lon = 37.8044, -122.2712

        dist = haversine_km(sf_lat, sf_lon, oak_lat, oak_lon)

        # Should be approximately 9-11 km
        assert 12 < dist < 14

    def test_known_distance_sf_to_la(self):
        """Test known distance between SF and LA."""
        # SF to LA ~350 miles
        sf_lat, sf_lon = 37.7749, -122.4194
        la_lat, la_lon = 34.0522, -118.2437

        dist = haversine_km(sf_lat, sf_lon, la_lat, la_lon)

        # Should be approximately 550-610 km
        assert 550 < dist < 610

    def test_null_coordinates_return_none(self):
        """Test that null coordinates return None."""
        assert haversine_km(None, -122.4194, 37.8044, -122.2712) is None
        assert haversine_km(37.7749, None, 37.8044, -122.2712) is None
        assert haversine_km(37.7749, -122.4194, None, -122.2712) is None
        assert haversine_km(37.7749, -122.4194, 37.8044, None) is None

    def test_symmetry(self):
        """Test that distance is symmetric (A to B = B to A)."""
        lat1, lon1 = 37.7749, -122.4194
        lat2, lon2 = 37.8044, -122.2712

        dist_ab = haversine_km(lat1, lon1, lat2, lon2)
        dist_ba = haversine_km(lat2, lon2, lat1, lon1)

        assert abs(dist_ab - dist_ba) < 0.001


class TestCalculateDistances:
    """Test distance calculation for DataFrames."""

    def test_orig_dest_distance(self):
        """Test origin to destination distance calculation."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749],
                "orig_lon": [-122.4194],
                "dest_lat": [37.8044],
                "dest_lon": [-122.2712],
            }
        )

        result = assign_zones_and_distances(df)

        assert "orig_dest_dist" in result.columns
        assert result["orig_dest_dist"][0] is not None
        assert 12 < result["orig_dest_dist"][0] < 14

    def test_missing_columns_handled(self):
        """Test that missing location columns result in null distance."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749],
                "orig_lon": [-122.4194],
                # No dest columns
            }
        )

        result = assign_zones_and_distances(df)

        assert "orig_dest_dist" in result.columns
        assert result["orig_dest_dist"][0] is None

    def test_null_coordinates_produce_null_distance(self):
        """Test that null coordinates result in null distance."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749, None],
                "orig_lon": [-122.4194, -122.4194],
                "dest_lat": [37.8044, 37.8044],
                "dest_lon": [-122.2712, -122.2712],
            }
        )

        result = assign_zones_and_distances(df)

        # First row should have valid distance
        assert result["orig_dest_dist"][0] is not None
        # Second row should have null (missing orig_lat)
        assert result["orig_dest_dist"][1] is None

    def test_all_distance_pairs_calculated(self):
        """Test that all configured distance pairs are calculated."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749],
                "orig_lon": [-122.4194],
                "dest_lat": [37.8044],
                "dest_lon": [-122.2712],
                "first_board_lat": [37.7800],
                "first_board_lon": [-122.4000],
                "survey_board_lat": [37.7850],
                "survey_board_lon": [-122.3900],
                "survey_alight_lat": [37.7900],
                "survey_alight_lon": [-122.3500],
                "last_alight_lat": [37.8000],
                "last_alight_lon": [-122.3000],
            }
        )

        result = assign_zones_and_distances(df)

        # Get expected distance columns from config
        config = get_config()
        for distance_config in config.geocoding_distances:
            dist_col = distance_config.column
            assert dist_col in result.columns
            assert result[dist_col][0] is not None


class TestTransform:
    """Test the main transform function."""

    def test_transform_calculates_distances_without_shapefiles(self):
        """Test that transform calculates distances when no shapefiles_dir provided."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749],
                "orig_lon": [-122.4194],
                "dest_lat": [37.8044],
                "dest_lon": [-122.2712],
            }
        )

        # No shapefiles_dir - should still calculate distances
        result = assign_zones_and_distances(df, shapefiles_dir=None)

        assert "orig_dest_dist" in result.columns
        assert result["orig_dest_dist"][0] is not None

    def test_transform_preserves_original_columns(self):
        """Test that transform preserves all original columns."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "survey_name": ["BART", "BART", "BART"],
                "orig_lat": [37.7749, 37.7800, 37.7850],
                "orig_lon": [-122.4194, -122.4000, -122.3900],
                "dest_lat": [37.8044, 37.8100, 37.8200],
                "dest_lon": [-122.2712, -122.2500, -122.2300],
            }
        )

        result = assign_zones_and_distances(df, shapefiles_dir=None)

        assert "id" in result.columns
        assert "survey_name" in result.columns
        assert result["id"].to_list() == [1, 2, 3]
        assert result["survey_name"].to_list() == ["BART", "BART", "BART"]

    def test_transform_multiple_rows(self):
        """Test transform handles multiple rows correctly."""
        df = pl.DataFrame(
            {
                "orig_lat": [37.7749, 37.8044, 37.8500],
                "orig_lon": [-122.4194, -122.2712, -122.2000],
                "dest_lat": [37.8044, 37.8500, 37.9000],
                "dest_lon": [-122.2712, -122.2000, -122.1500],
            }
        )

        result = assign_zones_and_distances(df, shapefiles_dir=None)

        # All rows should have distance values
        assert all(d is not None for d in result["orig_dest_dist"].to_list())
        # Distances should be different
        distances = result["orig_dest_dist"].to_list()
        assert len(set(distances)) > 1  # Not all same
