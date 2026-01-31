"""Pytest configuration and fixtures."""

import os
import shutil
import zipfile
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Point, box


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment and generate shapefile fixtures."""
    # Set environment variable to use test config
    os.environ["PIPELINE_CONFIG"] = "config/pipeline.test.yaml"

    # Create minimal fixture directories
    fixtures_dir = Path("tests/fixtures")
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    test_hive = fixtures_dir / "test_hive"
    test_hive.mkdir(exist_ok=True)

    # Generate test shapefiles
    _create_test_shapefiles(fixtures_dir)

    yield

    # Cleanup
    os.environ.pop("PIPELINE_CONFIG", None)


def _create_test_shapefiles(fixtures_dir: Path):
    """Generate minimal test shapefiles as zip files."""
    # Skip if already exists (for speed during development)
    if all(
        (fixtures_dir / f"test_{name}.zip").exists()
        for name in ["tm1_taz", "tm2_taz", "tm2_maz", "counties", "tracts", "puma", "stations"]
    ):
        return

    # Test coordinates from test_geocoding.py
    _sf_lat, _sf_lon = 37.7793, -122.4193
    _oak_lat, _oak_lon = 37.8044, -122.2712

    # Temporary directory for generation
    temp_dir = fixtures_dir / "temp_shapefiles"
    temp_dir.mkdir(exist_ok=True)

    try:
        # TM1 TAZ
        gdf = gpd.GeoDataFrame(
            {
                "TAZ1454": [100, 200],
                "geometry": [
                    box(-122.52, 37.70, -122.38, 37.81),
                    box(-122.30, 37.75, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_tm1_taz")

        # TM2 TAZ
        gdf = gpd.GeoDataFrame(
            {
                "TAZ": [1, 2],
                "geometry": [
                    box(-122.52, 37.70, -122.38, 37.81),
                    box(-122.30, 37.75, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_tm2_taz")

        # TM2 MAZ
        gdf = gpd.GeoDataFrame(
            {
                "MAZ": [1, 2, 3, 4],
                "geometry": [
                    box(-122.52, 37.70, -122.45, 37.81),
                    box(-122.45, 37.70, -122.38, 37.81),
                    box(-122.30, 37.75, -122.225, 37.85),
                    box(-122.225, 37.75, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_tm2_maz")

        # Counties
        gdf = gpd.GeoDataFrame(
            {
                "GEOID": ["06075", "06001"],
                "NAME": ["San Francisco", "Alameda"],
                "geometry": [
                    box(-122.52, 37.70, -122.38, 37.81),
                    box(-122.30, 37.75, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_counties")

        # Tracts
        gdf = gpd.GeoDataFrame(
            {
                "GEOID": ["06075017101", "06001400100"],
                "NAME": ["Census Tract 171.01", "Census Tract 4001"],
                "geometry": [
                    box(-122.52, 37.70, -122.38, 37.81),
                    box(-122.30, 37.75, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_tracts")

        # PUMA
        gdf = gpd.GeoDataFrame(
            {
                "GEOID20": ["0607501", "0600101"],
                "NAME": ["SF PUMA", "Alameda PUMA"],
                "geometry": [
                    box(-122.52, 37.70, -122.15, 37.85),
                    box(-122.52, 37.70, -122.15, 37.85),
                ],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_puma")

        # Stations
        gdf = gpd.GeoDataFrame(
            {
                "NAME": ["SF Civic Center", "Oakland 12th St"],
                "geometry": [Point(_sf_lon, _sf_lat), Point(_oak_lon, _oak_lat)],
            },
            crs="EPSG:4326",
        )
        _save_and_zip(gdf, temp_dir, fixtures_dir, "test_stations")

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def _save_and_zip(gdf: gpd.GeoDataFrame, temp_dir: Path, output_dir: Path, name: str):
    """Save GeoDataFrame as shapefile and zip it."""
    shp_path = temp_dir / f"{name}.shp"
    gdf.to_file(shp_path)

    zip_path = output_dir / f"{name}.zip"
    related_files = list(temp_dir.glob(f"{name}.*"))

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in related_files:
            zipf.write(file_path, file_path.name)
