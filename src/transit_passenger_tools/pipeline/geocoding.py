"""Geocoding module for survey standardization.

Assigns geographic zones to all location types using spatial joins.
"""
import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Location types to geocode
LOCATION_TYPES = [
    "home",
    "work",
    "school",
    "orig",  # trip origin
    "dest",  # trip destination
    "survey_board",  # where passenger boarded surveyed vehicle
    "survey_alight", # where passenger alighted surveyed vehicle
    "first_board",   # first boarding on full trip
    "last_alight"    # last alighting on full trip
]

# Geographic zone types to assign
ZONE_TYPES = [
    "tm1_taz",      # Travel Model 1 TAZ
    "tm2_taz",      # Travel Model 2 TAZ
    "tm2_maz",      # Travel Model 2 MAZ
    "county_geoid", # Census county GEOID
    "tract_geoid",  # Census tract GEOID
    "puma_geoid"    # Census PUMA GEOID
]


def geocode_all_locations(
    df: pl.DataFrame,
    shapefiles_dir: Path | None = None
) -> pl.DataFrame:
    """Geocode all location types and assign geographic zones.

    For each location type (home, work, school, etc.), assigns:
    - TM1 TAZ
    - TM2 TAZ and MAZ
    - Census county, tract, and PUMA

    Args:
        df: Input DataFrame with lat/lon columns for each location
        shapefiles_dir: Directory containing zone shapefiles
    
    Returns:
        DataFrame with added geographic zone columns
    """
    if shapefiles_dir is None:
        shapefiles_dir = Path("M:/Data/GIS layers")

    logger.info("Starting geocoding for all locations")

    # Import add_zone function
    from ..add_zone import add_zone_to_lat_lon

    result_df = df.clone()

    # Process each location type
    for location in LOCATION_TYPES:
        lat_col = f"{location}_lat"
        lon_col = f"{location}_lon"

        # Check if these columns exist
        if lat_col not in result_df.columns or lon_col not in result_df.columns:
            logger.debug(f"Skipping {location}: columns not found")
            continue

        # Count non-null coordinates
        non_null_count = result_df.filter(
            pl.col(lat_col).is_not_null() & pl.col(lon_col).is_not_null()
        ).height

        if non_null_count == 0:
            logger.debug(f"Skipping {location}: no valid coordinates")
            continue

        logger.info(f"Geocoding {location} ({non_null_count:,} records with coordinates)")

        # Geocode TM1 TAZ
        logger.debug(f"  Assigning TM1 TAZ for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "TM1_taz" / "bayarea_rtaz1454_rev1_WGS84.shp",
            zone_name_in_shapefile="TAZ1454",
            new_zone_colname=f"{location}_tm1_taz"
        )

        # Geocode TM2 TAZ
        logger.debug(f"  Assigning TM2 TAZ for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "TM2_maz_taz_v2.2" / "taz1454_v2.2_WGS84.shp",
            zone_name_in_shapefile="TAZ",
            new_zone_colname=f"{location}_tm2_taz"
        )

        # Geocode TM2 MAZ
        logger.debug(f"  Assigning TM2 MAZ for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "TM2_maz_taz_v2.2" / "maz_v2.2_WGS84.shp",
            zone_name_in_shapefile="MAZ",
            new_zone_colname=f"{location}_tm2_maz"
        )

        # Geocode Census tract
        logger.debug(f"  Assigning Census tract for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "Census" / "2020" / "tl_2020_06_tract" / "tl_2020_06_tract.shp",
            zone_name_in_shapefile="GEOID",
            new_zone_colname=f"{location}_tract_geoid"
        )

        # Geocode Census county
        logger.debug(f"  Assigning Census county for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "Census" / "2020" / "tl_2020_us_county" / "tl_2020_us_county.shp",
            zone_name_in_shapefile="GEOID",
            new_zone_colname=f"{location}_county_geoid"
        )

        # Geocode Census PUMA
        logger.debug(f"  Assigning Census PUMA for {location}")
        result_df = add_zone_to_lat_lon(
            df=result_df,
            lat_colname=lat_col,
            lon_colname=lon_col,
            shapefile_path=shapefiles_dir / "Census" / "2020" / "tl_2020_06_puma20" / "tl_2020_06_puma20.shp",
            zone_name_in_shapefile="GEOID20",
            new_zone_colname=f"{location}_puma_geoid"
        )

    logger.info("Geocoding complete")
    return result_df
