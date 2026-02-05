"""Geocoding module for survey standardization.

Assigns geographic zones to all location types using spatial joins
and calculates Haversine distances between key location pairs.
"""

import math
from pathlib import Path

import polars as pl

from transit_passenger_tools.config.settings import get_config
from transit_passenger_tools.geocoding.zones import spatial_join_coordinates_to_shapefile
from transit_passenger_tools.schemas import FieldDependencies

# Field dependencies
FIELD_DEPENDENCIES = FieldDependencies(
    inputs=[
        "home_lat",
        "home_lon",
        "workplace_lat",
        "workplace_lon",
        "school_lat",
        "school_lon",
        "orig_lat",
        "orig_lon",
        "dest_lat",
        "dest_lon",
        "survey_board_lat",
        "survey_board_lon",
        "survey_alight_lat",
        "survey_alight_lon",
        "first_board_lat",
        "first_board_lon",
        "last_alight_lat",
        "last_alight_lon",
    ],
    outputs=[
        "home_tm1_taz",
        "workplace_tm1_taz",
        "school_tm1_taz",
        "orig_tm1_taz",
        "dest_tm1_taz",
        "survey_board_tm1_taz",
        "survey_alight_tm1_taz",
        "first_board_tm1_taz",
        "last_alight_tm1_taz",
        "home_tm2_taz",
        "workplace_tm2_taz",
        "school_tm2_taz",
        "orig_tm2_taz",
        "dest_tm2_taz",
        "survey_board_tm2_taz",
        "survey_alight_tm2_taz",
        "first_board_tm2_taz",
        "last_alight_tm2_taz",
        "home_tm2_maz",
        "workplace_tm2_maz",
        "school_tm2_maz",
        "orig_tm2_maz",
        "dest_tm2_maz",
        "survey_board_tm2_maz",
        "survey_alight_tm2_maz",
        "first_board_tm2_maz",
        "last_alight_tm2_maz",
        "home_tract_GEOID",
        "workplace_tract_GEOID",
        "school_tract_GEOID",
        "orig_tract_GEOID",
        "dest_tract_GEOID",
        "survey_board_tract_GEOID",
        "survey_alight_tract_GEOID",
        "first_board_tract_GEOID",
        "last_alight_tract_GEOID",
        "home_county_GEOID",
        "workplace_county_GEOID",
        "school_county_GEOID",
        "orig_county_GEOID",
        "dest_county_GEOID",
        "survey_board_county_GEOID",
        "survey_alight_county_GEOID",
        "first_board_county_GEOID",
        "last_alight_county_GEOID",
        "home_PUMA_GEOID",
        "workplace_PUMA_GEOID",
        "school_PUMA_GEOID",
        "orig_PUMA_GEOID",
        "dest_PUMA_GEOID",
        "survey_board_PUMA_GEOID",
        "survey_alight_PUMA_GEOID",
        "first_board_PUMA_GEOID",
        "last_alight_PUMA_GEOID",
        "distance_orig_dest",
        "distance_board_alight",
        "distance_orig_first_board",
        "distance_orig_survey_board",
        "distance_survey_alight_dest",
        "distance_last_alight_dest",
    ],
)

# Location types to geocode
LOCATION_TYPES = [
    "home",
    "work",
    "school",
    "orig",  # trip origin
    "dest",  # trip destination
    "survey_board",  # where passenger boarded surveyed vehicle
    "survey_alight",  # where passenger alighted surveyed vehicle
    "first_board",  # first boarding on full trip
    "last_alight",  # last alighting on full trip
]

# Earth radius in kilometers for Haversine calculation
EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate Haversine distance between two points in kilometers.

    Args:
        lat1: Latitude of first point
        lon1: Longitude of first point
        lat2: Latitude of second point
        lon2: Longitude of second point

    Args:
        lat1, lon1: First point coordinates in degrees
        lat2, lon2: Second point coordinates in degrees

    Returns:
        Distance in kilometers
    """
    if any(x is None for x in [lat1, lon1, lat2, lon2]):
        return None

    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_KM * c


def calculate_distances(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate Haversine distances between location pairs in kilometers.

    Distance pairs are loaded from pipeline configuration.

    Args:
        df: DataFrame with lat/lon columns for locations

    Returns:
        DataFrame with added distance columns (in kilometers)
    """
    config = get_config()
    result_df = df

    for distance_config in config.geocoding_distances:
        from_loc = distance_config.from_
        to_loc = distance_config.to
        dist_col = distance_config.column

        from_lat = f"{from_loc}_lat"
        from_lon = f"{from_loc}_lon"
        to_lat = f"{to_loc}_lat"
        to_lon = f"{to_loc}_lon"

        # Check if all columns exist
        required_cols = [from_lat, from_lon, to_lat, to_lon]
        if not all(col in result_df.columns for col in required_cols):
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Float64).alias(dist_col))
            continue

        # Calculate distance using Polars expressions
        result_df = result_df.with_columns(
            pl.struct([from_lat, from_lon, to_lat, to_lon])
            .map_elements(
                lambda row, fl=from_lat, flon=from_lon, tl=to_lat, tlon=to_lon: haversine_km(
                    row[fl], row[flon], row[tl], row[tlon]
                ),
                return_dtype=pl.Float64,
            )
            .alias(dist_col)
        )

    return result_df


def assign_zones_and_distances(
    df: pl.DataFrame, shapefiles_dir: Path | None = None
) -> pl.DataFrame:
    """Transform geocoding fields - assign zones and calculate distances.

    For each location type (home, work, school, etc.), assigns geographic zones
    (TM1 TAZ, TM2 TAZ/MAZ, Census county/tract/PUMA) based on configuration.

    Also calculates Haversine distances between key location pairs.

    Args:
        df: Input DataFrame with lat/lon columns for each location
        shapefiles_dir: Directory containing zone shapefiles (optional).
            If None, only distance calculations are performed.

    Returns:
        DataFrame with added geographic zone and distance columns
    """
    config = get_config()

    # Calculate distances first (doesn't require external files)
    result_df = calculate_distances(df)

    # If no shapefiles directory, skip spatial joins
    if shapefiles_dir is None:
        return result_df

    # Process each location type
    for location in LOCATION_TYPES:
        lat_col = f"{location}_lat"
        lon_col = f"{location}_lon"

        # Check if these columns exist
        if lat_col not in result_df.columns or lon_col not in result_df.columns:
            continue

        # Count non-null coordinates
        non_null_count = result_df.filter(
            pl.col(lat_col).is_not_null() & pl.col(lon_col).is_not_null()
        ).height

        if non_null_count == 0:
            continue

        # Geocode all zone types for this location
        for zone_config in config.geocoding_zones:
            shapefile_path = Path(config.shapefiles[zone_config.shapefile_key])

            result_df = spatial_join_coordinates_to_shapefile(
                df=result_df,
                lat_col=lat_col,
                lon_col=lon_col,
                shapefile_path=str(shapefile_path),
                shapefile_id_col=zone_config.field_name,
                output_id_col=f"{location}_{zone_config.zone_type}",
            )

    return result_df
