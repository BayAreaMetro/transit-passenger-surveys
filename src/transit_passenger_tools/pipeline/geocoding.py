"""Geocoding module for survey standardization.

Assigns geographic zones to all location types using spatial joins,
calculates Haversine distances between key location pairs,
fuzzy-matches station names to GTFS stops, and provides reference
data loading (canonical route crosswalk + shapefiles).
"""

import logging
import math
from pathlib import Path
from typing import ClassVar, Optional

import geopandas as gpd
import polars as pl
from rapidfuzz import fuzz

from transit_passenger_tools.config import get_config
from transit_passenger_tools.models import FieldDependencies

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


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float | None:
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
    df: pl.DataFrame,
    shapefiles_dir: Path | None = None,
    id_col: str = "response_id",
) -> pl.DataFrame:
    """Transform geocoding fields - assign zones and calculate distances.

    For each location type (home, work, school, etc.), assigns geographic zones
    (TM1 TAZ, TM2 TAZ/MAZ, Census county/tract/PUMA) based on configuration.

    Also calculates Haversine distances between key location pairs.

    Args:
        df: Input DataFrame with lat/lon columns for each location
        shapefiles_dir: Directory containing zone shapefiles (optional).
            If None, only distance calculations are performed.
        id_col: Name of the unique row identifier column used for spatial
            join lookups. Defaults to ``"response_id"``.

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
            shapefile_path = config.resolve_shapefile_path(zone_config.shapefile_key)

            result_df = spatial_join_coordinates_to_shapefile(
                df=result_df,
                lat_col=lat_col,
                lon_col=lon_col,
                shapefile_path=str(shapefile_path),
                shapefile_id_col=zone_config.field_name,
                output_id_col=f"{location}_{zone_config.zone_type}",
                id_col=id_col,
            )

    return result_df


# ========== Spatial join helpers (formerly geocoding/zones.py) ==========

logger = logging.getLogger(__name__)


def spatial_join_coordinates_to_shapefile(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    shapefile_path: str,
    shapefile_id_col: str,
    output_id_col: str | None = None,
    id_col: str = "id",
    source_crs: str = "EPSG:4326",
) -> pl.DataFrame:
    """Perform spatial join between dataframe with lat/lon coordinates and a shapefile.

    Parameters:
    -----------
    df : pl.DataFrame
        Input dataframe with coordinate columns
    lat_col : str
        Name of the latitude column in the input dataframe
    lon_col : str
        Name of the longitude column in the input dataframe
    shapefile_path : str
        Path to the shapefile
    shapefile_id_col : str
        Name of the ID column in the shapefile (e.g., 'TAZ')
    output_id_col : str | None
        Name of the output column for the joined ID (e.g., 'Origin_TAZ'),
        defaults to id_col if None
    id_col : str
        Name of the ID column in the input dataframe (default: 'id')
    source_crs : str
        Source coordinate reference system (default: 'EPSG:4326' for WGS84)

    Returns:
    --------
    pl.DataFrame
        DataFrame with id_col and output_id_col columns
    """
    # Read shapefile and get target CRS
    shapefile_gdf = gpd.read_file(shapefile_path)[[shapefile_id_col, "geometry"]]
    target_crs = shapefile_gdf.crs

    if output_id_col is None:
        output_id_col = id_col

    # Convert coordinate columns to numeric and filter out nulls
    subset = df.select(
        [
            id_col,
            pl.col(lat_col).cast(pl.Float64, strict=False).alias(lat_col),
            pl.col(lon_col).cast(pl.Float64, strict=False).alias(lon_col),
        ]
    ).drop_nulls(subset=[lat_col, lon_col])

    # Convert to GeoDataFrame
    subset_pandas = subset.to_pandas()
    points_gdf = gpd.GeoDataFrame(
        subset_pandas,
        geometry=gpd.points_from_xy(subset_pandas[lon_col], subset_pandas[lat_col]),
        crs=source_crs,
    )

    # Reproject to match shapefile CRS
    if target_crs:
        points_gdf = points_gdf.to_crs(target_crs)

    # Perform spatial join
    joined = gpd.sjoin(points_gdf, shapefile_gdf, how="left", predicate="within")

    # Clean up result
    result_pandas = joined.drop(columns="geometry")

    # Handle the case where the shapefile ID column might have been renamed by sjoin
    if shapefile_id_col in result_pandas.columns:
        result_pandas = result_pandas.rename(columns={shapefile_id_col: output_id_col})
    elif f"{shapefile_id_col}_right" in result_pandas.columns:
        result_pandas = result_pandas.rename(columns={f"{shapefile_id_col}_right": output_id_col})

    # Drop index_right if it exists
    if "index_right" in result_pandas.columns:
        result_pandas = result_pandas.drop(columns="index_right")

    # Convert back to polars and select only ID columns
    result = pl.from_pandas(result_pandas)
    result_slim = result.select([id_col, output_id_col])

    # Join back to original dataframe to preserve all rows data type
    return df.join(result_slim, on=id_col, how="left")


def parse_latlons_from_columns(
    df: pl.DataFrame,
    latlon_suffixes: tuple[str, str] = ("lat", "lon"),
    id_suffix: str = "TAZ",
) -> list[tuple[str, str, str]]:
    """Parse latitude and longitude column pairs from dataframe columns.

    Parameters:
    -----------
    df : pl.DataFrame
        Input dataframe with coordinate columns

    Returns:
    --------
    list[tuple[str, str, str]]
        List of (latitude_column, longitude_column, output_column) tuples
    """
    lat_cols = [col for col in df.columns if latlon_suffixes[0] in col]
    lon_cols = [col for col in df.columns if latlon_suffixes[1] in col]

    lat_lon_pairs = []
    for lat_col in lat_cols:
        # Drop lat/lon suffix to find matching prefix, but keep origin case
        prefix = lat_col.replace(latlon_suffixes[0], "")
        output_col = lat_col.replace(latlon_suffixes[0], id_suffix)
        matching_col = next(
            (lon for lon in lon_cols if prefix == lon.replace(latlon_suffixes[1], "")), None
        )
        if matching_col:
            lat_lon_pairs.append((lat_col, matching_col, output_col))

    return lat_lon_pairs


# ========== Station geocoding (formerly geocoding/stations.py) ==========

# Common station name aliases for fuzzy matching
COMMON_STATION_ALIASES = {
    "SFO": "San Francisco International Airport",
    "OAK": "Oakland International Airport",
    "Millbrae": "Millbrae (Caltrain Transfer Platform)",
}


def geocode_stops_from_names(  # noqa: PLR0912, PLR0915, C901
    survey_df: pl.DataFrame,
    stops_gdf: gpd.GeoDataFrame,
    station_columns: dict[str, dict[str, str]],
    operator_names: list[str],
    stop_name_field: str,
    agency_field: str,
    fuzzy_threshold: int = 90,
    station_aliases: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Geocode decoded station names to coordinates using fuzzy matching.

    Args:
        survey_df: Survey DataFrame with decoded station name columns
        stops_gdf: GeoDataFrame with stop locations (must have geometry)
        station_columns: Maps input column -> output column names dict
            e.g., {
                "entry_station_name": {
                    "station": "survey_board_station",
                    "lat": "survey_board_lat",
                    "lon": "survey_board_lon",
                    "geo_level": "survey_board_geo_level"
                }
            }
        operator_names: List of operator names to filter stops by
            (e.g., ["BART", "Bay Area Rapid Transit"])
        stop_name_field: Column name in stops_gdf containing stop names
        agency_field: Column name in stops_gdf containing agency/operator names
        fuzzy_threshold: Minimum fuzzy match score (0-100)
        station_aliases: Optional dict of station name aliases for matching

    Returns:
        Survey DataFrame with added columns as specified in station_columns

    Raises:
        ValueError: If no stops found for operator, or if unmatched stations exist
    """
    if station_aliases is None:
        station_aliases = COMMON_STATION_ALIASES.copy()
    else:
        # Merge with common aliases
        station_aliases = {**COMMON_STATION_ALIASES, **station_aliases}

    # Filter to operator's stops
    search_pattern = "|".join(operator_names)
    mask = stops_gdf[agency_field].str.contains(search_pattern, case=False, na=False, regex=True)
    operator_stops = stops_gdf[mask].copy()

    if len(operator_stops) == 0:
        msg = f"No stops found for operator(s): {operator_names}"
        raise ValueError(msg)

    logger.info(
        "Found %s stops for operator(s): %s", len(operator_stops), ", ".join(operator_names)
    )

    # Convert to WGS84 and extract coordinates
    operator_stops = operator_stops.to_crs(epsg=4326)
    operator_stops["_stop_lon"] = operator_stops.geometry.x
    operator_stops["_stop_lat"] = operator_stops.geometry.y

    # Validate required fields
    if stop_name_field not in operator_stops.columns:
        msg = f"stop_name_field '{stop_name_field}' not found in stops GeoDataFrame"
        raise ValueError(msg)

    logger.info("Using stop name field: %s", stop_name_field)

    # Process each station column
    all_unmatched = []

    for station_col, output_cols in station_columns.items():
        logger.info("Processing station column: %s", station_col)

        # Get unique station names from survey
        unique_stations = survey_df.select(pl.col(station_col)).unique().drop_nulls()

        # Build lookup from survey name -> canonical name/coords
        name_to_canonical = {}
        name_to_lat = {}
        name_to_lon = {}
        unmatched_stations = []

        for row in unique_stations.iter_rows(named=True):
            survey_name = row[station_col]
            if survey_name is None:
                continue

            # Check aliases first
            survey_name_clean = str(survey_name).strip()
            if survey_name_clean in station_aliases:
                survey_name = station_aliases[survey_name_clean]
                logger.debug("Using alias: %s -> %s", survey_name_clean, survey_name)

            # Try exact match (case-insensitive)
            survey_name_upper = survey_name.upper()
            exact_match = operator_stops[
                operator_stops[stop_name_field].str.upper() == survey_name_upper
            ]

            if len(exact_match) > 0:
                canonical = exact_match.iloc[0][stop_name_field]
                lat = exact_match.iloc[0]["_stop_lat"]
                lon = exact_match.iloc[0]["_stop_lon"]
            else:
                # Fuzzy match
                best_match = None
                best_score = 0
                best_lat = None
                best_lon = None
                for _, stop_row in operator_stops.iterrows():
                    canonical_name = stop_row[stop_name_field]
                    score = fuzz.ratio(survey_name_upper, canonical_name.upper())
                    if score > best_score:
                        best_score = score
                        best_match = canonical_name
                        best_lat = stop_row["_stop_lat"]
                        best_lon = stop_row["_stop_lon"]

                if best_score >= fuzzy_threshold:
                    canonical = best_match
                    lat = best_lat
                    lon = best_lon
                    logger.debug(
                        "Fuzzy matched '%s' -> '%s' (%.1f%%)",
                        survey_name,
                        canonical,
                        best_score,
                    )
                else:
                    logger.warning(
                        "No match for '%s' (best: %s at %.1f%%)",
                        survey_name,
                        best_match,
                        best_score,
                    )
                    unmatched_stations.append(survey_name)
                    continue

            name_to_canonical[survey_name_clean] = canonical
            name_to_lat[survey_name_clean] = lat
            name_to_lon[survey_name_clean] = lon

        match_rate = (
            len(name_to_canonical) / unique_stations.height * 100
            if unique_stations.height > 0
            else 0
        )
        logger.info(
            "  Matched %s/%s unique stations (%.1f%%)",
            len(name_to_canonical),
            unique_stations.height,
            match_rate,
        )

        if unmatched_stations:
            all_unmatched.extend([f"{station_col}: {name}" for name in unmatched_stations])

        # Add columns to dataframe
        survey_df = survey_df.with_columns(
            [
                pl.col(station_col)
                .replace_strict(name_to_canonical, default=None, return_dtype=pl.Utf8)
                .alias(output_cols["station"]),
                pl.col(station_col)
                .replace_strict(name_to_lat, default=None, return_dtype=pl.Float64)
                .alias(output_cols["lat"]),
                pl.col(station_col)
                .replace_strict(name_to_lon, default=None, return_dtype=pl.Float64)
                .alias(output_cols["lon"]),
                pl.when(pl.col(station_col).is_not_null())
                .then(pl.lit("station"))
                .otherwise(None)
                .alias(output_cols["geo_level"]),
            ]
        )

    # Raise error if any stations were unmatched
    if all_unmatched:
        msg = f"Unmatched stations ({len(all_unmatched)}): {', '.join(all_unmatched)}"
        raise ValueError(msg)

    return survey_df


# ========== Nearest-stop snapping ==========


def snap_to_nearest_stop(
    df: pl.DataFrame,
    stops_gdf: gpd.GeoDataFrame,
    lat_col: str,
    lon_col: str,
    stop_name_col: str,
    stop_name_field: str = "stop_name",
    max_distance_m: float = 250.0,
    agency_filter: list[str] | None = None,
    agency_field: str = "agency",
) -> pl.Series:
    """Find the nearest GTFS stop for each row using spatial indexing.

    For rows that already have a non-null value in *stop_name_col* or that
    lack lat/lon coordinates, the original value is preserved.  Only null
    stop-name rows with valid coordinates are candidates for snapping.

    Args:
        df: Survey DataFrame.
        stops_gdf: GeoDataFrame of transit stops (must have geometry + stop
            name column).  Loaded once and shared across calls.
        lat_col: Column with boarding/alighting latitude.
        lon_col: Column with boarding/alighting longitude.
        stop_name_col: Column whose nulls we want to fill (e.g.
            ``"board_stop_name"``).
        stop_name_field: Name of the stop-name column in *stops_gdf*.
        max_distance_m: Maximum snap distance in metres.  Rows whose nearest
            stop exceeds this threshold are left null.
        agency_filter: Optional list of agency names (case-insensitive
            substring match) to restrict candidate stops.
        agency_field: Column in *stops_gdf* that holds the agency name.

    Returns:
        A ``pl.Series`` of the same length as *df* containing the
        (possibly back-filled) stop name strings.
    """
    # --- Filter stops to agency if requested ---
    if agency_filter:
        pattern = "|".join(agency_filter)
        mask = stops_gdf[agency_field].str.contains(pattern, case=False, na=False, regex=True)
        filtered = stops_gdf[mask].copy()
    else:
        filtered = stops_gdf.copy()

    if len(filtered) == 0:
        logger.warning("snap_to_nearest_stop: no stops matched agency filter %s", agency_filter)
        return df[stop_name_col]

    # Ensure WGS 84
    filtered = filtered.to_crs(epsg=4326)

    # --- Identify candidate rows (null stop name + valid coords) ---
    candidates_mask = (
        df[stop_name_col].is_null()
        & df[lat_col].is_not_null()
        & df[lon_col].is_not_null()
    )
    candidate_indices = candidates_mask.arg_true().to_list()

    if not candidate_indices:
        logger.info(
            "snap_to_nearest_stop: no null %s rows with coordinates — nothing to do",
            stop_name_col
            )
        return df[stop_name_col]

    logger.info(
        "snap_to_nearest_stop: %s candidates for %s (max %s m, %s stops)",
        len(candidate_indices),
        stop_name_col,
        max_distance_m,
        len(filtered),
    )

    # --- Build GeoDataFrame for candidates ---
    lats = df[lat_col].gather(candidate_indices).to_numpy()
    lons = df[lon_col].gather(candidate_indices).to_numpy()
    candidate_gdf = gpd.GeoDataFrame(
        {"snap_idx": candidate_indices},
        geometry=gpd.points_from_xy(lons, lats, crs="EPSG:4326"),
    )

    # --- Project to a metre-based CRS for distance calculation ---
    # UTM 10N covers the entire Bay Area
    CRS_METRES = "EPSG:32610"  # noqa: N806
    candidate_gdf = candidate_gdf.to_crs(CRS_METRES)
    filtered = filtered.to_crs(CRS_METRES)

    # --- sjoin_nearest ---
    joined = gpd.sjoin_nearest(
        candidate_gdf,
        filtered[[stop_name_field, "geometry"]],
        how="left",
        max_distance=max_distance_m,
        distance_col="dist_m",
    )
    # sjoin_nearest can produce duplicates when equidistant; keep closest
    joined = joined.sort_values("dist_m").drop_duplicates(subset="snap_idx", keep="first")

    # --- Build result Series ---
    original = df[stop_name_col].to_list()
    matched = 0
    for _, row in joined.iterrows():
        idx = row["snap_idx"]
        name = row.get(stop_name_field)
        dist = row.get("dist_m")
        if name is not None and dist is not None and dist <= max_distance_m:
            original[idx] = name
            matched += 1

    logger.info(
        "snap_to_nearest_stop: matched %s / %s candidates within %s m",
        matched,
        len(candidate_indices),
        max_distance_m,
    )

    return pl.Series(stop_name_col, original, dtype=pl.Utf8)


# ========== Reference data (formerly geocoding/reference.py) ==========


class ReferenceData:
    """Singleton class for loading and caching reference data.

    Provides lazy-loaded access to:
    - Canonical route crosswalk (route names -> technology/operator)
    - Shapefiles for geocoding (TAZ, MAZ, counties, tracts, PUMA, stations)
    """

    _instance: Optional["ReferenceData"] = None
    _canonical_routes: pl.DataFrame | None = None
    _shapefiles: ClassVar[dict[str, gpd.GeoDataFrame]] = {}

    def __new__(cls) -> "ReferenceData":
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def canonical_routes(self) -> pl.DataFrame:
        """Get canonical route crosswalk as Polars DataFrame.

        Loads from fixtures/canonical_route_crosswalk.csv on first access.
        Contains mappings from survey_route_name to:
        - canonical_route
        - canonical_operator
        - operator_detail
        - technology
        - technology_detail

        Returns:
            Polars DataFrame with ~10,137 rows of route mappings.
        """
        if self._canonical_routes is None:
            config = get_config()
            self._canonical_routes = pl.read_csv(
                config.canonical_route_crosswalk_path,
                encoding="utf8-lossy",
            )
        return self._canonical_routes

    def get_route_technology(self, operator: str, route: str) -> dict[str, str]:
        """Get technology and operator details for a route.

        Args:
            operator: Canonical operator name (e.g., "BART", "AC TRANSIT")
            route: Route name from survey

        Returns:
            Dictionary with keys: canonical_operator, technology, operator_detail
            Technology value is normalized to match TechnologyType enum values

        Raises:
            ValueError: If route not found in canonical crosswalk
        """
        result = self.canonical_routes.filter(
            (pl.col("canonical_operator") == operator) & (pl.col("survey_route_name") == route)
        )

        if result.height == 0:
            msg = (
                f"Route not found in canonical crosswalk: "
                f"operator='{operator}', route='{route}'"
            )
            raise ValueError(msg)

        row = result.row(0, named=True)
        technology_raw = row["technology"]

        # Normalize technology value to match TechnologyType enum (title case)
        technology_normalized = technology_raw.title() if technology_raw else technology_raw

        return {
            "canonical_operator": row["canonical_operator"],
            "technology": technology_normalized,
            "operator_detail": row.get("operator_detail", row["canonical_operator"]),
        }

    def get_zone_shapefile(self, zone_type: str) -> gpd.GeoDataFrame:
        """Get shapefile for a specific zone type.

        Loads and caches shapefiles on first access. Shapefiles are used for
        spatial joins to assign geographic zones to survey locations.

        Args:
            zone_type: Type of zone to load. One of:
                - 'tm1_taz': Travel Model 1 TAZ zones
                - 'tm2_taz': Travel Model 2 TAZ zones
                - 'tm2_maz': Travel Model 2 MAZ zones
                - 'counties': County boundaries
                - 'tracts': Census tracts
                - 'puma': PUMA boundaries
                - 'stations': Rail station points

        Returns:
            GeoDataFrame with zone geometry and attributes

        Raises:
            KeyError: If zone_type not recognized
            FileNotFoundError: If shapefile path doesn't exist
        """
        if zone_type not in self._shapefiles:
            config = get_config()
            shapefile_path = config.shapefiles[zone_type]

            # Load shapefile
            gdf = gpd.read_file(shapefile_path)

            # Transform to NAD83 California Zone 6 Feet for consistent CRS
            if gdf.crs is None or gdf.crs.to_epsg() != config.crs["nad83_ca_zone6"]:
                gdf = gdf.to_crs(epsg=config.crs["nad83_ca_zone6"])

            self._shapefiles[zone_type] = gdf

        return self._shapefiles[zone_type]

    @property
    def shapefiles(self) -> dict[str, gpd.GeoDataFrame]:
        """Get all loaded shapefiles as dictionary.

        Returns:
            Dictionary mapping zone_type -> GeoDataFrame
        """
        return self._shapefiles


def get_reference_data() -> ReferenceData:
    """Get singleton ReferenceData instance.

    Returns:
        Shared ReferenceData instance
    """
    return ReferenceData()
