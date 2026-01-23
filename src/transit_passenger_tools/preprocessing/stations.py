"""Utilities for geocoding transit stations using codebook and stop data."""
import logging

import geopandas as gpd
import polars as pl
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

# Common station name aliases for fuzzy matching
COMMON_STATION_ALIASES = {
    "SFO": "San Francisco International Airport",
    "OAK": "Oakland International Airport",
    "Millbrae": "Millbrae (Caltrain Transfer Platform)"
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
    mask = stops_gdf[agency_field].str.contains(
        search_pattern, case=False, na=False, regex=True
    )
    operator_stops = stops_gdf[mask].copy()

    if len(operator_stops) == 0:
        msg = f"No stops found for operator(s): {operator_names}"
        raise ValueError(msg)

    logger.info(
        "Found %s stops for operator(s): %s",
        len(operator_stops),
        ", ".join(operator_names)
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
        unique_stations = (
            survey_df.select(pl.col(station_col))
            .unique()
            .drop_nulls()
        )
        
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
                        survey_name, canonical, best_score
                    )
                else:
                    logger.warning(
                        "No match for '%s' (best: %s at %.1f%%)",
                        survey_name, best_match, best_score
                    )
                    unmatched_stations.append(survey_name)
                    continue

            name_to_canonical[survey_name_clean] = canonical
            name_to_lat[survey_name_clean] = lat
            name_to_lon[survey_name_clean] = lon

        match_rate = (
            len(name_to_canonical) / unique_stations.height * 100
            if unique_stations.height > 0 else 0
        )
        logger.info(
            "  Matched %s/%s unique stations (%.1f%%)",
            len(name_to_canonical), unique_stations.height, match_rate
        )

        if unmatched_stations:
            all_unmatched.extend(
                [f"{station_col}: {name}" for name in unmatched_stations]
            )

        # Add columns to dataframe
        survey_df = survey_df.with_columns([
            pl.col(station_col).replace_strict(
                name_to_canonical, default=None, return_dtype=pl.Utf8
            ).alias(output_cols["station"]),
            pl.col(station_col).replace_strict(
                name_to_lat, default=None, return_dtype=pl.Float64
            ).alias(output_cols["lat"]),
            pl.col(station_col).replace_strict(
                name_to_lon, default=None, return_dtype=pl.Float64
            ).alias(output_cols["lon"]),
            pl.when(pl.col(station_col).is_not_null())
            .then(pl.lit("station"))
            .otherwise(None)
            .alias(output_cols["geo_level"])
        ])

    # Raise error if any stations were unmatched
    if all_unmatched:
        msg = f"Unmatched stations ({len(all_unmatched)}): {', '.join(all_unmatched)}"
        raise ValueError(msg)

    return survey_df
