"""Helpers for adding zone information based on spatial joins.

Using GeoPandas for now but waiting for GeoPolars to mature.
"""

import geopandas as gpd
import polars as pl


def spatial_join_coordinates_to_shapefile(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    shapefile_path: str,
    shapefile_id_col: str,
    output_id_col: str | None = None,
    id_col: str = "id",
    source_crs: str = "EPSG:4326"
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
        Name of the output column for the joined ID (e.g., 'Origin_TAZ'), defaults to id_col if None
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
    subset = df.select([
        id_col,
        pl.col(lat_col).cast(pl.Float64, strict=False).alias(lat_col),
        pl.col(lon_col).cast(pl.Float64, strict=False).alias(lon_col)
    ]).drop_nulls(subset=[lat_col, lon_col])

    # Convert to GeoDataFrame
    subset_pandas = subset.to_pandas()
    points_gdf = gpd.GeoDataFrame(
        subset_pandas,
        geometry=gpd.points_from_xy(subset_pandas[lon_col], subset_pandas[lat_col]),
        crs=source_crs
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
    return result.select([id_col, output_id_col])
