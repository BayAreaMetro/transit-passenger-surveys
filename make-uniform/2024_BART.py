"""Spatial Aggregation of BART 2024 Survey Data to VTA TAZs."""

from pathlib import Path
import polars as pl
import geopandas as gpd

# Set up input and output directories
SURVEY_PATH = r"M:/Data/OnBoard/Data and Reports/BART/2024_StationProfileV1_NewWeights_ReducedVariables.xlsx"
SHP_PATH = r"M:/Data/Requests/Louisa Leung/Caltrain Survey Data/VTATAZ_CCAG/VTATAZ.shp"
BG_PATH = r"M:/Data/Requests/Louisa Leung/tl_2025_06_bg/tl_2025_06_bg.shp"
OUTPUT_DIR = r"M:/Data/Requests/Louisa Leung/BART Survey Data/"

# NOTE: Will become a reusable function in a shared utils module
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
    result_slim = result.select([id_col, output_id_col])
    
    # Join back to original dataframe to preserve all rows data type
    final_result = df.join(result_slim, on=id_col, how="left")
    
    return final_result


def parse_latlons_from_columns(
    df: pl.DataFrame,
    latlon_suffixes: tuple[str, str] = ("lat", "lon"),
    id_suffix: str = "TAZ"
    ) -> list[tuple[str, str, str]]:
    """Parse latitude and longitude column pairs from dataframe columns.

    Parameters:
    -----------
    df : pl.DataFrame
        Input dataframe with coordinate columns

    Returns:
    --------
    list[tuple[str, str, str]]
        List of (latitude_column, longitude_column) tuples
    """
    lat_cols = [col for col in df.columns if latlon_suffixes[0] in col]
    lon_cols = [col for col in df.columns if latlon_suffixes[1] in col]

    lat_lon_pairs = []
    for lat_col in lat_cols:
        # Drop lat/lon suffix to find matching prefix, but keep origin case
        prefix = lat_col.replace(latlon_suffixes[0], "")
        output_col = lat_col.replace(latlon_suffixes[0], id_suffix)
        matching_col = next(
            (
                lon for lon in lon_cols
                if prefix == lon.replace(latlon_suffixes[1], "")
            ),
            None
        )
        if matching_col:
            lat_lon_pairs.append((lat_col, matching_col, output_col))

    return lat_lon_pairs


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main() -> None:
    """Main processing function for BART spatial aggregation."""
    # Read survey data
    print("Reading survey data...")
    survey = pl.read_excel(SURVEY_PATH, sheet_name="data")
    codebook = pl.read_excel(SURVEY_PATH, sheet_name="codebook", has_header=False)

    codebook.columns = ["Variable", "Description", "Value", "Value_Description"]

    # Process each location type
    print("Processing spatial joins...")
    taz_configs = parse_latlons_from_columns(survey, ("LAT", "LONG"), "TAZ")
    
    # Process each location type and collect results
    for lat_col, lon_col, output_col in taz_configs:
        print(f"  Processing {output_col}...")
        survey_final = spatial_join_coordinates_to_shapefile(
            df=survey,
            lat_col=lat_col,
            lon_col=lon_col,
            shapefile_path=SHP_PATH,
            shapefile_id_col="TAZ",
            output_id_col=output_col,
            id_col="UNIQUE_IDENTIFIER"
        )
        # Also do block groups
        survey_final = spatial_join_coordinates_to_shapefile(
            df=survey_final,
            lat_col=lat_col,
            lon_col=lon_col,
            shapefile_path=BG_PATH,
            shapefile_id_col="GEOID",
            output_id_col=output_col.replace("TAZ", "BG"),
            id_col="UNIQUE_IDENTIFIER"
        )
        # Drop the LAT/LONG columns to sanitize
        survey_final = survey_final.drop([lat_col, lon_col])

    # Remove PII columns
    pii_columns = []

    # Also remove any other columns containing "address"
    address_cols = [col for col in survey_final.columns if "address_addr" in col.lower()]
    columns_to_drop = [
        col for col in list(set(pii_columns + address_cols))
        if col in survey_final.columns
    ]

    if columns_to_drop:
        survey_final = survey_final.drop(columns_to_drop)

    # Write output
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    output_file = Path(OUTPUT_DIR) / "BART_2024_Aggregated.csv"
    print(f"Writing output to {output_file}...")
    survey_final.write_csv(output_file)

    codebook_output_file = Path(OUTPUT_DIR) / "BART_2024_Codebook.csv"
    print(f"Writing codebook to {codebook_output_file}...")
    codebook.write_csv(codebook_output_file)

    print("Processing complete!")


if __name__ == "__main__":
    main()
