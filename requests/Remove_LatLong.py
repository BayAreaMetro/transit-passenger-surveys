"""Strip PII variables from Transit Passenger Survey Data.

A generalized version of https://github.com/BayAreaMetro/transit-passenger-surveys/blob/master/requests/2024_BART.py

"""


from pathlib import Path
import polars as pl
import geopandas as gpd
import os

# Set up input and output directories
SURVEY_PATH = r"E:/Box/Modeling and Surveys/Surveys/Transit Passenger Surveys/Ongoing TPS/Individual Operator Efforts/VTA 2024/ETC VTA MTC Shared Folder/Summary Data and Report/od_20250327_vta_ca_weighted_draftfinal.xlsx"
# vtaTAZ_PATH = r"M:/Data/Requests/Louisa Leung/Caltrain Survey Data/VTATAZ_CCAG/VTATAZ.shp"
# BG_PATH = r"M:/Data/Requests/Louisa Leung/tl_2025_06_bg.zip"
TRACT_PATH = r"M:/Data/Requests/Louisa Leung/tl_2025_06_tract.zip"
OUTPUT_DIR = r"E:/Box/Modeling and Surveys/Share Data/Protected Data/Kimley-Horn/SMCTD_Dumbarton_Busway"
OUTPUT_PREFIX = "VTA_2024"


# ============================================================================
# INPUT CONFIGURATION
# ============================================================================

SURVEY_SHEET_NAME = "OD_RESULTS_WEEKDAY"
CODEBOOK_SHEET_NAME = "data dictionary"

CODEBOOK_COLUMNS = [
    "FIELD NAME",
    "DESCRIPTION",
    "CODE VALUES"
]

# ============================================================================
# FINALCOLUMNS | Optional: Specify final columns to keep in output
# ============================================================================
FINAL_COLUMNS = []


# If it doesn't already exist, map the network drive
if not Path("M:/").exists():
    os.system(r'net use M: \\models.ad.mtc.ca.gov\data\models /persistent:no')

if not Path("M:/").exists():
    raise FileNotFoundError("Could not access M: drive. Please check network connection.")


# NOTE: Will become a reusable function in a shared utils module
def spatial_join_coordinates_to_shapefile(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    shapefile_gdf: gpd.GeoDataFrame,
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
    shapefile_gdf : gpd.GeoDataFrame
        GeoDataFrame of the shapefile
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
    shapefile_gdf = shapefile_gdf[[shapefile_id_col, "geometry"]]
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
    id_suffixes: list = ["TAZ"]
    ) -> list[tuple[str, str, str, str]]:
    """Parse latitude and longitude column pairs from dataframe columns.

    Parameters:
    -----------
    df : pl.DataFrame
        Input dataframe with coordinate columns

    Returns:
    --------
    list[tuple[str, str, str, str]]
        List of (latitude_column, longitude_column, output_column, id_suffix) tuples
    """
    lat_cols = [col for col in df.columns if latlon_suffixes[0] in col]
    lon_cols = [col for col in df.columns if latlon_suffixes[1] in col]

    lat_lon_pairs = []
    for lat_col in lat_cols:
        # Drop lat/lon suffix to find matching prefix, but keep origin case
        prefix = lat_col.replace(latlon_suffixes[0], "")
        for id_suffix in id_suffixes:
            output_col = lat_col.replace(latlon_suffixes[0], id_suffix)
            matching_col = next(
                (
                    lon for lon in lon_cols
                    if prefix == lon.replace(latlon_suffixes[1], "")
                ),
                None
            )
            if matching_col:
                lat_lon_pairs.append((lat_col, matching_col, output_col, id_suffix))

    return lat_lon_pairs


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main() -> None:
    """Main processing function for BART spatial aggregation."""
    # Read survey data
    print("Reading survey data...")
    survey = pl.read_excel(SURVEY_PATH, sheet_name=SURVEY_SHEET_NAME, infer_schema_length=15000)
    codebook = pl.read_excel(SURVEY_PATH, sheet_name=CODEBOOK_SHEET_NAME, has_header=False)
    
    # Prepare codebook
    # codebook.columns = ["Variable", "Description", "Value", "Value_Description"]
    codebook.columns = CODEBOOK_COLUMNS

    
    # Filter survey to the required columns
    if FINAL_COLUMNS:
        survey = survey.select(FINAL_COLUMNS)
    print(f"Survey data contains {survey.height} records and {survey.width} columns.")
    
    # Load shapefiles into GeoDataFrames
    print("Loading shapefiles...")
    geo_cache = {
        # "vtaTAZ": gpd.read_file(vtaTAZ_PATH).rename(columns={"TAZ": "vtaTAZ"}),
        # "BG": gpd.read_file(BG_PATH).rename(columns={"GEOID": "BG"}),
        "TRACT": gpd.read_file(TRACT_PATH).rename(columns={"GEOID": "TRACT"})
    }
    zones = list(geo_cache.keys())

    # Process each location type
    print("Processing spatial joins...")
    taz_configs = parse_latlons_from_columns(survey, ("LAT", "LONG"), zones)   
    
    # Process each location type and collect results
    _survey = survey.clone()
    for lat_col, lon_col, output_col, shp_id_col in taz_configs:
        print(f"  Processing {output_col}...")
        _survey = spatial_join_coordinates_to_shapefile(
            df=_survey,
            lat_col=lat_col,
            lon_col=lon_col,
            shapefile_gdf=geo_cache[shp_id_col],
            shapefile_id_col=shp_id_col,
            output_id_col=output_col,
            id_col="ID"
        )

    # Remove PII columns
    print("Removing PII columns...")   
    danger_parts = (
        "lat", "lon", "latitude", "longitude",
        "address_place", "address_addr", "address_searchkey",
        "_OLD"
    )

    # Scan through for columns to remove
    for col in _survey.columns:
        if any(suffix in col.lower() for suffix in danger_parts):
            print(f"Dropping PII column: {col}...")
            _survey = _survey.drop(col)

    # Final survey store    
    survey_final = _survey

    # Write output - Split by Zone aggregation type
    print("Writing output files...")   
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Save the full survey with all zone types and LAT/LON included
    full_output_file = Path(OUTPUT_DIR) / f"{OUTPUT_PREFIX}_latlon_and_zones.csv"
    print(f"Writing full output to {full_output_file}...")
    survey_final.write_csv(full_output_file)
    
    # Save separate files for each zone type
    for zone_type in zones:
        output_file = Path(OUTPUT_DIR) / f"{OUTPUT_PREFIX}_{zone_type}.csv"
        print(f"Writing output to {output_file}...")
        
        # Drop all other zone types
        other_zones = [z for z in zones if z != zone_type]
        drop_cols = [col for col in survey_final.columns if any(f"_{oz}" in col for oz in other_zones)]
        _survey_zone = survey_final.drop(drop_cols)
        _survey_zone.write_csv(output_file)

    codebook_output_file = Path(OUTPUT_DIR) / f"{OUTPUT_PREFIX}_Codebook.csv"
    print(f"Writing codebook to {codebook_output_file}...")
    codebook.write_csv(codebook_output_file)

    print("Processing complete!")


if __name__ == "__main__":
    main()
