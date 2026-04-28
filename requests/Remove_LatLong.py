"""
Strip PII variables from Transit Passenger Survey data.
"""


from pathlib import Path
import polars as pl
import geopandas as gpd
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

# ----------------------------------------------------------------------------
# Select survey to process
# ----------------------------------------------------------------------------
SURVEY_NAME = "Caltrain_2024"
# SURVEY_NAME = "VTA_2024"

# ----------------------------------------------------------------------------
# Survey-specific file paths
# ----------------------------------------------------------------------------
SURVEY_CONFIG = {
    "Caltrain_2024": {
        "survey_path": (
            r"E:/Box/Modeling and Surveys/Surveys/Transit Passenger Surveys/"
            r"Ongoing TPS/Individual Operator Efforts/Caltrain 2024/"
            r"Caltrain MTC RSG Project Folder/OD Data and Deliverables "
            r"(Includes Data and Report)/2024 Caltrain OD Data (sent 11.7.2024).xlsx"
        ),
        "output_prefix": "Caltrain_2024",
        "survey_sheet_name": "Data",
        "codebook_sheet_name": "Data Dictionary",
        "codebook_columns": [
            "Position",
            "Variable Type",
            "Variable",
            "Variable Label",
            "Value Number",
            "Value Label",
        ],
    },

    "VTA_2024": {
        "survey_path": (
            r"E:/Box/Modeling and Surveys/Surveys/Transit Passenger Surveys/"
            r"Ongoing TPS/Individual Operator Efforts/VTA 2024/"
            r"ETC VTA MTC Shared Folder/Summary Data and Report/"
            r"od_20250327_vta_ca_weighted_draftfinal.xlsx"
        ),
        "output_prefix": "VTA_2024",
        "survey_sheet_name": "OD_RESULTS_WEEKDAY",
        "codebook_sheet_name": "data dictionary",
        "codebook_columns": [
            "FIELD NAME",
            "DESCRIPTION",
            "CODE VALUES",
        ],
    },
}

# ----------------------------------------------------------------------------
# Additional survey config
# ----------------------------------------------------------------------------
if SURVEY_NAME not in SURVEY_CONFIG:
    raise ValueError(f"Unknown SURVEY_NAME: {SURVEY_NAME}")

SURVEY_PATH = SURVEY_CONFIG[SURVEY_NAME]["survey_path"]
OUTPUT_PREFIX = SURVEY_CONFIG[SURVEY_NAME]["output_prefix"]
SURVEY_SHEET_NAME = SURVEY_CONFIG[SURVEY_NAME]["survey_sheet_name"]
CODEBOOK_SHEET_NAME = SURVEY_CONFIG[SURVEY_NAME]["codebook_sheet_name"]
CODEBOOK_COLUMNS = SURVEY_CONFIG[SURVEY_NAME]["codebook_columns"]

# ----------------------------------------------------------------------------
# Spatial inputs
# ----------------------------------------------------------------------------
TRACT_PATH = r"M:/Data/Requests/Louisa Leung/tl_2025_06_tract.zip"

# VTATAZ_PATH = r"M:/Data/Requests/Louisa Leung/Caltrain Survey Data/VTATAZ_CCAG/VTATAZ.shp"
# BG_PATH = r"M:/Data/Requests/Louisa Leung/tl_2025_06_bg.zip"

# ----------------------------------------------------------------------------
# Output settings
# ----------------------------------------------------------------------------
OUTPUT_DIR = (
     r"E:/Box/Modeling and Surveys/Share Data/Protected Data/"
     r"Kimley-Horn/SMCTD_Dumbarton_Busway"
 )


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
    # Make sure ID column is not case sensitive
    actual_id_col = next(
        (c for c in df.columns if c.lower() == id_col.lower()),
        None
    )

    if actual_id_col is None:
        raise ValueError(f"Could not find ID column matching '{id_col}'")

    id_col = actual_id_col

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
    lat_suffix = latlon_suffixes[0].lower()
    lon_suffix = latlon_suffixes[1].lower()

    lat_cols = [col for col in df.columns if lat_suffix in col.lower()]
    lon_cols = [col for col in df.columns if lon_suffix in col.lower()]

    lat_lon_pairs = []
    for lat_col in lat_cols:
        # Drop lat/lon suffix to find matching prefix, but keep origin case
        prefix = lat_col.lower().replace(lat_suffix, "")
        for id_suffix in id_suffixes:
            idx = lat_col.lower().find(lat_suffix)
            output_col = lat_col[:idx] + id_suffix + lat_col[idx + len(lat_suffix):]
            matching_col = next(
                (
                    lon for lon in lon_cols
                    if prefix == lon.lower().replace(lon_suffix, "")
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
    taz_configs = parse_latlons_from_columns(survey, ("_LAT", "_LON"), zones)   

    print(f"Found {len(taz_configs)} lat/lon pairs:")
    for config in taz_configs:
        print(" ", config)
    
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

    # Collect all zone output column names to protect
    zone_output_cols = {output_col for _, _, output_col, _ in taz_configs}
    

    # Remove PII columns
    print("Removing PII columns...")   
    danger_parts = (
        "lat", "lon", "latitude", "longitude",
        "address",
        "_OLD"
    )

    # Scan through for columns to remove    
    for col in _survey.columns:
        if col in zone_output_cols:
            continue
        if any(part in col.lower() for part in danger_parts):
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
