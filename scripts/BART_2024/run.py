"""
Spatial Aggregation of BART 2024 Survey Data to VTA TAZs
"""

import polars as pl
from pathlib import Path

from transit_passenger_tools.add_zone import spatial_join_coordinates_to_shapefile



# Set up input and output directories
SURVEY_PATH = r"M:/Data/OnBoard/Data and Reports/BART/2024/2024 BART OD Data (sent 11.7.2024).xlsx"
SHP_PATH = r"M:/Data/Requests/Louisa Leung/BART Survey Data/VTATAZ_CCAG/VTATAZ.shp"
OUTPUT_DIR = r"M:/Data/Requests/Louisa Leung/BART Survey Data/"
    

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main processing function for BART spatial aggregation."""
    

    # Read survey data
    print("Reading survey data...")
    survey = pl.read_excel(SURVEY_PATH, sheet_name="Data")
    
    # Process each location type
    print("Processing spatial joins...")
    
    location_configs = [
        ('origin_lat', 'origin_lon', 'Origin_TAZ'),
        ('destination_lat', 'destination_lon', 'Destination_TAZ'),
        ('home_address_lat', 'home_address_lon', 'Home_TAZ'),
        ('work_address_lat', 'work_address_lon', 'Work_TAZ'),
        ('school_address_lat', 'school_address_lon', 'School_TAZ')
    ]
    
    # Process each location type and collect results
    taz_assignments = {}
    for lat_col, lon_col, output_col in location_configs:
        print(f"  Processing {output_col}...")
        result = spatial_join_coordinates_to_shapefile(
            df=survey,
            lat_col=lat_col,
            lon_col=lon_col,
            shapefile_path=SHP_PATH,
            shapefile_id_col='TAZ',
            output_id_col=output_col,
            id_col='id'
        )
        taz_assignments[output_col] = result
    
    # Join all TAZ assignments back to original dataframe
    print("Merging results...")
    survey_final = survey.clone()
    
    for output_col, taz_df in taz_assignments.items():
        survey_final = survey_final.join(taz_df, on='id', how='left')
    
    # Remove PII geography columns
    pii_columns = [
        'origin_lat', 'origin_lon', 'destination_lat', 'destination_lon',
        'home_address_lat', 'home_address_lon', 'work_address_lat', 
        'work_address_lon', 'school_address_lat', 'school_address_lon'
    ]
    
    # Also remove any other columns containing "address"
    address_cols = [col for col in survey_final.columns if 'address' in col.lower()]
    columns_to_drop = [col for col in list(set(pii_columns + address_cols)) if col in survey_final.columns]
    
    if columns_to_drop:
        survey_final = survey_final.drop(columns_to_drop)
    
    # Write output
    output_file = Path(OUTPUT_DIR) / "BART_2024_Aggregated.csv"
    print(f"Writing output to {output_file}...")
    survey_final.write_csv(output_file)
    
    print("Processing complete!")


if __name__ == "__main__":
    main()
