"""Spatial Aggregation of BART 2024 Survey Data to VTA TAZs."""

from pathlib import Path

import polars as pl

from transit_passenger_tools.add_zone import (
    parse_latlons_from_columns,
    spatial_join_coordinates_to_shapefile,
)

# Set up input and output directories
SURVEY_PATH = r"M:/Data/OnBoard/Data and Reports/BART/2024/2024 BART OD Data (sent 11.7.2024).xlsx"
SHP_PATH = r"M:/Data/Requests/Louisa Leung/BART Survey Data/VTATAZ_CCAG/VTATAZ.shp"
OUTPUT_DIR = r"M:/Data/Requests/Louisa Leung/BART Survey Data/"


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

