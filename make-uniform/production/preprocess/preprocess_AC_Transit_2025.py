# Clean column names
# For string columns, replace ' and #
# Still to do: process transfer routes

import pandas as pd
import re

# File path
input_file = r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\AC Transit 2025 (OD Survey)\AC_Transit_MTC_ETC_Shared_Folder\Survey Databases\Final\od_20260318_ac-transit_weighted-secondary-weekend 1.xlsx"
output_file = r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\AC Transit 2025 (OD Survey)\AC_Transit_MTC_ETC_Shared_Folder\Survey Databases\Final\AC_Transit_2025_preprocessed.csv"

# Read the Excel file
print("Reading Excel file...")
df = pd.read_excel(input_file, sheet_name='OD_RESULTS')

print(f"Original shape: {df.shape}")
print(f"Original columns: {list(df.columns)}")

# Clean column names
# 1. Replace " [" with "_"
# 2. Remove ]
# 3. Check for spaces in column names
cleaned_columns = []
for col in df.columns:
    # Replace " [" with "_"
    new_col = col.replace(" [", "_")
    # Remove ]
    new_col = new_col.replace("]", "")
    cleaned_columns.append(new_col)

df.columns = cleaned_columns

# Check for spaces in column names
columns_with_spaces = [col for col in df.columns if ' ' in col]
if columns_with_spaces:
    print(f"\nColumns with spaces ({len(columns_with_spaces)}):")
    for col in columns_with_spaces:
        print(f"  - '{col}'")
else:
    print("\nNo spaces found in column names.")

print(f"\nCleaned columns: {list(df.columns)}")

# Remove all single quotes (') and pound signs (#) from all data
print("\nRemoving single quotes and pound signs from data...")

# For string columns, replace ' and #
for col in df.columns:
    if df[col].dtype == 'object':  # String columns
        df[col] = df[col].astype(str).str.replace("'", "", regex=False).str.replace("#", "", regex=False)
        # Convert 'nan' strings back to actual NaN
        df[col] = df[col].replace('nan', pd.NA)

# Save to new file
print(f"\nSaving cleaned data to: {output_file}")
df.to_csv(output_file, index=False)

print("Done! File cleaned successfully.")
print(f"Final shape: {df.shape}")
