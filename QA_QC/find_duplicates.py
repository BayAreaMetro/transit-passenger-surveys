"""
Purpose:
--------
This script is intended for quality control (QC) of consultant survey deliverables.
It identifies duplicate records in the survey data based on all fields except the unique identifier (ID).

Outputs:
- Total number of duplicate rows found, excluding ID.
- Excel export of duplicate records when duplicates are found.
"""

import pandas as pd

# File path
file_path = r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\AC Transit 2025 (OD Survey)\AC_Transit_MTC_ETC_Shared_Folder\Survey Databases\Final\od_20260318_ac-transit_weighted-secondary-weekend 1.xlsx"

# Sheet name
sheet_name = "OD_RESULTS"

# Read Excel file
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Report row count
num_rows = len(df)
print(f"Rows read from {sheet_name}: {num_rows:,}")

# Verify ID column exists
if "ID" not in df.columns:
    raise ValueError("Column 'ID' not found in the worksheet.")

# Columns to use for duplicate check (everything except ID)
check_cols = [col for col in df.columns if col != "ID"]

# Find duplicate rows based on all columns except ID
duplicate_mask = df.duplicated(subset=check_cols, keep=False)

duplicates = df[duplicate_mask].sort_values(by=check_cols)

# Results
num_duplicate_rows = len(duplicates)

print(f"Total duplicate rows found (excluding ID): {num_duplicate_rows}")

if num_duplicate_rows > 0:
    print("\nDuplicate records:")
    print(duplicates)

    # Export duplicates
    output_file = file_path.replace(".xlsx", "_Check_duplicates.xlsx")
    duplicates.to_excel(output_file, index=False)

    print(f"\nDuplicates exported to:\n{output_file}")
else:
    print("No duplicate rows found.")