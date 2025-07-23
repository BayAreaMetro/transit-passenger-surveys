#
# preprocess ACE dataset to raw input for Build_Standard_Database.R
#
import pathlib
import pandas as pd
import geopandas as gpd

pd.options.display.max_rows = 999

# See notes_ACE_2023.csv
KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    "d_lat",
    "d_lng",
    "home_lat",
    "home_lon",
    # todo: survey_[board|alight]_[lat|lon] can be inferred by board, alight
    # 02 Access and Egress Modes
    "access",
    "egress",
    # 03 Transit Transfers - no data
    # 04 Origin and Destination Trip Purpose - we only have trip purpose
    "purpose",
    # 05 Time Leaving and Returning Home - no data
    # 06 Fare Payment
    "ticket",
    # 07 Half Tour Questions for Work and School - no data
    # 08 Person Demographics
    "hispanic",
    "race_1",
    "race_2",
    "race_3",
    "race_4",
    "race_5",
    "race_other",
    "year_born_four_digit", # created from age
    "gender",
    "employment",
    "english_level",
    # 09 Household Demographics
    "lang_binary", # created from lang
    "lang",
    "hh_size",
    "hh_emp",
    "veh",
    "income",
    # 10 Survey Metadata
    "id",
    "initial_weight",
    "weight",
    "survey_lang",
    "board",
    "alight",
    "direction",
    "survey_date",
    "survey_time_estimate"
]
ACE_dir = pathlib.Path("M:\Data\OnBoard\Data and Reports\ACE\\2023")
ACE_xlsx = ACE_dir / "ACE Onboard Data (sent 7.7.23).xlsx"

ACE_data_df = pd.read_excel(
    io=ACE_xlsx,
    sheet_name="ACE Onboard Data Weighted 7.7.2"
)
print(f"Read {len(ACE_data_df):,} lines from {ACE_xlsx}")
print(ACE_data_df.head())
print(ACE_data_df.dtypes)

# todo: convert access_other to access codes
print(f"{ACE_data_df.value_counts(subset=['access','access_other'],dropna=False)=}")
print(f"{ACE_data_df.value_counts(subset=['egress','egress_other'],dropna=False)=}")

age_cat_to_year_born = {
    1: 2008, # Under 18, 2023-15 = 2008
    2: 2002, # 18-24, 2023-21 = 2002
    3: 1993, # 25-34, 2023-30 = 1993
    4: 1983, # 35-44, 2023-40 = 1983
    5: 1973, # 45-54, 2023-50 = 1973
    6: 1965, # 55-61, 2023-58 = 1965
    7: 1960, # 62-64, 2023-63 = 1960
    8: 1953, # 65+,   2023-70 = 1953
}
ACE_data_df["year_born_four_digit"] = ACE_data_df["age"].map(age_cat_to_year_born)

# create lang_binary from lang
ACE_data_df["lang_binary"] = "OTHER"
ACE_data_df.loc[ACE_data_df.lang == 1, "lang_binary"] = "ENGLISH ONLY"

# Survey time: This wasn't a question but the survey notes (via train_number) that it
# was conducted on trains ACE 04, ACE 06, ACE 08 and ACE 10
# which are eastbound trains to Stockton (https://acerail.com/schedules/)
ACE_data_df["direction"] = "EASTBOUND"

# Set survey_date based on report Table 3
train_number_to_survey_date = {
    1: "2023-04-20", # ACE 04
    2: "2023-04-19", # ACE 06
    3: "2023-04-18", # ACE 08
    4: "2023-04-17", # ACE 10
}
ACE_data_df["survey_date"] = ACE_data_df.train_number.map(train_number_to_survey_date)

# Impute survey_time from the train_number / schedule and the board station
# First, assume ACE 04
ACE_data_df["survey_time_estimate"] = 15
ACE_data_df.loc[ACE_data_df.board >= 4, "survey_time_estimate"] = 16  # Fremont
ACE_data_df.loc[ACE_data_df.board >= 8, "survey_time_estimate"] = 17  # Tracy
# now shift an hour for later trains
ACE_data_df.loc[ACE_data_df.train_number == 2, "survey_time_estimate"] = ACE_data_df["survey_time_estimate"] + 1  # ACE 06
ACE_data_df.loc[ACE_data_df.train_number == 3, "survey_time_estimate"] = ACE_data_df["survey_time_estimate"] + 2  # ACE 08
ACE_data_df.loc[ACE_data_df.train_number == 4, "survey_time_estimate"] = ACE_data_df["survey_time_estimate"] + 3  # ACE 10
# Finally convert to time string
ACE_data_df["survey_time_estimate"] = ACE_data_df["survey_time_estimate"].apply(lambda x: f"{x}:00:00")

# Convert home_zip to home_lat, home_lon

# 0. Convert from float to int then to string, then pad with leading zeros if needed
ACE_data_df["home_zip"] = ACE_data_df["home_zip"].fillna(0).astype(int).astype(str).str.zfill(5)

# 1. Load ZIP Code shapefile (ZCTAs)
zcta_gdf = gpd.read_file("M:/Data/Census/Geography/2020_ZCTAs/2020_ZCTAs.shp")

# 2. Ensure GEOID is treated as string
zcta_gdf["ZCTA"] = zcta_gdf["GEOID"].astype(str)

# 3. Reproject to a projected CRS for accurate centroid calculation
zcta_gdf = zcta_gdf.to_crs(epsg=26910)

# 4. Replace geometry with centroids (in projected CRS)
zcta_gdf = zcta_gdf.copy()
zcta_gdf["geometry"] = zcta_gdf.geometry.centroid

# 5. Reproject centroids to lat/lon
zcta_gdf = zcta_gdf.to_crs(epsg=4326)

# 6. Extract lon/lat
zcta_gdf["home_lon"] = zcta_gdf.geometry.x
zcta_gdf["home_lat"] = zcta_gdf.geometry.y

# 7. Create ZIP-to-centroid lookup table
zcta_centroids = zcta_gdf[["ZCTA", "home_lat", "home_lon"]]

# 8. Ensure ACE_data_df["home_zip"] is 5-digit string
ACE_data_df["home_zip"] = ACE_data_df["home_zip"].astype(str).str.zfill(5)

# 9. Merge with centroid data
ACE_data_df = ACE_data_df.merge(
    zcta_centroids,
    how="left",
    left_on="home_zip",
    right_on="ZCTA"
)

# 10. Drop extra column
ACE_data_df.drop(columns=["ZCTA"], inplace=True)

# Expand the data to April 2023 monthly ridership using the existing "weight" variable for naive weight correction
# Load csv with monthly ridership data

ridership_data = pd.read_csv(ACE_dir / "April 2023 Monthly Performance Report.csv")

# Retrieve monthly ridership value from April 2023

apr_value_2023 = ridership_data.loc[ridership_data['Fiscal_Year'] == 2023, 'APR'].values[0]

# Calculate average daily from monthly ridership total, prepare to distribute naive weight over all records

days = 20 # There were 20 weekdays in April 2023 and there is currently no weekend service for ACE
num_records = len(ACE_data_df)
naive_weight = (apr_value_2023 / days) / num_records

# Rename existing "weight" variable from consultant to "initial_weight"

ACE_data_df.rename(columns={'weight': 'initial_weight'}, inplace=True)

# Multiply initial_weight (a true "weight", not an expansion factor) by naive weight (really an expansion factor)
# Result is a final "weight" value that sums to average daily ridership

ACE_data_df['weight'] = ACE_data_df['initial_weight'] * naive_weight

# save to csv
ACE_csv = ACE_dir / "ACE_Onboard_preprocessed.csv"
ACE_data_df[KEEP_COLUMNS].to_csv(ACE_csv, index=False)
print(f"ACE_data_df[KEEP_COLUMNS].head()=\n{ACE_data_df[KEEP_COLUMNS].head()}")
print(f"Saved {ACE_csv}")

