#
# preprocess Regional Snapshot survey dataset to raw input for Build_Standard_Database.R
#
import pathlib
import pandas as pd

pd.options.display.max_rows = 999

KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    "orig_lat", # trip origin
    "orig_lon",
    "dest_lat", # trip destination
    "dest_lon",
    "Zip_Code", # home location is via zipcode
    # 02 Access and Egress Modes
    # 03 Transit Transfers - no data
    # 04 Origin and Destination Trip Purpose - we only have trip purpose
    "Q1",                # trip_purp
    # 05 Time Leaving and Returning Home - no data
    # 06 Fare Payment
    "Q5",                # fare_medium
    "Q6",                # fare_category
    # 07 Half Tour Questions for Work and School - no data
    # 08 Person Demographics
    # 09 Household Demographics
    "Q13",               # persons
    "Q14",               # workers
    "Q15",               # language_at_home_detail
    "english_at_home",   # from Q15
    # 10 Survey Metadata
    "ID",                # renamed from CCGID
    "Weight",            # weight
    "Source",            # survey_type
    "Lang",              # interview_language
    "Date",              # interivew date
    "canonical_operator",# from Syscode
    "survey_tech",       # from Type and Syscode
    "Route",             # survey route, to be added to canonical_route_crosswalk
    "Dir",               # survey route direction
    "Strata"             # time_period: note AM is not split into EA/AM and PM is not split into PM/EV
]
snapshot_dir = pathlib.Path("M:\Data\OnBoard\Data and Reports\Snapshot Survey")
snapshot_xlsx = snapshot_dir / "mtc snapshot survey_final data file.xlsx"

snapshot_df = pd.read_excel(
    io=snapshot_xlsx,
    sheet_name="data file",
)

snapshot_df["Date"] = pd.to_datetime(snapshot_df.Date)
print(f"Read {len(snapshot_df):,} lines from {snapshot_xlsx}")
print(snapshot_df.head())
print(snapshot_df.dtypes)

# 01 Geocoded Location Data
# ==== trip origin ====
snapshot_df.loc[ snapshot_df["Orig_Lat/Long"].str.lower()=="unspecified", "Orig_Lat/Long"] = None
lat_lon = snapshot_df["Orig_Lat/Long"].str.split(",", expand=True)
snapshot_df["orig_lat"] = pd.to_numeric(lat_lon[0], errors='coerce') # invalid parsing will be set as NaN
snapshot_df["orig_lon"] = pd.to_numeric(lat_lon[1], errors='coerce')

# ==== trip destination ====
snapshot_df.loc[ snapshot_df["Dest_Lat/Long"].str.lower()=="unspecified", "Dest_Lat/Long"] = None
lat_lon = snapshot_df["Dest_Lat/Long"].str.split(",", expand=True)
snapshot_df["dest_lat"] = pd.to_numeric(lat_lon[0], errors='coerce') # invalid parsing will be set as NaN
snapshot_df["dest_lon"] = pd.to_numeric(lat_lon[1], errors='coerce')

print(snapshot_df[["CCGID","Orig_Lat/Long","orig_lat","orig_lon",
                   "Dest_Lat/Long","dest_lat","dest_lon"]].head())

# 04 Origin and Destination Trip Purpose - we only have trip purpose
print("trip_purpose:")
print(snapshot_df.Q1.value_counts())
print(f"M for multiple: {len(snapshot_df[snapshot_df.Q1 == 'M'])/len(snapshot_df)}")

# 09 Household Demographics
print("language_at_home:")
print(snapshot_df.Q15.value_counts())
snapshot_df["english_at_home"] = 0
snapshot_df.loc[ snapshot_df.Q15==1, "english_at_home"] = 1

age_cat_to_year_born = {
}
# snapshot_df["year_born_four_digit"] = snapshot_df["age"].map(age_cat_to_year_born)

# 10 Survey Metadata
snapshot_df.rename(columns={'CCGID':'ID'}, inplace=True)
SYSCODE_TO_OPERATOR = {
    1 : "AC TRANSIT",
    2 : "BART",
    3 : "CALTRAIN",
    4 : "COUNTY CONNECTION",
    5 : "DUMBARTON",
    6 : "FAST",
    7 : "LAVTA",
    8 : "MARIN TRANSIT",
    9 : "NAPA VINE",
    10: "PETALUMA TRANSIT",
    11: "RIO VISTA",
    12: "SAMTRANS",
    13: "VTA",
    14: "Santa Rosa CityBus",
    15: "MUNI",
    16: "SMART",
    17: "SOLTRANS",
    18: "Sonoma County Transit",
    19: "TRI-DELTA",
    20: "UNION CITY",
    21: "VACAVILLE CITY COACH",
    22: "WESTCAT",
    23: "SF BAY FERRY"
}
snapshot_df["canonical_operator"] = snapshot_df.Syscode.map(SYSCODE_TO_OPERATOR)
# Cable cars classified as local bus consistent with
# https://github.com/BayAreaMetro/modeling-website/wiki/TransitModes
print(snapshot_df[["Syscode","Type"]].value_counts())
TYPE_TO_SURVEY_TECH = {
    1: "commuter rail", # Rail
    2: "ferry",         # Ferry
    3: "local bus",     # Bus (general)
    4: "local bus",     # Bus - Local (AC Transit, Westcat, and Soltrans only)
    5: "express bus",   # Bus - Express (AC Transit, Westcat, and Soltrans only)
    6: "express bus",   # Bus - Transbay (AC Transit and Westcat only)
    7: "light rail",    # Light rail (Muni and VTA only)
    8: "local bus",     # Cable car/streetcar (Muni only)
}
snapshot_df["survey_tech"] = snapshot_df.Type.map(TYPE_TO_SURVEY_TECH)
# BART is heavy rail
snapshot_df.loc[(snapshot_df.survey_tech=="commuter rail") &
                (snapshot_df.canonical_operator=="BART"), "survey_tech"] = "heavy rail"
print(snapshot_df[["canonical_operator","survey_tech"]].value_counts())

print(f"snapshot_df[KEEP_COLUMNS].head()=\n{snapshot_df[KEEP_COLUMNS].head()}")

# save to csv
snapshot_csv = snapshot_dir / "mtc_snapshot_preprocessed.csv"
snapshot_df[KEEP_COLUMNS].to_csv(
    snapshot_csv, 
    index=False,
    date_format="%Y-%m-%d")
print(f"Saved {snapshot_csv}")