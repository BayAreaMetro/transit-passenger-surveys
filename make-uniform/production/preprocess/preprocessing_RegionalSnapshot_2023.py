#
# preprocess Regional Snapshot survey dataset to raw input for Build_Standard_Database.R
# Working files are in M:\Data\OnBoard\Data and Reports\Snapshot Survey
# Input: "mtc snapshot survey_final data file.xlsx"
# Output: mtc_snapshot_preprocessed.csv
#         mtc_snapshot_preprocess.log
import logging
import pathlib
import pandas as pd
import numpy as np
import geopandas # for home zip => lat/long

pd.options.display.max_rows = 999
logger = logging.getLogger("survey_preprocessor")

KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    "orig_lat", # trip origin
    "orig_lon",
    "dest_lat", # trip destination
    "dest_lon",
    "home_lat", # home location is via zipcode
    "home_lon",
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
    "Q16",               # eng_proficient
    "Q18",               # gender
    "hispanic",          # Based on Q19_[1234]
    "race_dmy_asn",      # Race: Asian from Q19_[1234]
    "race_dmy_blk",      # Race: Black from Q19_[1234]
    "race_dmy_hwi",      # Race: Native Hawaiin or other Pacific Islander from Q19_[1234]
    "race_dmy_ind",      # Race: American indian Q19_[1234]
    "race_dmy_wht",      # Race: White from Q19_[1234]
    "year_born_four_digit", # Based on Q20 (age)
    "work_status",       # Based on Q22
    "student_status",    # Based on Q22
    # 09 Household Demographics
    "Q13",               # persons
    "Q14",               # workers
    "Q15",               # language_at_home_detail
    "english_at_home",   # from Q15
    "Q22",               # household income
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

LOG_FILE = "mtc_snapshot_preprocess.log"
# ================= Create logger =================
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(ch)
# file handler
fh = logging.FileHandler(snapshot_dir / LOG_FILE, mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(fh)

logging.info(f"Writing log file to {snapshot_dir / LOG_FILE}")
snapshot_df = pd.read_excel(
    io=snapshot_xlsx,
    sheet_name="data file",

    dtype={
        # Zip_Code
        'Zip_Code':str,
        # language at home
        'Q15':str,
        # race/ethnicity will be recoded
        'Q19_1':str, 'Q19_2':str, 'Q19_3':str, 'Q19_4':str,
    }
)

snapshot_df["Date"] = pd.to_datetime(snapshot_df.Date)
logging.info(f"Read {len(snapshot_df):,} lines from {snapshot_xlsx}")
logging.debug(f"head:\n{snapshot_df.head()}")
logging.debug(f"dtypes:\n{snapshot_df.dtypes}")

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

logging.debug(f"Location head():\n{snapshot_df[['CCGID','Orig_Lat/Long','orig_lat','orig_lon','Dest_Lat/Long','dest_lat','dest_lon']].head()}")

# zip code
logging.debug(f"Zip_Code value_counts().head():\n{snapshot_df.Zip_Code.value_counts(dropna=False).head(20)}")
# read 2020 Census Zip Code shapefiles
ZIP_SHAPEFILE = "M:\\Data\\GIS layers\\Census\\2020\\tl_2020_us_zcta520\\tl_2020_us_zcta520.shp"
fh.setLevel(logging.INFO)
zip_gdf = geopandas.read_file(ZIP_SHAPEFILE)
fh.setLevel(logging.DEBUG)
logging.info(f"Read {len(zip_gdf):,} rows from {ZIP_SHAPEFILE}")
logging.debug(f"head:\n{zip_gdf.head()}")
logging.debug(f"dtypes:\n{zip_gdf.dtypes}")
logging.debug(f"crs:\n{zip_gdf.crs}")

# transform to WGS84
zip_gdf.to_crs(epsg=4326, inplace=True)
logging.debug(f"converted to crs:\n{zip_gdf.crs}")

# calculate centroid
zip_centroid_coords = zip_gdf.geometry.centroid.get_coordinates()
zip_centroid_coords["Zip_Code"] = zip_gdf.GEOID20
zip_centroid_coords.rename(columns={"x":"home_lon", "y":"home_lat"}, inplace=True)
logging.debug(f"zip_centroid_coords.head():\n{zip_centroid_coords.head()}")

# release this
del zip_gdf
# join snapshot_df to zip_centroid_coords
snapshot_df = pd.merge(
    left=snapshot_df,
    right=zip_centroid_coords,
    how='left',
    on='Zip_Code',
    indicator=True)
logging.debug(f"Zip_Code join results\n{snapshot_df._merge.value_counts(dropna=False)}")
snapshot_df.drop(columns=['_merge'], inplace=True)

# 04 Origin and Destination Trip Purpose - we only have trip purpose
logging.debug(f"trip_purpose:\n{snapshot_df.Q1.value_counts(dropna=False)}")
logging.info(f"Q1 (Trip Purpose) M for multiple: {len(snapshot_df[snapshot_df.Q1 == 'M'])/len(snapshot_df)}")

# 08 Person Demographics
RACE_CODE = {
    '1': "African American/Black",
    '2': "American Indian / Alaska Native",
    '3': "Asian",
    '4': "Hispanic, Latino or Spanish origin",
    '5': "Native Hawaiian or Other Pacific Islander",
    '6': "White",
    '7': "Another (Unspecified)",
    '8': "NOT USED",
    '9': "Mixed (Unspecified)"
}
# recode Q19 to strings
logging.debug(f"Q19 (Race) value_counts:\n{snapshot_df[['Q19_1','Q19_2','Q19_3','Q19_4']].value_counts(dropna=False)}")
snapshot_df.Q19_1 = snapshot_df.Q19_1.map(RACE_CODE)
snapshot_df.Q19_2 = snapshot_df.Q19_2.map(RACE_CODE)
snapshot_df.Q19_3 = snapshot_df.Q19_3.map(RACE_CODE)
snapshot_df.Q19_4 = snapshot_df.Q19_4.map(RACE_CODE)
snapshot_df["Q19_count"] = snapshot_df[['Q19_1','Q19_2','Q19_3','Q19_4']].count(axis=1)
logging.debug(f"Q19 (Race) value_counts after recode:\n{snapshot_df[['Q19_1','Q19_2','Q19_3','Q19_4','Q19_count']].head(20)}")

# "Hispanic, Latino or Spanish origin" -- code into "hispanic"
snapshot_df.loc[ snapshot_df.Q19_1 == "Hispanic, Latino or Spanish origin", "hispanic"] = "hispanic"
snapshot_df.loc[ snapshot_df.Q19_2 == "Hispanic, Latino or Spanish origin", "hispanic"] = "hispanic"
snapshot_df.loc[ snapshot_df.Q19_3 == "Hispanic, Latino or Spanish origin", "hispanic"] = "hispanic"
snapshot_df.loc[ snapshot_df.Q19_4 == "Hispanic, Latino or Spanish origin", "hispanic"] = "hispanic"
# Now that it's coded, remove from Q19
snapshot_df.loc[ snapshot_df.Q19_1 == "Hispanic, Latino or Spanish origin", "Q19_1" ] = np.nan
snapshot_df.loc[ snapshot_df.Q19_2 == "Hispanic, Latino or Spanish origin", "Q19_2" ] = np.nan
snapshot_df.loc[ snapshot_df.Q19_3 == "Hispanic, Latino or Spanish origin", "Q19_3" ] = np.nan
snapshot_df.loc[ snapshot_df.Q19_4 == "Hispanic, Latino or Spanish origin", "Q19_4" ] = np.nan
snapshot_df["Q19_count"] = snapshot_df[["Q19_1","Q19_2","Q19_3","Q19_4"]].count(axis=1)
# For the rest, if no race category is marked, call this missing. Otherwise, non hispanic
snapshot_df.loc[ pd.isna(snapshot_df.hispanic) & (snapshot_df.Q19_count == 0), "hispanic"] = ""
snapshot_df.loc[ pd.isna(snapshot_df.hispanic) & (snapshot_df.Q19_count >  0), "hispanic"] = "not hispanic"

# race coding
snapshot_df["race_dmy_asn"] = ""
snapshot_df.loc[ snapshot_df.Q19_count > 0, "race_dmy_asn"] = "no" # default to 0 if any race specified
snapshot_df.loc[(snapshot_df.Q19_1 == "Asian") |
                (snapshot_df.Q19_2 == "Asian") |
                (snapshot_df.Q19_3 == "Asian") |
                (snapshot_df.Q19_4 == "Asian"), "race_dmy_asn"] = "yes"

snapshot_df["race_dmy_blk"] = ""
snapshot_df.loc[ snapshot_df.Q19_count > 0, "race_dmy_blk"] = "no" # default to 0 if any race specified
snapshot_df.loc[(snapshot_df.Q19_1 == "African American/Black") |
                (snapshot_df.Q19_2 == "African American/Black") |
                (snapshot_df.Q19_3 == "African American/Black") |
                (snapshot_df.Q19_4 == "African American/Black"), "race_dmy_blk"] = "yes"

snapshot_df["race_dmy_hwi"] = ""
snapshot_df.loc[ snapshot_df.Q19_count > 0, "race_dmy_hwi"] = "no" # default to 0 if any race specified
snapshot_df.loc[(snapshot_df.Q19_1 == "Native Hawaiian or Other Pacific Islander") |
                (snapshot_df.Q19_2 == "Native Hawaiian or Other Pacific Islander") |
                (snapshot_df.Q19_3 == "Native Hawaiian or Other Pacific Islander") |
                (snapshot_df.Q19_4 == "Native Hawaiian or Other Pacific Islander"), "race_dmy_hwi"] = "yes"

snapshot_df["race_dmy_ind"] = ""
snapshot_df.loc[ snapshot_df.Q19_count > 0, "race_dmy_ind"] = "no" # default to 0 if any race specified
snapshot_df.loc[(snapshot_df.Q19_1 == "American Indian / Alaska Native") |
                (snapshot_df.Q19_2 == "American Indian / Alaska Native") |
                (snapshot_df.Q19_3 == "American Indian / Alaska Native") |
                (snapshot_df.Q19_4 == "American Indian / Alaska Native"), "race_dmy_ind"] = "yes"

snapshot_df["race_dmy_wht"] = ""
snapshot_df.loc[ snapshot_df.Q19_count > 0, "race_dmy_wht"] = "no" # default to 0 if any race specified
snapshot_df.loc[(snapshot_df.Q19_1 == "White") |
                (snapshot_df.Q19_2 == "White") |
                (snapshot_df.Q19_3 == "White") |
                (snapshot_df.Q19_4 == "White"), "race_dmy_wht"] = "yes"

logging.debug(snapshot_df[["Q19_1","Q19_2","Q19_3","Q19_4","Q19_count",
                   "hispanic",
                   "race_dmy_asn","race_dmy_blk","race_dmy_hwi","race_dmy_ind","race_dmy_wht"]].head(30))

logging.debug(snapshot_df[["Q19_1","Q19_2","Q19_3","Q19_4","Q19_count",
                   "hispanic",
                   "race_dmy_asn","race_dmy_blk","race_dmy_hwi","race_dmy_ind","race_dmy_wht"]].value_counts())

# age / year born
AGE_CAT_TO_YEAR_BORN = {
    1: 2015, # Under 13 [7.5],  2023-8 = 2015
    2: 2008, # 13 - 17 [15], 2023-15 = 2008
    3: 2002, # 18 - 24 [21], 2023-21 = 2002
    4: 1993, # 25-34 [29.5], 2023-30 = 1993
    5: 1983, # 35-44 [39.5], 2023-40 = 1983
    6: 1973, # 45-54 [49.5], 2023-50 = 1973
    7: 1963, # 55-64 [59.5], 2023-60 = 1963
    8: 1953, # 65 or older [70], 2023-70 = 1953
}
snapshot_df["year_born_four_digit"] = snapshot_df.Q20.map(AGE_CAT_TO_YEAR_BORN)
logging.debug(snapshot_df[["Q20","year_born_four_digit"]].value_counts(dropna=False))

Q22_TO_WORK_STATUS = {
    1:  "full- or part-time", # Employed full time<br> (35 or more hours/week)
    2:  "full- or part-time", # Employed part time
    3:  "non-worker",         # Student
    4:  "non-worker",         # Retired
    5:  "non-worker",         # Unemployed
    6:  "non-worker",         # Other (Unspecified)
    9:  "non-worker",         # Homemaker/Caregiver
    10: "non-worker",         # Disabled
}
Q22_TO_STUDENT_STATUS = {
    1:  "non-student",        # Employed full time<br> (35 or more hours/week)
    2:  "non-student",        # Employed part time
    3:  "full- or part-time", # Student
    4:  "non-student",        # Retired
    5:  "non-student",        # Unemployed
    6:  "non-student",        # Other (Unspecified)
    9:  "non-student",        # Homemaker/Caregiver
    10: "non-student",        # Disabled
}
# todo: M is dropped?
snapshot_df["work_status"]  = snapshot_df.Q22.map(Q22_TO_WORK_STATUS)
snapshot_df["student_status"] = snapshot_df.Q22.map(Q22_TO_STUDENT_STATUS)
logging.info(f"Q22 (Employment Status) M for multiple: {len(snapshot_df[snapshot_df.Q22 == 'M'])/len(snapshot_df)}")
logging.debug(snapshot_df[["work_status","student_status"]].value_counts(dropna=False))

# 09 Household Demographics
logging.debug("language_at_home:")
logging.debug(snapshot_df.Q15.value_counts(dropna=False))
snapshot_df["english_at_home"] = ""
snapshot_df.loc[ snapshot_df.Q15=="1", "english_at_home"] = 1
# non-english at home
snapshot_df.loc[ (snapshot_df.Q15 != "B") & (snapshot_df.Q15 != "1"), "english_at_home" ] = 0
logging.debug(f"Q15 (Language at home) M for multiple: {len(snapshot_df[snapshot_df.Q15 == 'M'])/len(snapshot_df)}")

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
logging.debug(snapshot_df[["Syscode","Type"]].value_counts())
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
logging.debug(snapshot_df[["canonical_operator","survey_tech"]].value_counts())

logging.debug(f"snapshot_df[KEEP_COLUMNS].head()=\n{snapshot_df[KEEP_COLUMNS].head()}")

# save to csv
snapshot_csv = snapshot_dir / "mtc_snapshot_preprocessed.csv"
snapshot_df[KEEP_COLUMNS].to_csv(
    snapshot_csv, 
    index=False,
    date_format="%Y-%m-%d")
logging.info(f"Saved {snapshot_csv}")