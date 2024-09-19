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

# See notes_Regional Snapshot_2023.csv
KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    "orig_lat",          # trip origin
    "orig_lon",
    "orig_geo_level",    # specification level of origin, one of "point" or "city"
    "dest_lat",          # trip destination
    "dest_lon",
    "dest_geo_level",    # specification level of destination, one of "point" or "city"
    "home_lat",          # home location is via zipcode
    "home_lon",        
    "home_geo_level",    # specification level of home, here it's "zip"
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
    "interview_date",    # from Intdate
    "canonical_operator",# from Syscode
    "survey_tech",       # from Type and Syscode
    "Route",             # survey route, to be added to canonical_route_crosswalk
    "Dir",               # survey route direction
    "Strata"             # time_period: note AM is not split into EA/AM and PM is not split into PM/EV
]
snapshot_dir = pathlib.Path("M:\Data\OnBoard\Data and Reports\Snapshot Survey")
snapshot_xlsx = snapshot_dir / "mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx"

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

logging.info(f"Read {len(snapshot_df):,} lines from {snapshot_xlsx}")
logging.debug(f"head:\n{snapshot_df.head()}")
logging.debug(f"dtypes:\n{snapshot_df.dtypes}")

# 01 Geocoded Location Data

# first, read place (city) shapefile
PLACE_SHAPEFILE = "M:\\Data\\GIS layers\\Census\\2023\\tl_2023_06_place\\tl_2023_06_place.shp"
fh.setLevel(logging.INFO)
place_gdf = geopandas.read_file(PLACE_SHAPEFILE)
fh.setLevel(logging.DEBUG)
logging.info(f"Read {len(place_gdf):,} rows from {PLACE_SHAPEFILE}")
logging.debug(f"head:\n{place_gdf.head()}")
logging.debug(f"dtypes:\n{place_gdf.dtypes}")
logging.debug(f"crs:\n{place_gdf.crs}")

# There are a number of entires with duplicate names; choose the one with the larger area
place_gdf.sort_values(by=['NAME','ALAND'], ascending=[True,False], inplace=True)
logging.debug(f"Places with duplicate names:\n{place_gdf.loc[place_gdf.duplicated(subset='NAME', keep=False)]}")
place_gdf.drop_duplicates(subset='NAME', keep='first', inplace=True)

# transform to WGS84
place_gdf.to_crs(epsg=4326, inplace=True)
logging.debug(f"converted to crs:\n{place_gdf.crs}")

# calculate centroid
place_centroid_coords = place_gdf.geometry.centroid.get_coordinates()
place_centroid_coords["PLACE_NAME"] = place_gdf.NAME.str.upper()
place_centroid_coords.rename(columns={"x":"place_lon", "y":"place_lat"}, inplace=True)
logging.debug(f"place_centroid_coords.head():\n{place_centroid_coords.head(10)}")

# ==== trip origin ====
snapshot_df.loc[ snapshot_df["Orig_Lat/Long"].str.lower()=="unspecified", "Orig_Lat/Long"] = None
lat_lon = snapshot_df["Orig_Lat/Long"].str.split(",", expand=True)
snapshot_df["orig_lat"] = pd.to_numeric(lat_lon[0], errors='coerce') # invalid parsing will be set as NaN
snapshot_df["orig_lon"] = pd.to_numeric(lat_lon[1], errors='coerce')
# note specifcation level for these
snapshot_df.loc[ pd.notna(snapshot_df.orig_lat), "orig_geo_level" ] = "point"

# a few spelling fixes
for city_col in ["Q3a", "Q4a"]:
    snapshot_df[city_col] = snapshot_df[city_col].str.upper()
    snapshot_df[city_col] = snapshot_df[city_col].str.strip() # strip whitespace
    snapshot_df[city_col] = snapshot_df[city_col].str.strip(".") # strip periods
    snapshot_df.loc[ snapshot_df[city_col]=="UNSPECIFED",       city_col] = "UNSPECIFIED"
    snapshot_df.loc[ snapshot_df[city_col]=="HILLSDALE",        city_col] = "SAN MATEO"  # shopping mall/Caltrain station in San Mateo
    snapshot_df.loc[ snapshot_df[city_col]=="SAINT HELENA",     city_col] = "ST. HELENA"
    snapshot_df.loc[ snapshot_df[city_col]=="SANTA HELENA",     city_col] = "ST. HELENA"
    snapshot_df.loc[ snapshot_df[city_col]=="TERRA LINDA",      city_col] = "SAN RAFAEL" # district of San Rafael
    snapshot_df.loc[ snapshot_df[city_col]=="BENECIA",          city_col] = "BENICIA"
    snapshot_df.loc[ snapshot_df[city_col]=="SAN FRANCICO",     city_col] = "SAN FRANCISCO"
    snapshot_df.loc[ snapshot_df[city_col]=="BERNAL HEIGHTS",   city_col] = "SAN FRANCISCO"
    snapshot_df.loc[ snapshot_df[city_col]=="SUISUN",           city_col] = "SUISUN CITY"
    snapshot_df.loc[ snapshot_df[city_col]=="VACAVILLLE",       city_col] = "VACAVILLE"
    snapshot_df.loc[ snapshot_df[city_col]=="PETAALUMA",        city_col] = "PETALUMA"
    snapshot_df.loc[ snapshot_df[city_col]=="MILBRAE",          city_col] = "MILLBRAE"
    snapshot_df.loc[ snapshot_df[city_col]=="MARE ISLAND",      city_col] = "VALLEJO" # https://en.wikipedia.org/wiki/Mare_Island
    snapshot_df.loc[ snapshot_df[city_col]=="WINDSOR CA",       city_col] = "WINDSOR"
    snapshot_df.loc[ snapshot_df[city_col]=="BERRYESSA",        city_col] = "SAN JOSE"
    snapshot_df.loc[ snapshot_df[city_col]=="PITTSBURGH",       city_col] = "PITTSBURG"
    snapshot_df.loc[ snapshot_df[city_col]=="BAYPOINT",         city_col] = "BAY POINT"
    #  stations
    snapshot_df.loc[ snapshot_df[city_col]=="NORTH BERKELEY",   city_col] = "BERKELEY"
    snapshot_df.loc[ snapshot_df[city_col]=="BLOSSOM HILL",     city_col] = "SAN JOSE"
    snapshot_df.loc[ snapshot_df[city_col]=="LAWRENCE",         city_col] = "SUNNYVALE"
    snapshot_df.loc[ snapshot_df[city_col]=="LAURENCE STATION", city_col] = "SUNNYVALE"
    snapshot_df.loc[ snapshot_df[city_col]=="SOUTH HAYWARD",    city_col] = "HAYWARD"
    snapshot_df.loc[ snapshot_df[city_col]=="MONTGOMERY",       city_col] = "SAN FRANCISCO"

# try for place-based join on origin city
snapshot_df = pd.merge(
    left      = snapshot_df,
    right     = place_centroid_coords,
    how       = 'left',
    left_on   = 'Q3a', # origin city
    right_on  = 'PLACE_NAME',
    indicator = True,
    validate  = 'many_to_one'
)
logging.debug("success joining on origin city:\n" + 
              str(snapshot_df.loc[ pd.isna(snapshot_df.orig_lat) &
                                   pd.notna(snapshot_df.Q3a), '_merge'].value_counts()))
logging.debug("unmached: \n" + 
              str(snapshot_df.loc[ pd.isna(snapshot_df.orig_lat) &
                                   pd.notna(snapshot_df.Q3a) & 
                                   (snapshot_df._merge=='left_only'), ['Q3a','_merge']].value_counts()))
# set it
snapshot_df.loc[ pd.isna(snapshot_df.orig_lat) &
                 pd.notna(snapshot_df.place_lat), "orig_geo_level"] = "city"
snapshot_df.loc[ pd.isna(snapshot_df.orig_lat) &
                 pd.notna(snapshot_df.place_lat), "orig_lat"] = snapshot_df.place_lat
snapshot_df.loc[ pd.isna(snapshot_df.orig_lon) &
                 pd.notna(snapshot_df.place_lon), "orig_lon"] = snapshot_df.place_lon
snapshot_df.drop(columns=['_merge','place_lat','place_lon'], inplace=True)
logging.debug(f"orig_geo_level:\n{snapshot_df.orig_geo_level.value_counts(dropna=False)}")

# ==== trip destination ====
snapshot_df.loc[ snapshot_df["Dest_Lat/Long"].str.lower()=="unspecified", "Dest_Lat/Long"] = None
lat_lon = snapshot_df["Dest_Lat/Long"].str.split(",", expand=True)
snapshot_df["dest_lat"] = pd.to_numeric(lat_lon[0], errors='coerce') # invalid parsing will be set as NaN
snapshot_df["dest_lon"] = pd.to_numeric(lat_lon[1], errors='coerce')
# note specifcation level for these
snapshot_df.loc[ pd.notna(snapshot_df.orig_lat), "dest_geo_level" ] = "point"

logging.debug(f"Location head():\n" + str(snapshot_df[[
    'CCGID',
    'Orig_Lat/Long','orig_lat','orig_lon','orig_geo_level',
    'Dest_Lat/Long','dest_lat','dest_lon','dest_geo_level']].head()))

# try for place-based join on origin city
snapshot_df = pd.merge(
    left      = snapshot_df,
    right     = place_centroid_coords,
    how       = 'left',
    left_on   = 'Q4a', # origin city
    right_on  = 'PLACE_NAME',
    indicator = True,
    validate  = 'many_to_one'
)

logging.debug("success joining on destination city:\n" + 
              str(snapshot_df.loc[ pd.isna(snapshot_df.dest_lat) &
                                   pd.notna(snapshot_df.Q4a), '_merge'].value_counts()))
logging.debug("unmached: \n" + 
              str(snapshot_df.loc[ pd.isna(snapshot_df.dest_lat) &
                                   pd.notna(snapshot_df.Q4a) & 
                                   (snapshot_df._merge=='left_only'), ['Q4a','_merge']].value_counts()))
# set it
snapshot_df.loc[ pd.isna(snapshot_df.dest_lat) &
                 pd.notna(snapshot_df.place_lat), "dest_geo_level"] = "city"
snapshot_df.loc[ pd.isna(snapshot_df.dest_lat) &
                 pd.notna(snapshot_df.place_lat), "dest_lat"] = snapshot_df.place_lat
snapshot_df.loc[ pd.isna(snapshot_df.dest_lon) &
                 pd.notna(snapshot_df.place_lon), "dest_lon"] = snapshot_df.place_lon
snapshot_df.drop(columns=['_merge','place_lat','place_lon'], inplace=True)
logging.debug(f"dest_geo_level:\n{snapshot_df.dest_geo_level.value_counts(dropna=False)}")

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
snapshot_df["home_geo_level"] = "zip"

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
logging.debug(f"Duplicated IDs:\n{snapshot_df.loc[ snapshot_df.duplicated(subset=['ID'], keep=False) ]}")
# verify it's unique and always set
assert(len(snapshot_df.ID.unique()) == len(snapshot_df))
assert(len(snapshot_df.loc[ pd.isna(snapshot_df.ID)]) == 0)

INTERVIEW_DATE_RANGE = pd.concat([
    pd.date_range(start="2023-08-17", end="2023-08-17", freq="1D").to_series(index=[1],           name='interview_date'),
    pd.date_range(start="2023-08-22", end="2023-08-25", freq="1D").to_series(index=[2,3,4,5],     name='interview_date'),
    pd.date_range(start="2023-09-19", end="2023-11-09", freq="1D").to_series(index=range(6,58),   name='interview_date'),
    pd.date_range(start="2023-11-12", end="2023-11-16", freq="1D").to_series(index=range(58,63),  name='interview_date'),
    pd.date_range(start="2023-11-28", end="2023-12-02", freq="1D").to_series(index=range(63,68),  name='interview_date'),
    pd.date_range(start="2023-12-04", end="2023-12-09", freq="1D").to_series(index=range(68,74),  name='interview_date'),
    pd.date_range(start="2023-12-11", end="2023-12-15", freq="1D").to_series(index=range(74,79),  name='interview_date'),
    pd.date_range(start="2024-02-01", end="2024-02-03", freq="1D").to_series(index=range(79,82),  name='interview_date'),
    pd.date_range(start="2024-02-05", end="2024-02-15", freq="1D").to_series(index=range(82,93),  name='interview_date'),
    pd.date_range(start="2024-02-17", end="2024-02-18", freq="1D").to_series(index=range(93,95),  name='interview_date'),
    pd.date_range(start="2024-02-20", end="2024-02-23", freq="1D").to_series(index=range(95,99),  name='interview_date'),
    pd.date_range(start="2024-02-26", end="2024-03-02", freq="1D").to_series(index=range(99,105), name='interview_date'),
    pd.date_range(start="2024-03-04", end="2024-03-09", freq="1D").to_series(index=range(105,111),name='interview_date'),
    pd.date_range(start="2024-03-11", end="2024-03-30", freq="1D").to_series(index=range(111,131),name='interview_date'),
    pd.date_range(start="2024-04-01", end="2024-05-23", freq="1D").to_series(index=range(131,184),name='interview_date'),
]).to_frame()
INTERVIEW_DATE_RANGE["Intdate"] = INTERVIEW_DATE_RANGE.index
snapshot_df = pd.merge(
    left=snapshot_df,
    right=INTERVIEW_DATE_RANGE,
    on=['Intdate'],
    how='left',
    indicator=True,
    validate='many_to_one'
)
logging.debug(f'Intdate merge:\n{snapshot_df._merge.value_counts()}')
snapshot_df.drop(columns=['_merge'], inplace=True)

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
    11: "RIO-VISTA",
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