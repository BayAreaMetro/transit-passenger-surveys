#
# preprocess Goldeng Gate Transit/Ferry dataset to raw input for Build_Standard_Database.R
#
import logging
import pathlib
import geopandas
import pandas as pd

pd.options.display.max_rows = 999
logger = logging.getLogger("survey_preprocessor")

KEEP_COLUMNS = [
    "canonical_operator",      # one of GOLDEN GATE TRANSIT or GOLDEN GATE FERRY
    "survey_tech",             # from survey dataset and Route
    "ID",                      # ID, based on rownum (ferry, then GGT)
    # 01 Geocoded Location Data
    "orig_lat",                # from Q1a (city)
    "orig_lon",                # from Q1a (city)
    "orig_geo_level",          # city
    "dest_lat",                # from Q2a (city)
    "dest_lon",                # from Q2a (city)
    "dest_geo_level",          # city
    # todo: "survey_board_lat",   # from Q1a/Board Stop
    # todo: "survey_board_lon",   # from Q1a/Board Stop
    # todo: "survey_alight_lat",  # from Q2a/Alight Stop
    # todo: "survey_alight_lon",  # from Q2a/Alight Stop
    "home_lat",                # home location is via zipcode
    "home_lon",        
    "home_geo_level",          # specification level of home
    # 02 Access and Egress Modes
    "Access_1_recode",         # access mode 1
    "Egress_1_recode",         # egress mode 1
    # 03 Transit Transfers
    # todo: transit info from acceses/egress modes that are transit
    # 04 Origin and Destination Trip Purpose
    "Q1b",                     # orig_purp
    "Q2b",                     # dest_purp
    # 05 Time Leaving and Returning Home - no data
    # 06 Fare Payment
    "Q4",                      # fare_medium
    "Q5",                      # fare_category
    # 07 Half Tour Questions for Work and School - no data
    # 08 Person Demographics
    "eng_proficient",          # from Q15/Q17
    "gender",                  # from Q16/Q18
    "hispanic",                # Based on Q17/Q19_[1234]
    "race_dmy_asn",            # Race: Asian from Q17/Q19_[1234]
    "race_dmy_blk",            # Race: Black from Q17/Q19_[1234]
    "race_dmy_ind",            # Race: American indian Q17/Q19_[1234]
    "race_dmy_wht",            # Race: White from Q17/Q19_[1234]
    "year_born_four_digit",    # Based on Q18/Q20 (age)
    # 09 Household Demographics
    "persons",                 # from Q13/Q15
    "language_at_home_binary", # from Q14/Q16
    "language_at_home_detail", # from Q14/Q16
    "household_income",        # from Q19/Q21
    # 10 Survey Metadata
    "Route",                   # survey route
    "Dir",                     # survey route direction
    "Source",                  # survey_type
    "Lang",                    # interview_language
    "interview_date",          # from IntDate
    "Strata",                  # time_period
    "weight",                  # TODO: add simple weighting
]
GG_dir = pathlib.Path("M:\Data\OnBoard\Data and Reports\Golden Gate Transit\\2023")
GG_ferry_xlsx = GG_dir / "GGFerry2023 Final Data.xlsx"
GG_transit_xlsx = GG_dir / "GGT2023 Final Data.xlsx"
GG_ridership_xlsx = GG_dir / "Average Daily Ridership for GGT and GGF - Snapshot Survey Period.xlsx"

LOG_FILE = "GG_Transit_Ferry_preprocess.log"
# ================= Create logger =================
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(ch)
# file handler
fh = logging.FileHandler(GG_dir / LOG_FILE, mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(fh)

logging.info(f"Writing log file to {GG_dir / LOG_FILE}")

# read them both
GG_ferry_df = pd.read_excel(
    io=GG_ferry_xlsx,
    sheet_name="Data"
)
logging.info(f"Read {len(GG_ferry_df):,} lines from {GG_ferry_xlsx}")
GG_ferry_df = GG_ferry_df.add_prefix("ferry_")
GG_ferry_df.insert(loc=0, column="sub_survey", value="ferry")
GG_ferry_df.insert(loc=0, column="survey_tech", value="ferry")
GG_ferry_df.insert(loc=0, column="canonical_operator", value="GOLDEN GATE TRANSIT")
logging.debug(f"GG_ferry_df:\n{GG_ferry_df.head()}")

GG_transit_df = pd.read_excel(
    io=GG_transit_xlsx,
    sheet_name="Data"
)
logging.info(f"Read {len(GG_transit_df):,} lines from {GG_transit_xlsx}")
GG_transit_df = GG_transit_df.add_prefix("ggt_")
GG_transit_df.insert(loc=0, column="sub_survey", value="ggt")
GG_transit_df.insert(loc=0, column="survey_tech", value="express bus")
GG_transit_df.insert(loc=0, column="canonical_operator", value="GOLDEN GATE TRANSIT")
logging.debug(f"GG_transit_df:\n{GG_transit_df.head()}")

# explicitly make these the same
GG_ferry_df.rename(columns={
    'ferry_Q1b'         : 'Q1b',       # orig_purp
    'ferry_Q1a'         : 'orig_city', # origin city
    'ferry_Q2b'         : 'Q2b',       # dest_purp
    'ferry_Q2a'         : 'dest_city', # destination city
    'ferry_Q14'         : 'Home_Zipcode',
    'ferry_Access_1'    : 'Access_1', # access modes
    'ferry_Access_2'    : 'Access_2', # access modes
    'ferry_Access_3'    : 'Access_3', # access modes
    'ferry_Access_4'    : 'Access_4', # access modes
    'ferry_Egress_1'    : 'Egress_1', # egress modes
    'ferry_Egress_2'    : 'Egress_2', # egress modes
    'ferry_Egress_3'    : 'Egress_3', # egress modes
    'ferry_Egress_4'    : 'Egress_4', # egress modes
    'ferry_Q4'          : 'Q4',  # fare_medium
    'ferry_Q5'          : 'Q5',  # fare_category
    'ferry_Q15'         : 'persons',
    'ferry_Q16_1'       : 'language_at_home_1',
    'ferry_Q16_2'       : 'language_at_home_2',
    'ferry_Q16_3'       : 'language_at_home_3',
    'ferry_Q16_4'       : 'language_at_home_4',
    'ferry_Q17'         : 'eng_proficient',
    'ferry_Q18'         : 'gender',
    'ferry_Q19_1'       : 'race_1',
    'ferry_Q19_2'       : 'race_2',
    'ferry_Q19_3'       : 'race_3',
    'ferry_Q19_4'       : 'race_4',
    'ferry_Q20'         : 'age_cat',
    'ferry_Q21'         : 'household_income',
    'ferry_sys_RespNum' : 'sys_RespNum',
    'ferry_CCGID'       : 'CCGID',
    'ferry_Source'      : 'Source',
    'ferry_Lang'        : 'Lang',
    'ferry_RUNID'       : 'RUNID',
    'ferry_Route'       : 'Route',
    'ferry_Dir'         : 'Dir',
    'ferry_IntDate'     : 'IntDate',
    'ferry_Strata'      : 'Strata',
}, inplace=True)
GG_transit_df.rename(columns={
    'ggt_Q1b'           : 'Q1b',       # orig_purp
    'ggt_Q1c'           : 'orig_city', # origin city
    'ggt_Q2b'           : 'Q2b',       # dest_purp
    'ggt_Q2c'           : 'dest_city', # destination city
    'ggt_Q20'           : 'Home_Zipcode',
    'ggt_Access_1'      : 'Access_1', # access modes
    'ggt_Access_2'      : 'Access_2', # access modes
    'ggt_Access_3'      : 'Access_3', # access modes
    'ggt_Access_4'      : 'Access_4', # access modes
    'ggt_Egress_1'      : 'Egress_1', # egress modes
    'ggt_Egress_2'      : 'Egress_2', # egress modes
    'ggt_Egress_3'      : 'Egress_3', # egress modes
    'ggt_Egress_4'      : 'Egress_4', # egress modes
    'ggt_Q4'            : 'Q4',  # fare_medium
    'ggt_Q5'            : 'Q5',  # fare_category
    'ggt_Q13'           : 'persons',
    'ggt_Q14_1'         : 'language_at_home_1',
    'ggt_Q14_2'         : 'language_at_home_2',
    'ggt_Q14_3'         : 'language_at_home_3',
    'ggt_Q14_4'         : 'language_at_home_4',
    'ggt_Q15'           : 'eng_proficient',
    'ggt_Q16'           : 'gender',
    'ggt_Q17_1'         : 'race_1',
    'ggt_Q17_2'         : 'race_2',
    'ggt_Q17_3'         : 'race_3',
    'ggt_Q17_4'         : 'race_4',
    'ggt_Q18'           : 'age_cat',
    'ggt_Q19'           : 'household_income',
    'ggt_sys_RespNum'   : 'sys_RespNum',
    'ggt_CCGID'         : 'CCGID',
    'ggt_SOURCE'        : 'Source',
    'ggt_LANG'          : 'Lang',
    'ggt_RUNID'         : 'RUNID',
    'ggt_Route'         : 'Route',
    'ggt_Dir'           : 'Dir',
    'ggt_IntDate'       : 'IntDate',
    'ggt_Strata'        : 'Strata',
}, inplace=True)

# Put them together
GG_df = pd.concat([GG_ferry_df, GG_transit_df]).reset_index(drop=True)
logging.debug(f"GG_df.head():\n{GG_df.head()}")
logging.debug(f"GG_df.dtypes\n{GG_df.dtypes}")
logging.debug(f"sub_survey:\n{GG_df.sub_survey.value_counts(dropna=False)}")
del GG_ferry_df 
del GG_transit_df

# ================ 01 Geocoded Location Data ================
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
# add angel island even though it's not a city
# https://en.wikipedia.org/wiki/Angel_Island_(California)
ANGEL_ISLAND = pd.DataFrame({"place_lon":-122.43,"place_lat":37.86,"PLACE_NAME":'ANGEL ISLAND'}, index=[0])
place_centroid_coords = pd.concat([ANGEL_ISLAND,place_centroid_coords])

logging.debug(f"{len(place_centroid_coords)=}")
logging.debug(f"place_centroid_coords.head():\n{place_centroid_coords.head(10)}")

# ==== trip origin ====
# a few spelling fixes
for city_col in ["orig_city", "dest_city"]:
    GG_df[city_col] = GG_df[city_col].str.upper()
    GG_df[city_col] = GG_df[city_col].str.strip() # strip whitespace
    GG_df[city_col] = GG_df[city_col].str.strip(".") # strip periods
    GG_df.loc[ GG_df[city_col]=="TERRA LINDA",      city_col] = "SAN RAFAEL" # district of San Rafael

# try for place-based join on origin city
GG_df = pd.merge(
    left      = GG_df,
    right     = place_centroid_coords,
    how       = 'left',
    left_on   = 'orig_city',
    right_on  = 'PLACE_NAME',
    indicator = True,
    validate  = 'many_to_one'
)
logging.debug("success joining on origin city:\n" + 
              str(GG_df.loc[ pd.notna(GG_df.orig_city), '_merge'].value_counts()))
logging.debug("unmached: \n" + 
              str(GG_df.loc[ pd.notna(GG_df.orig_city) & 
                             (GG_df._merge=='left_only'), ['orig_city','_merge']].value_counts()))
# set it
GG_df.loc[ pd.notna(GG_df.place_lat), "orig_geo_level"] = "city"
GG_df.loc[ pd.notna(GG_df.place_lat), "orig_lat"] = GG_df.place_lat
GG_df.loc[ pd.notna(GG_df.place_lon), "orig_lon"] = GG_df.place_lon
GG_df.drop(columns=['_merge','place_lat','place_lon'], inplace=True)
logging.debug(f"orig_geo_level:\n{GG_df.orig_geo_level.value_counts(dropna=False)}")

# ==== trip destination ====
# try for place-based join on destination city
GG_df = pd.merge(
    left      = GG_df,
    right     = place_centroid_coords,
    how       = 'left',
    left_on   = 'dest_city', # destination city
    right_on  = 'PLACE_NAME',
    indicator = True,
    validate  = 'many_to_one'
)
logging.debug("success joining on destination city:\n" + 
              str(GG_df.loc[ pd.notna(GG_df.dest_city), '_merge'].value_counts()))
logging.debug("unmached: \n" + 
              str(GG_df.loc[ pd.notna(GG_df.dest_city) & 
                             (GG_df._merge=='left_only'), ['dest_city','_merge']].value_counts()))
# set it
GG_df.loc[ pd.notna(GG_df.place_lat), "dest_geo_level"] = "city"
GG_df.loc[ pd.notna(GG_df.place_lat), "dest_lat"] = GG_df.place_lat
GG_df.loc[ pd.notna(GG_df.place_lon), "dest_lon"] = GG_df.place_lon
GG_df.drop(columns=['_merge','place_lat','place_lon'], inplace=True)
logging.debug(f"dest_geo_level:\n{GG_df.dest_geo_level.value_counts(dropna=False)}")

# ==== Home zip code ===
GG_df['Home_Zipcode'] = GG_df.Home_Zipcode.astype(str)
logging.debug(f"Home_Zipcode value_counts().head():\n{GG_df.Home_Zipcode.value_counts(dropna=False).head(20)}")
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
zip_centroid_coords["Home_Zipcode"] = zip_gdf.GEOID20
zip_centroid_coords.rename(columns={"x":"home_lon", "y":"home_lat"}, inplace=True)
logging.debug(f"zip_centroid_coords.head():\n{zip_centroid_coords.head()}")

# release this
del zip_gdf
# join snapshot_df to zip_centroid_coords
GG_df = pd.merge(
    left=GG_df,
    right=zip_centroid_coords,
    how='left',
    on='Home_Zipcode',
    indicator=True)
logging.debug(f"Zip_Code join results\n{GG_df._merge.value_counts(dropna=False)}")
GG_df["home_geo_level"] = None
GG_df.loc[GG_df._merge == 'both', "home_geo_level"] = "zip"
GG_df.drop(columns=['_merge'], inplace=True)

# ================ 02 Access and Egress Modes ================
ACCESS_EGRESS_RECODE = {
    1   : "walk",    # Walked all the way
    2   : "bike",    # Bike
    3   : "pnr",     # Drove (park and ride)
    4   : "knr",     # Dropped off/Picked up by car
    5   : "transit", # Golden Gate Ferry
    6   : "transit", # BART
    7   : "transit", # Muni
    8   : "transit", # Another Golden Gate Bus
    9   : "transit", # Marin Transit / West Marin Stage
    10  : "transit", # SMART Train
    11  : "other",   # Other (Unspecified)
    12  : "transit", # AC Transit
    13  : "transit", # SOLTrans
    14  : "tnc",     # Uber/Lyft or similar
    15  : "transit", # Sonoma County Transit
    16  : "transit", # Caltrain
    17  : "transit", # Petaluma Transit
    18  : "transit", # WestCat
    19  : "transit", # Shuttle
    20  : "transit", # Santa Rosa CityBus
    21  : "transit", # VTA
    22  : "transit", # Mendocino Transit
    23  : "transit", # SamTrans
}
for acc_egr_col in ['Access_1','Access_2','Access_3','Access_4','Egress_1','Egress_2','Egress_3','Egress_4']:
    GG_df[f"{acc_egr_col}_recode"] = GG_df[acc_egr_col].map(ACCESS_EGRESS_RECODE)
logging.debug(f"Access modes:\n{GG_df[['Access_1_recode','Access_2_recode','Access_3_recode','Access_4_recode']].value_counts(dropna=False)}")
logging.debug(f"Access modes:\n{GG_df[['Egress_1_recode','Egress_2_recode','Egress_3_recode','Egress_4_recode']].value_counts(dropna=False)}")

# ================ 08 Person Demographics ================
logging.debug(f"gender value_counts:\n{GG_df.gender.value_counts()}")

RACE_CODE = {
    1: "white",    # Caucasian/White
    2: "hispanic", # Hispanic/Latino
    3: "black",    # African American/Black
    4: "asian",    # Asian/Pacific Islander
    5: "Native American",
    6: "other",    # Other (Unspecified)
}
for race_col in ['race_1','race_2','race_3','race_4']:
    GG_df[race_col] = GG_df[race_col].map(RACE_CODE)
logging.debug(f"race cols:\n{GG_df[['race_1','race_2','race_3','race_4']].value_counts(dropna=False)}")

GG_df["hispanic"] = None
GG_df.loc[pd.notna(GG_df.race_1) | 
          pd.notna(GG_df.race_2) | 
          pd.notna(GG_df.race_3) | 
          pd.notna(GG_df.race_4), "hispanic"] = 0
GG_df.loc[(GG_df.race_1 == "hispanic") | 
          (GG_df.race_2 == "hispanic") | 
          (GG_df.race_3 == "hispanic") | 
          (GG_df.race_4 == "hispanic"), "hispanic"] = 1

GG_df["race_dmy_asn"] = None
GG_df.loc[pd.notna(GG_df.race_1) | 
          pd.notna(GG_df.race_2) | 
          pd.notna(GG_df.race_3) | 
          pd.notna(GG_df.race_4), "race_dmy_asn"] = 0
GG_df.loc[(GG_df.race_1 == "asian") | 
          (GG_df.race_2 == "asian") | 
          (GG_df.race_3 == "asian") | 
          (GG_df.race_4 == "asian"), "race_dmy_asn"] = 1

GG_df["race_dmy_blk"] = None
GG_df.loc[pd.notna(GG_df.race_1) | 
          pd.notna(GG_df.race_2) | 
          pd.notna(GG_df.race_3) | 
          pd.notna(GG_df.race_4), "race_dmy_blk"] = 0
GG_df.loc[(GG_df.race_1 == "black") | 
          (GG_df.race_2 == "black") | 
          (GG_df.race_3 == "black") | 
          (GG_df.race_4 == "black"), "race_dmy_blk"] = 1

GG_df["race_dmy_ind"] = None
GG_df.loc[pd.notna(GG_df.race_1) | 
          pd.notna(GG_df.race_2) | 
          pd.notna(GG_df.race_3) | 
          pd.notna(GG_df.race_4), "race_dmy_ind"] = 0
GG_df.loc[(GG_df.race_1 == "Native American") | 
          (GG_df.race_2 == "Native American") | 
          (GG_df.race_3 == "Native American") | 
          (GG_df.race_4 == "Native American"), "race_dmy_ind"] = 1

GG_df["race_dmy_wht"] = None
GG_df.loc[pd.notna(GG_df.race_1) | 
          pd.notna(GG_df.race_2) | 
          pd.notna(GG_df.race_3) | 
          pd.notna(GG_df.race_4), "race_dmy_wht"] = 0
GG_df.loc[(GG_df.race_1 == "white") | 
          (GG_df.race_2 == "white") | 
          (GG_df.race_3 == "white") | 
          (GG_df.race_4 == "white"), "race_dmy_wht"] = 1

logging.debug(f"race cols:\n{GG_df[['race_1','race_2','race_3','race_4','hispanic','race_dmy_asn','race_dmy_blk','race_dmy_ind','race_dmy_wht']].value_counts(dropna=False)}")

AGE_CAT_TO_YEAR_BORN = {
    1: 2007, # Under 18  [15.5], 2023-16 = 2007
    2: 2001, # 18-24     [21.5], 2023-22 = 2001
    3: 1993, # 25-34     [29.5], 2023-30 = 1993
    4: 1983, # 35-44     [39.5], 2023-40 = 1983
    5: 1973, # 45-54     [49.5], 2023-50 = 1973
    6: 1963, # 55-64     [59.5], 2023-60 = 1963
    7: 1953, # 65 or older [70], 2023-70 = 1953
}
GG_df["year_born_four_digit"] = GG_df.age_cat.map(AGE_CAT_TO_YEAR_BORN)
logging.debug(GG_df[["age_cat","year_born_four_digit"]].value_counts(dropna=False))

# ================ 09 Household Demographics ================
LANGUAGE_AT_HOME = {
    1   : "English",
    2   : "Spanish",
    3   : "Chinese",
    4   : "Other", # (Unspecified)
    5   : "Arabic",
    6   : "Armenian",
    7   : "Catalan",
    8   : "Danish",
    9   : "Dutch",
    10  : "Farsi",
    11  : "Finnish",
    12  : "French",
    13  : "German",
    14  : "Greek",
    15  : "Hindi",
    16  : "Hungarian",
    17  : "Irish Gaelic",
    18  : "Italian",
    19  : "Japanese",
    20  : "Korean",
    21  : "Luxembourgish",
    22  : "Malay",
    23  : "Norwegian",
    24  : "Portuguese",
    25  : "Russian",
    26  : "Swedish",
    27  : "Tagalog",
    28  : "Thai",
    29  : "Turkish",
    30  : "Ukrainian",
    31  : "Vietnamese"
}
for lang_col in ['language_at_home_1','language_at_home_2','language_at_home_3','language_at_home_4']:
    GG_df[lang_col] = GG_df[lang_col].map(LANGUAGE_AT_HOME)
logging.debug(f"language_at_home:\n{GG_df[['language_at_home_1','language_at_home_2','language_at_home_3','language_at_home_4',]].value_counts(dropna=False)}")

GG_df["language_at_home_binary"] = None
GG_df.loc[ (GG_df.language_at_home_1=="English") & 
           pd.isna(GG_df.language_at_home_2) &
           pd.isna(GG_df.language_at_home_3) &
           pd.isna(GG_df.language_at_home_4),  "language_at_home_binary"] = "ENLISH ONLY"
# if any language is specified and it's nost just english, mark as other
GG_df.loc[ pd.notna(GG_df.language_at_home_1) &
           (GG_df.language_at_home_1!="English"), "language_at_home_binary"] = "OTHER"
# if a single language is specified, mark as language_at_home_detail
GG_df.loc[ pd.notna(GG_df.language_at_home_1) &
           pd.isna(GG_df.language_at_home_2) &
           pd.isna(GG_df.language_at_home_3) &
           pd.isna(GG_df.language_at_home_4), "language_at_home_detail"] = GG_df.language_at_home_1.str.upper()

logging.debug(f"language_at_home:\n{GG_df[['language_at_home_1','language_at_home_2','language_at_home_3','language_at_home_4','language_at_home_binary','language_at_home_detail']].value_counts(dropna=False)}")
logging.debug(f"language_at_home_detail:\n{sorted(GG_df['language_at_home_detail'].dropna().unique())}")

# ================ 10 Survey Metadata ================
# ID - just create this; the available options don't seem to work
# verify it's unique and always set
GG_df["ID"] = range(1, len(GG_df)+1)
assert(len(GG_df.ID.unique()) == len(GG_df))
assert(len(GG_df.loc[ pd.isna(GG_df.ID)]) == 0)

# survey_type
logging.debug(f"\n{GG_df.Source.value_counts(dropna=False)=}")

# date_string
# ferry surveying from 1=Thursday, June 1, 2023 to 83=Friday, October 6, 2023
FERRY_DATE_RANGE = pd.concat([
    pd.date_range(start="2023-06-01", end="2023-07-16", freq="1D").to_series(index=range(1,47), name='interview_date'),
    pd.date_range(start="2023-08-01", end="2023-08-01", freq="1D").to_series(index=[47],        name='interview_date'),
    pd.date_range(start="2023-09-01", end="2023-10-06", freq="1D").to_series(index=range(48,84),name='interview_date')
]).to_frame()
FERRY_DATE_RANGE["sub_survey"] = "ferry"
FERRY_DATE_RANGE["IntDate"] = FERRY_DATE_RANGE.index
# logging.debug(f"FERRY_DATE_RANGE: len={len(FERRY_DATE_RANGE)}\n{FERRY_DATE_RANGE}")

GGT_DATE_RANGE = pd.concat([
    pd.date_range(start="2023-04-17", end="2023-06-30", freq="1D").to_series(index=range(1,76),   name='interview_date'),
    pd.date_range(start="2023-09-01", end="2023-10-06", freq="1D").to_series(index=range(76,112), name='interview_date'),
]).to_frame()
GGT_DATE_RANGE["sub_survey"] = "ggt"
GGT_DATE_RANGE["IntDate"] = GGT_DATE_RANGE.index
logging.debug(f"GGT_DATE_RANGE: len={len(GGT_DATE_RANGE)}\n{GGT_DATE_RANGE}")

INT_DATE_TO_DATE = pd.concat([FERRY_DATE_RANGE, GGT_DATE_RANGE])
# logging.debug(f"INT_DATE_TO_DATE: len={len(INT_DATE_TO_DATE)}\n{INT_DATE_TO_DATE}")
GG_df = pd.merge(
    left=GG_df,
    right=INT_DATE_TO_DATE,
    on=['sub_survey','IntDate'],
    how='left',
    indicator=True,
    validate='many_to_one'
)
logging.debug(f'IntDate merge:\n{GG_df._merge.value_counts()}')
GG_df.drop(columns=['_merge'], inplace=True)
logging.debug(f'Strata:\n{GG_df.Strata.value_counts()}')

# add weighting, using imported marginal values

# read the total ridership data
ridership_df = pd.read_excel(
        io=GG_ridership_xlsx,
        sheet_name="Ridership"
    )

# function to distribute ridership totals over valid survey records

def distribute_ridership(survey, ridership): 
    # Check if necessary columns exist in both files
    if 'Route' not in survey.columns or 'Strata' not in survey.columns:
        raise ValueError("The survey file must have 'Route' and 'Strata' columns.")
    if 'Route' not in ridership.columns or 'Weekday' not in ridership.columns or 'Weekend' not in ridership.columns:
        raise ValueError("The ridership file must have 'Route', 'Weekday', and 'Weekend' columns.")
    
    # Merge the survey data with ridership data based on the 'Route' column
    merged_df = pd.merge(survey, ridership, on='Route', how='left')
    
    # Mapping from more detailed strata to collapsed strata
    strata_mapping = {
        'AM OFF': 'Weekday',
        'AM PEAK': 'Weekday',
        'EVENING': 'Weekday',
        'MIDDAY': 'Weekday',
        'PM PEAK': 'Weekday',
        'SAT': 'Weekend',
        'SUN': 'Weekend'
    }
    
    # add a new column for distributed ridership, initially zero
    merged_df['weight'] = 0
    
    # Set ridership to zero for 'Event' and 'Giants' routes
    merged_df.loc[merged_df['Route'].isin(['LARKSPUR - EVENT', 'LARKSPUR - GIANTS']), 'weight'] = 0
    
    # Distribute ridership based on the collapsed strata and ridership totals
    for route in merged_df['Route'].unique():
        if route in ['LARKSPUR - EVENT', 'LARKSPUR - GIANTS']:
            continue  # Skip "Event" and "Giants" routes, as their ridership is already set to zero
        
        for strata, ridership_col in strata_mapping.items():
            # Filter rows for the current route and the strata category
            route_strata_records = merged_df[(merged_df['Route'] == route) & (merged_df['Strata'] == strata)]
            num_records = len(route_strata_records)
            
            if num_records == 0:
                print(f"No records found for route {route} in {strata}.")
                continue
            
            # Get the ridership total for the route and strata (weekday/weekend)
            total_ridership = merged_df.loc[(merged_df['Route'] == route), ridership_col].iloc[0]
            
            # Calculate even distribution of ridership for each record
            ridership_per_record = total_ridership / num_records if num_records > 0 else 0
            
            # Assign the calculated ridership to each record for this route and strata
            merged_df.loc[(merged_df['Route'] == route) & (merged_df['Strata'] == strata), 'weight'] = ridership_per_record
    
    # round the 'weight' column to 2 decimal places
    merged_df['weight'] = merged_df['weight'].round(2)

    # Return the updated DataFrame
    return merged_df

# run function and output final file
GG_df = distribute_ridership(GG_df, ridership_df)

# save to CSV
GG_csv = GG_dir / "GoldenGate_Transit_Ferry_preprocessed.csv"
GG_df[KEEP_COLUMNS].to_csv(GG_csv, index=False)
print(f"Saved {len(GG_df):,} rows to {GG_csv}")
