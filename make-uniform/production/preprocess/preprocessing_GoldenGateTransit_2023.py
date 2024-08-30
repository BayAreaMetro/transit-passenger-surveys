#
# preprocess Goldeng Gate Transit/Ferry dataset to raw input for Build_Standard_Database.R
#
import logging
import pathlib
import pandas as pd

pd.options.display.max_rows = 999
logger = logging.getLogger("survey_preprocessor")

KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    # 02 Access and Egress Modes
    # 03 Transit Transfers - no data
    # 04 Origin and Destination Trip Purpose - we only have trip purpose
    # 05 Time Leaving and Returning Home - no data
    # 06 Fare Payment
    # 07 Half Tour Questions for Work and School - no data
    # 08 Person Demographics
    # 09 Household Demographics
    # 10 Survey Metadata
    "ID",               # ID, based on rownum (ferry, then GGT)
    "Source",           # survey_type
    "Lang",             # interview_language
    "interview_date",   # from IntDate
    "Strata",           # time_period
    # "survey_tech",      # from survey dataset and Route
]
GG_dir = pathlib.Path("M:\Data\OnBoard\Data and Reports\Golden Gate Transit\\2023")
GG_ferry_xlsx = GG_dir / "GGFerry2023 Final Data.xlsx"
GG_transit_xlsx = GG_dir / "GGT2023 Final Data.xlsx"

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
logging.debug(f"GG_ferry_df:\n{GG_ferry_df.head()}")

GG_transit_df = pd.read_excel(
    io=GG_transit_xlsx,
    sheet_name="Data"
)
logging.info(f"Read {len(GG_transit_df):,} lines from {GG_transit_xlsx}")
GG_transit_df = GG_transit_df.add_prefix("ggt_")
GG_transit_df.insert(loc=0, column="sub_survey", value="ggt")
 # TODO: distinguish between local and express bus
logging.debug(f"GG_transit_df:\n{GG_transit_df.head()}")

# explicitly make these the same
GG_ferry_df.rename(columns={
    'ferry_sys_RespNum' : 'sys_RespNum',
    'ferry_CCGID'       : 'CCGID',
    'ferry_Source'      : 'Source',
    'ferry_Lang'        : 'Lang',
    'ferry_RUNID'       : 'RUNID',
    'ferry_Route'       : 'Route',
    'ferry_IntDate'     : 'IntDate',
    'ferry_Strata'      : 'Strata',
}, inplace=True)
GG_transit_df.rename(columns={
    'ggt_sys_RespNum'   : 'sys_RespNum',
    'ggt_CCGID'         : 'CCGID',
    'ggt_SOURCE'        : 'Source',
    'ggt_LANG'          : 'Lang',
    'ggt_RUNID'         : 'RUNID',
    'ggt_Route'         : 'Route',
    'ggt_IntDate'       : 'IntDate',
    'ggt_Strata'        : 'Strata',
}, inplace=True)

# Put them together
GG_df = pd.concat([GG_ferry_df, GG_transit_df]).reset_index(drop=True)
logging.debug(f"GG_df.head():\n{GG_df.head()}")
logging.debug(f"GG_df.dtypes\n{GG_df.dtypes}")
del GG_ferry_df 
del GG_transit_df

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
    right=GGT_DATE_RANGE,
    on=['sub_survey','IntDate'],
    indicator=True,
    validate='many_to_one'
)
logging.debug(f'IntDate merge:\n{GG_df._merge.value_counts()}')
GG_df.drop(columns=['_merge'], inplace=True)
logging.debug(f'Strata:\n{GG_df.Strata.value_counts()}')

# todo: handle Run ID, Route, Dir

age_cat_to_year_born = {
    1: 2008, # Under 18, 2023-15 = 2008
    2: 2002, # 18-24, 2023-21 = 2002
    3: 1993, # 25-34, 2023-30 = 1993
    4: 1983, # 35-44, 2023-40 = 1983
    5: 1973, # 45-54, 2023-50 = 1973
    6: 1965, # 55-61, 2023-58 = 1965
    7: 1950, # 62-64, 2023-63 = 1960
    8: 1943, # 65+, 2023-70 = 
}
# GG_ferry_df["year_born_four_digit"] = GG_ferry_df["age"].map(age_cat_to_year_born)
# save to csv
GG_csv = GG_dir / "GoldenGate_Transit_Ferry_preprocessed.csv"
GG_df[KEEP_COLUMNS].to_csv(GG_csv, index=False)
print(f"Saved {len(GG_df):,} rows to {GG_csv}")