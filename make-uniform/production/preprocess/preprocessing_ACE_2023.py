#
# preprocess ACE dataset to raw input for Build_Standard_Database.R
#
import pathlib
import pandas as pd

pd.options.display.max_rows = 999

KEEP_COLUMNS = [
    # 01 Geocoded Location Data
    "d_lat",
    "d_lng",
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
    "lang",
    "hh_size",
    "hh_emp",
    "veh",
    "income",
    # 10 Survey Metadata
    "id",
    "weight",
    "survey_lang",
    "board",
    "alight"
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
    7: 1950, # 62-64, 2023-63 = 1960
    8: 1943, # 65+, 2023-70 = 
}
ACE_data_df["year_born_four_digit"] = ACE_data_df["age"].map(age_cat_to_year_born)
# save to csv
ACE_csv = ACE_dir / "ACE_Onboard_preprocessed.csv"
ACE_data_df[KEEP_COLUMNS].to_csv(ACE_csv)
print(f"Saved {ACE_csv}")