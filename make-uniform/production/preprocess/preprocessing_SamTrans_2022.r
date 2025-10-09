# preprocessing_SamTrans_2022.r
# SI

# Libraries
suppressMessages(library(tidyverse))
library(readxl)

data_in <- "M:/Data/OnBoard/Data and Reports/SamTrans/2019_2022/SamTrans 2022 SamTrans Data for report and tables.xlsx"
wincross <- read_xlsx(data_in,sheet = "Wincross Data")
trip_diary <- read_xlsx(data_in,sheet = "Trip Diary")

samtrans <- left_join(wincross,trip_diary,by="CCGID") %>% 
  select(-sys_RespNum.x,-sys_RespNum.y)

# Bring in data file

# suppress scientific notation
options(scipen = 999)

KEEP_COLUMNS = c(
  "canonical_operator",      # SAMTRANS
  "survey_tech",             # from Route
  "CCGID",                   # ID assigned by CCG
  # 01 Geocoded Location Data
  "Start_lat",               # Origin lat
  "Start_lon",               # Origin lon
  "End_lat",                 # Destination lat
  "End_lon",                 # Destination lon
  "survey_board_lat",        # Survey board lat
  "survey_board_lon",        # Survey board lon
  "survey_alight_lat",       # Survey alight lat
  "survey_alight_lon",       # Survey alight lon
  "first_board_lat",         # First transit boarding lat
  "first_board_lon",         # First transit boarding lon
  "last_alight_lat",         # Last transit alighting lat
  "last_alight_lon",         # Last transit alighting lat
  "home_lat",                # Home lat
  "home_lon",                # Home lon
  "work_lat",                # Work lat
  "work_lon",                # Work lon
  "school_lat",              # School lat
  "school_lon",              # School lon
  # 02 Access and Egress Modes
  "GetFirstBus",             # access mode 
  "FromLastBus",             # egress mode 
  # 03 Transit Transfers
  "first_route_before_survey_board",
  # 04 Origin and Destination Trip Purpose
  "From",                    # orig_purp
  "To",                      # dest_purp
  # 05 Time Leaving and Returning Home 
  "depart_hour",             # Hour departing home
  "return_hour",             # Hour returning home
  # 06 Fare Payment
  "fare",                    # fare_medium
  "farecat",                 # fare_category
  # 07 Half Tour Questions for Work and School - no data
  # 08 Person Demographics
  "eng_proficient",          # from Q15/Q17
  "gender",                  # from Q16/Q18
  "hispanic",                # Based on Q17/Q19_[1234]
  "race_dmy_asn",            # Race: Asian from Q17/Q19_[1234]
  "race_dmy_blk",            # Race: Black from Q17/Q19_[1234]
  "race_dmy_ind",            # Race: American indian Q17/Q19_[1234]
  "race_dmy_wht",            # Race: White from Q17/Q19_[1234]
  "race_other_string",       # Race: Other from Q17/Q19_[1234]
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
)
