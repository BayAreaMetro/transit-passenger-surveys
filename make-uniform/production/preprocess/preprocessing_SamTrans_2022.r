# preprocessing_SamTrans_2022.r
# SI

# Libraries
suppressMessages(library(tidyverse))
library(readxl)

# Locations
data_in  <- "M:/Data/OnBoard/Data and Reports/SamTrans/2019_2022/SamTrans 2022 SamTrans Data for report and tables.xlsx"
data_out <- "M:/Data/OnBoard/Data and Reports/SamTrans/2019_2022/SamTrans_2022_preprocessed.csv"

# Bring in data that is saved across two spreadsheets (with a shared unique ID), join
wincross <- read_xlsx(data_in,sheet = "Wincross Data")
trip_diary <- read_xlsx(data_in,sheet = "Trip Diary")

samtrans <- left_join(wincross,trip_diary,by="CCGID") %>% 
  select(-sys_RespNum.x,-sys_RespNum.y)

# suppress scientific notation
options(scipen = 999)

# Identify files to keep

KEEP_COLUMNS = c(
  "CCGID",                   # ID assigned by CCG
  # 01 Geocoded Location Data
  "orig_lat",                # Origin lat
  "orig_lon",                # Origin lon
  "dest_lat",                # Destination lat
  "dest_lon",                # Destination lon
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
  "WorkLat",                 # Work lat
  "WorkLong",                # Work lon
  "school_lat",              # School lat
  "school_lon",              # School lon
  # 02 Access and Egress Modes
  "GetFirstBus",             # Access mode 
  "FromLastBus",             # Egress mode 
  # 03 Transit Transfers
  "number_transfers_orig_board",      # Number of transfers before boarding survey vehicle
  "number_transfers_alight_dest",     # Number of transfers after alighting survey vehicle
  "first_route_before_survey_board",  # First route transfer before survey board 
  "second_route_before_survey_board", # Second route transfer before survey board
  "third_route_before_survey_board",  # Third route transfer before survey board
  "first_route_after_survey_alight",  # First route transfer after survey alight
  "second_route_after_survey_alight", # Second route transfer after survey alight
  "third_route_after_survey_alight",  # Third route transfer after survey alight
  # 04 Origin and Destination Trip Purpose
  "From",                    # Origin purpose
  "To",                      # Destination purpose
  # 05 Time Leaving and Returning Home 
  "depart_hour",             # Hour departing home
  "return_hour",             # Hour returning home
  # 06 Fare Payment
  "fare",                    # Fare medium
  "farecat",                 # Fare category
  # 07 Half Tour Questions for Work and School - no data
  "workbefore",              # Work before trip
  "workafter",               # Work after trip
  "schbefore",               # School before trip
  "schafter",                # School after trip
  # 08 Person Demographics
  "engspk",                  # English proficiency
  "gender",                  # Gender
  "hisp",                    # Hispanic/Latino
  "race_dmy_ind",            # Race: American indian from ETH1-4
  "race_dmy_hwi",            # Race: Native Hawaiian/Pacific Islander from ETH1-4
  "race_dmy_blk",            # Race: Black from ETH1-4
  "race_dmy_wht",            # Race: White from ETH1-4
  "race_dmy_asn",            # Race: Asian from ETH1-4
  "race_other_string",       # Race: other or mixed from ETH1-4
  "birthyear",               # Year born
  "work",                    # Employment status
  # 09 Household Demographics
  "hh",                      # Persons
  "hhwork",                  # Household workers
  "language_at_home_binary", # From langhh
  "langhh",                  # Language spoken at home
  "income",                  # Household income
  "cars",                    # Household vehicles
  # 10 Survey Metadata
  "Route",                   # Survey route
  "Dir",                     # Survey route direction
  "Mode",                    # Survey type
  "Lang",                    # Interview language
  "date",                    # Date trip occurred
  "Strata",                  # Time period of survey
  "interview_end_time",      # End of survey time
  "weight",                  # Weight
  "DTYPE"                    # Weekpart
)

# Concatenate transfer operators and transfer routes into a single column
samtrans <- samtrans %>%
  mutate(
    first_route_before_survey_board = str_c(first_system_before_survey_board, first_route_before_survey_board, sep = " ") %>% str_squish(),
    second_route_before_survey_board = str_c(second_system_before_survey_board, second_route_before_survey_board, sep = " ") %>% str_squish(),
    third_route_before_survey_board = str_c(second_system_before_survey_board, third_route_before_survey_board, sep = " ") %>% str_squish(),
    first_route_after_survey_alight = str_c(first_system_after_survey_alight, first_route_after_survey_alight, sep = " ") %>% str_squish(),
    second_route_after_survey_alight = str_c(second_system_after_survey_alight, second_route_after_survey_alight, sep = " ") %>% str_squish(),
    third_route_after_survey_alight = str_c(third_system_after_survey_alight, third_route_after_survey_alight, sep = " ") %>% str_squish()
  )

# Fix the race/ethnicity coding to match the standard survey pattern
samtrans <- samtrans %>%
  mutate(
    race_dmy_ind = as.integer(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 1), FALSE)),
    race_dmy_hwi = as.integer(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 2), FALSE)),
    race_dmy_blk = as.integer(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 3), FALSE)),
    race_dmy_wht = as.integer(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 4), FALSE)),
    race_dmy_asn = as.integer(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 5), FALSE)),
    race_other_string = if_else(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 6), FALSE),"other",""),
    # Removing 7 for Hispanic as that is handled in a separate variable
    race_other_string = if_else(coalesce(if_any(all_of(c("ETH1", "ETH2", "ETH3", "ETH4")), ~ .x == 8), FALSE),"multiracial","")
  )

# Language at home binary
samtrans <- samtrans %>% 
  mutate(language_at_home_binary=if_else(langhh==1,"No","Yes"))

# Set NA transfers to 0 for both boarding and alighting side
samtrans <- samtrans %>%
  mutate(number_transfers_orig_board = coalesce(number_transfers_orig_board, 0),
         number_transfers_alight_dest = coalesce(number_transfers_alight_dest, 0))

# Remove single quotes and # from file, select just the keep columns, define above
samtrans <- samtrans %>%
  mutate(across(everything(),~ str_replace_all(as.character(.x), "['#]", "")
  )) %>% 
  select(all_of(KEEP_COLUMNS))

# Export file

write.csv(samtrans,data_out,row.names = F)
