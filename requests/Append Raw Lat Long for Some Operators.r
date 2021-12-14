# Append Raw Lat Long for Some Operators.r
# Append lat/long for origin, first boarding, last alighting, survey board, survey alight, destination

# Import Library

suppressMessages(library(tidyverse))
library(geosphere)

# Input TPS

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
load (TPS_SURVEY_IN)

# Bring in raw Caltrain data

dir_path <- "M:/Data/OnBoard/Data and Reports/"

f_bart_survey_path <- paste0(dir_path,
                             "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")
f_caltrain_survey_path <- paste0(dir_path,
                                 "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE_modifyTransfer_fixRouteNames_NO POUND OR SINGLE QUOTE.csv")
f_ggtransit_survey_path <- paste0(dir_path,
                                  "Golden Gate Transit/2018/As CSV/20180907_OD_GoldenGate_allDays_addCols_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")
f_weta_survey_path <- paste0(dir_path,
                             "WETA/WETA 2018/As CSV/WETA-Final Weighted Data-Standardized_addCols_NO POUND OR SINGLE QUOTE.csv")
f_actransit_survey_path <- paste0(dir_path,
                                  "AC Transit/2018/As CSV/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_fixTransfers_NO POUND OR SINGLE QUOTE.csv")
f_muni_survey_path <- paste0(dir_path,
                             "Muni/As CSV/MUNI_DRAFTFINAL_20171114_fixedTransferNum_NO POUND OR SINGLE QUOTE.csv")
f_vta_survey_path <- paste0(dir_path,
                            "VTA/As CSV/VTA_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")


BART_raw            <- read.csv(f_bart_survey_path,header = T) %>% 
  mutate(operator="BART") %>% 
  select(operator,
         ID=ID,
         origin_lon        =OR_ADDRESS_LONG,
         origin_lat        =OR_ADDRESS_LAT,
         first_board_lon   =FIRST_BOARD_LON,
         first_board_lat   =FIRST_BOARD_LAT,
         survey_board_lon  =FIRST_ENTER_BART_LON,
         survey_board_lat  =FIRST_ENTER_BART_LAT,
         survey_alight_lon =BART_EXIT_LON,
         survey_alight_lat =BART_EXIT_LAT,
         last_alight_lon   =LAST_ALIGHT_LON,
         last_alight_lat   =LAST_ALIGHT_LAT,
         destination_lon   =DE_ADDRESS_LONG,
         destination_lat   =DE_ADDRESS_LAT
         )
caltrain_raw        <- read.csv(f_caltrain_survey_path,header = T) %>% 
  mutate(operator="Caltrain") %>% 
  select(operator,
         ID=ID,
         origin_lon        =ORIGIN_LON,
         origin_lat        =ORIGIN_LAT,
         first_board_lon   =X1stACCESS_LON,
         first_board_lat   =X1stACCESS_LAT,
         survey_board_lon  =ENTER_STATION_LON,
         survey_board_lat  =ENTER_STATION_LAT,
         survey_alight_lon =EXIT_STATION_LON,
         survey_alight_lat =EXIT_STATION_LAT,
         last_alight_lon   =LastEGRESS_LON,
         last_alight_lat   =LastEGRESS_LAT,
         destination_lon   =DESTINATION_LON,
         destination_lat   =DESTINATION_LAT
  )
ggtransit_raw       <- read.csv(f_ggtransit_survey_path,header = T) %>% 
  mutate(operator="Golden Gate Transit") %>% 
  select(operator,
         ID=id,
         origin_lon        =final_orig_lon,
         origin_lat        =final_orig_lat,
         first_board_lon   =final_first_boarding_lon,
         first_board_lat   =final_first_boarding_lat,
         survey_board_lon  =final_survey_board_lon,
         survey_board_lat  =final_survey_board_lat,
         survey_alight_lon =final_survey_alight_lon,
         survey_alight_lat =final_survey_alight_lat,
         last_alight_lon   =final_last_alighting_lon,
         last_alight_lat   =final_last_alighting_lat,
         destination_lon   =final_dest_lon,
         destination_lat   =final_dest_lat
  )
weta_raw            <- read.csv(f_weta_survey_path,header = T) %>% 
  mutate(operator="WETA") %>% 
  select(operator,
         ID=id,
         origin_lon        =orig_lon,
         origin_lat        =orig_lat,
         first_board_lon   =first_board_lon,
         first_board_lat   =first_board_lat,
         survey_board_lon  =survey_board_lon,
         survey_board_lat  =survey_board_lat,
         survey_alight_lon =survey_alight_lon,
         survey_alight_lat =survey_alight_lat,
         last_alight_lon   =last_alight_lon,
         last_alight_lat   =last_alight_lat,
         destination_lon   =dest_lon,
         destination_lat   =dest_lon
  )
actransit_raw       <- read.csv(f_actransit_survey_path,header = T)
muni_raw            <- read.csv(f_muni_survey_path,header = T)
vta_raw             <- read.csv(f_vta_survey_path,header = T)

# Output location

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
BOX_TM               <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
Output               <- file.path(BOX_TM,"Share Data","Protected Data","Joel Freedman")

# Remove MAZ-level variables

final <- TPS %>% 
  select(!grep("maz",names(TPS),ignore.case = T))

write.csv(final, file.path(Output,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZ_Only.csv"), row.names = FALSE, quote = T)


 