##################################################################################################
### Script to create a database of canonical route and station names
### Author: John Helsel, October 2018
##################################################################################################

# Libraries and optins
library(tidyverse)
library(rlang)
library(sf)
library(geosphere)

options(stringsAsFactors = FALSE)

# User check to assign proper paths for input data and writes

user_list <- data.frame(
  
  user = c("helseljw", 
           "John Helsel",
           "USDO225024"), 
  path = c("../../Data and Reports/", 
           "../../Data and Reports/", 
           "~/GitHub/onboard-surveys/Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

# Crosswalk Paths

get_rail_names_inputs_path <- "get_rail_names_inputs.csv"

# Input data paths
ac_transit_path <- paste0(dir_path,
  "AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_ADD_STANDARD_VARS.csv")

bart_path <- paste0(dir_path,
  "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")

caltrain_path <- paste0(dir_path, 
  "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE NO POUND OR SINGLE QUOTE.csv")

# marin_path <- paste0(dir_path,
#   "Marin Transit/Final Data/marin transit_data file_finalreweighted043018.csv")
  
sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

canonical_station_path <- paste0(dir_path,
  "Geography Files/Passenger_Railway_Stations_2018.shp")

canonical_route_path <- "../production/canonical_route_crosswalk.csv"

# Read crosswalk files
op_delim <- "___"

get_rail_names_inputs <- read.csv(get_rail_names_inputs_path)

# Read raw survey files
ac_transit_raw_df <- read.csv(ac_transit_path) %>% 
  rename_all(tolower)

bart_raw_df <- read.csv(bart_path) %>%
  rename_all(tolower) %>%
  # Rename typos in column name and standardize '_'
  rename(access_trnsfr_list1 = accesstrnsfr_list1,
         access_trnsfr_list2 = accesstrnsfr_list2,
         access_trnsfr_list3 = accesstrsnfr_list3,
         access_trnsfr_list1_imputed = access_trnsf_list1_imputed,
         access_trnsfr_list2_imputed = accesstrnsfr_list2_imputed,
         access_trnsfr_list3_imputed = accesstrnsfr_list3_imputed) %>%
  rename(egress_trnsfr_list3 = egresstrnsfr_list3,
         egress_trnsfr_list3_imputed = egresstransr_list3_imputed)
  
caltrain_raw_df <- read.csv(caltrain_path) %>%
  rename_all(tolower)

# marin_raw_df <- read.csv(marin_path) %>% 
#   rename_all(tolower)

sf_muni_raw_df <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

# Actual geocoding of rail station to station is now located in the "Build 
# Standard Database.Rmd" file. This recoding is only for the canonical database.

bart_raw_df <- bart_raw_df %>% 
  mutate(route = paste("BART", first_entered_bart, bart_exit_station, sep = op_delim)) %>%
  # Replace unknown records with imputed values
  mutate(access_trnsfr_list1 = ifelse(access_trnsfr_list1 == "Unknown", access_trnsfr_list1_imputed, access_trnsfr_list1),
         access_trnsfr_list2 = ifelse(access_trnsfr_list2 == "Unknown", access_trnsfr_list2_imputed, access_trnsfr_list2),
         access_trnsfr_list3 = ifelse(access_trnsfr_list3 == "Unknown", access_trnsfr_list3_imputed, access_trnsfr_list3),
         egress_trnsfr_list1 = ifelse(egress_trnsfr_list1 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list1),
         egress_trnsfr_list2 = ifelse(egress_trnsfr_list2 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list2),
         egress_trnsfr_list3 = ifelse(egress_trnsfr_list3 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list3)) %>% 
  # Replace Caltrain records with missing.
  mutate(access_trnsfr_list1 = ifelse(str_detect(access_trnsfr_list1, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), access_trnsfr_list1),
         access_trnsfr_list2 = ifelse(str_detect(access_trnsfr_list2, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), access_trnsfr_list2),
         access_trnsfr_list3 = ifelse(str_detect(access_trnsfr_list3, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), access_trnsfr_list3),
         egress_trnsfr_list1 = ifelse(str_detect(egress_trnsfr_list1, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), egress_trnsfr_list1),
         egress_trnsfr_list2 = ifelse(str_detect(egress_trnsfr_list2, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), egress_trnsfr_list2),
         egress_trnsfr_list3 = ifelse(str_detect(egress_trnsfr_list3, "Caltrain"), paste("CALTRAIN", "MISSING", "MISSING", sep = op_delim), egress_trnsfr_list3))

# Caltrain create internal Route
caltrain_raw_df <- caltrain_raw_df %>%
  mutate(route = paste("CALTRAIN", enter_station, exit_station, sep = op_delim))

# Begin building canonical database

# Adjust route names within AC Transit survey

transfer_names <- ac_transit_raw_df %>%
  select(matches("final_trip|final_route")) %>%
  select(-matches("code|stopid|other")) %>%
  colnames()

ac_transit_routes <- ac_transit_raw_df %>%
  select(one_of(transfer_names)) %>%
  gather(variable, value = survey_name) %>%
  filter(survey_name != "") %>% 
  unique() %>%
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "Other"), "Missing", canonical_operator)) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " */ *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
 
  mutate(canonical_name = str_replace(canonical_name, "^ROUTE ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^AC Transit [0-9A-Z]{1,5} ) ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^AC Transit ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[TO.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[CLOCKWISE.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[COUNTERCLOCKWISE.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "12 Dtn. Oakland Dtn. Berkeley 4th St. Harrison", "12 Berkeley BART to Downtown Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "14 W Oakland Bart Fruitval Bart", "14 Downtown Oakland to Fruitvale BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "18 San Pablo & Monroe Berkeley Merritt BART", "18 University Village Albany to Montclair")) %>%
  mutate(canonical_name = str_replace(canonical_name, "20 Dimond District Oakland to downtown Oakland", "20 Fruitvale Ave Alameda 11th MLK Jr Wy")) %>%
  mutate(canonical_name = str_replace(canonical_name, "21 Fruitvale Ave Alameda Oakland Airport", "21 Dimond Dist to Oakland Airport")) %>%
  mutate(canonical_name = str_replace(canonical_name, "210 Ohlone College to Union Landing Shopping Center", "210 Union Landing Frmt Blvd Ohlone")) %>%
  mutate(canonical_name = str_replace(canonical_name, "212 Fremont BART Newpark Mall Pacific Commons", "212 Fremont BART to NewPark Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "215 Fremont BART Mission Warm Springs Industrial Area", "215 Fremont BART to Gateway Blvd & Lakeside Pkwy")) %>%
  mutate(canonical_name = str_replace(canonical_name, "217 Frmt BART Mission Milpitas Alder", "217 Fremont BART to Great Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "22 Hayward Bart South Hayward Bart Chabot", "22 Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "232 Fremont BART UC BART Mission Ohlone Newark", "232 Fremont BART to New Park Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "239 Fremont BART Warm Springs", "239 Fremont BART to Warm Springs Blvd & Dixon Landing Rd")) %>%
  mutate(canonical_name = str_replace(canonical_name, "251 Fremont BART Mowry Thornton", "251 Fremont BART to NewPark Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "32 Hayward Bart CV Bart Hwd Bart Cherryland", "32 Hayward BART to Castro Valley BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "37 HWD BART WHITMAN SO HWD BART", "37 Hayward BART to South Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "376 North Richmond Shuttle", "376 El Cerrito Del Norte BART to Richmond Parkway Transit Center")) %>%
  mutate(canonical_name = str_replace(canonical_name, "39 Fruitvale Bart Skyline High School", "39 Fruitvale BART to Skyline High School")) %>%
  mutate(canonical_name = str_replace(canonical_name, "40 Dtn Oakland Eastmont TC Bayfair Bart", "40 Downtown Oakland to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "45 Eastmont T C Foothill Sq", "45 Eastmont Transit Center to Foothill Square Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "46 Coliseum Bart Knowland Zoo", "46 Coliseum BART to Oakland Zoo")) %>%
  mutate(canonical_name = str_replace(canonical_name, "46L Coliseum Bart Golf Links Rd Dunkirk Ave", "46L Coliseum BART Mountain Golf Links Rd Dunkirk Ave")) %>%
  mutate(canonical_name = str_replace(canonical_name, "47 Fruitvale Bart Maxwell Park Div 4", "47 Fruitvale BART to Maxwell Park")) %>%
  mutate(canonical_name = str_replace(canonical_name, "48 Bayfair BART Castro Valley Hayward bart", "48 Hayward BART to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "51A Oakland Alameda Fruitvale Bart", "51A Rockridge BART to Fruitvale BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "51B RockRidge 3RUN", "51B Rockridge BART to Berkeley Amtrak")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # 
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  # mutate(canonical_name = str_replace(canonical_name, "", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ROUTE "), "AC TRANSIT", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AC Transit"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^BART---", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^CALTRAIN---", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection (Route )*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "COUNTY CONNECTION", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "DHS"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "DUMBARTON EXPRESS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Emery"), "EMERYVILLE MTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairfield and Suisun Transit \\(FAST\\)"), "FAIRFIELD-SUISUN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Golden Gate"), "GOLDEN GATE TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "MUNI"), "MUNI", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo"), "PRESIDIGO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans (Route )?", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SamTrans"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Francisco Bay Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Francisco Bay Ferry "), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SolTrans"), "SOLTRANS", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Sierra Point"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Stanford Marguerite"), "Stanford Marguerite Shuttle", canonical_name)) %>%
    mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Tri Delta"), "TRI-DELTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UC Berkeley"), "UC BERKELEY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Union City"), "UNION CITY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "BERKELEY GATEWAY TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WESTCAT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

ac_transit_routes <- ac_transit_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "AC Transit",
         survey_year = 2013) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()
  
#Adjust route names within BART survey
transfer_names <- bart_raw_df %>%
  select_at(vars(contains("trnsfr"))) %>%
  select_at(vars(-contains("agency"))) %>%
  colnames()

bart_routes <- bart_raw_df %>% 
  select(one_of(transfer_names)) %>%
  gather(variable, value = survey_name) %>%
  filter(survey_name != "") %>%
  unique() %>%
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " */ *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "illogical"), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Missing"), "Missing", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AC Transit Route "), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^ACE.*", "ACE")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE "), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "AC TRANSIT", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alta Bates"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bayhill"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bishop Ranch"), "Bishop Ranch", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "North Burlingame shuttle", "North Burlingame Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain L[A-Z]* ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain (?=B)", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\(unspecified\\)", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^CALTRAIN___", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Bayhill Shuttle.*", "Bayhill San Bruno Shuttle")) %>% 
  mutate(canonical_name = str_replace(canonical_name, "(?<=Mariners Island ).*", "PCA Employer Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Childrens Hospital"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "COUNTY CONNECTION", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CPMC"), "CPMC", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Crocker Park"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CSU"), "CSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "DUMBARTON EXPRESS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Emery"), "EMERYVILLE MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Estuary Crossing"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairfield and Suisun Transit \\(FAST\\)"), "FAIRFIELD-SUISUN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairmont Hospital"), "Fairmont Hospital", canonical_operator)) %>%
           
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Foster City"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Golden Gate Transit"), "GOLDEN GATE TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Muni"), "MUNI", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni Route 55 16th St", "55 16th Street")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oshaughnessy", "OShaughnessy")) %>%
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Cable Car"), paste0(canonical_name, " Cable Car"), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni Cable Car ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni (Route )?", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " Historic Streetcar", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=Express )BUS ", "")) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=.{1,4}Light Rail:.{1,50}) {1,5}Metro", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=[A-Z]{1}) Light Rail: ", " ")) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay Shuttle"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin Transit Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Marin Transit"), "MARIN TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Mariners"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Monterey-Salinas Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Monterey-Salinas"), "Monterey-Salinas Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Oyster Point Shuttle \\(South SF\\)", "Oyster Point Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Oyster"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo"), "PRESIDIGO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Rio Vista Delta Breeze Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Rio Vista Delta"), "RIO-VISTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans (Route )?", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SamTrans"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "San Joaquin \\(Amtrak\\)", "San Joaquin")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Joaquin"), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Cruz Metro"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Rosa City[ ]?Bus Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Rosa City"), "Santa Rosa City", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Seton Medical"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFGH"), "SFGH", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SolTrans"), "SOLTRANS", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Sierra Point"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Stanford Marguerite"), "Stanford Marguerite Shuttle", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Tri Delta Transit"), "TRI-DELTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UC Berkeley"), "UC BERKELEY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Union City"), "UNION CITY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Utah Grand"), "Utah Grand", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route 29 ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "VINE"), "NAPA VINE", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA Route 902", "902 Light Rail")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^VTA.{0,20}):.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "53 West Valley College To Sunnyvale Transit Ctr", "53 Westgate Sunnyvale Ctrn")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VTA Route ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Downtown Area Shuttle", "Downtown Area Shuttle (Dash)")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "VTA", str_to_title(str_to_lower(canonical_name)), canonical_name)) %>% 
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WESTCAT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Yahoo"), "Yahoo", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "OTHER", canonical_operator))
  
bart_routes <- bart_routes %>%
  mutate(survey = "BART",
         survey_year = 2015) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()

# Adjust route names within Caltrain survey
caltrain_names <- caltrain_raw_df %>%
  select(route, matches("transfer_"), -matches("loc")) %>%
  colnames()

caltrain_routes <- caltrain_raw_df %>% 
  select(one_of(caltrain_names)) %>%
  gather(variable, value = survey_name) %>%
  filter(survey_name != "") %>%
  unique() %>% 
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " */ *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  
  
  mutate(canonical_name = str_replace_all(canonical_name, "AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AC Transit"), "AC TRANSIT", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Amtrak"), "AMTRAK", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "Angel Island.*", "Angel Island Tiburon Ferry")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^BART[A-Z ]* ", "BART___")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "(?<=BART[_A-Za-z /]{1,50}) [Tt]o ", op_delim)) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "BART___", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "^BART ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Bayview"), "Bayview", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Burlingame ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Burlingame"), "CALTRAIN", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^Caltrain SHUTTLE", "Shuttle")) %>% 
  mutate(canonical_name = str_replace_all(canonical_name, "^Caltrain ", "")) %>% 
  mutate(canonical_name = str_replace_all(canonical_name, "^CALTRAIN___", "")) %>% 
  mutate(canonical_name = str_replace_all(canonical_name, "So S", "South S")) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "CALTRAIN", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN"), "CALTRAIN", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN___"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^County Connection"), "COUNTY CONNECTION", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Dumbarton Express Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "DUMBARTON EXPRESS", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^Golden Gate Ferry [A-Z]* ", "")) %>%
  mutate(canonical_name = ifelse(str_detect(survey_name, "^Golden Gate Ferry"), paste(canonical_name, "Ferry"), canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate Ferry"), "GOLDEN GATE FERRY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Golden Gate Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate Transit"), "GOLDEN GATE TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Menlo Park"), "Menlo Park", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Muni"), "MUNI", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oshaughnessy", "OShaughnessy")) %>%
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Cable Car"), paste0(canonical_name, " Cable Car"), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni Cable Car ", "")) %>%  
  mutate(canonical_name = str_replace(canonical_name, "^Muni Route ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " Historic Streetcar", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "33 Stanyan", "33 Ashbury 18th")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=OWL).*", "")) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=.{1,4}Light Rail:.{1,50}) {1,5}Metro", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=[A-Z]{1}) Light Rail: ", " ")) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Palo Alto"), "Palo Alto", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Sam *Trans*\\s+(Route )*", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " – ", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "ñ", "n")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Sam *Trans*"), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA (Route |-)*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^Santa Clara VTA.{0,20} Light Rail):.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Santa Clara VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^(Santa Clara )*VTA"), "VTA", canonical_operator)) %>%
  mutate(canonical_name = ifelse(str_detect(survey_name, "DASH"), "201 Downtown Area Shuttle (Dash)", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "DASH"), "VTA", canonical_operator)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "VTA", str_to_title(str_to_lower(canonical_name)), canonical_name)) %>% 
  mutate(canonical_name = ifelse(str_detect(survey_name, "SJC"), "SJC Airport Flyer Shuttle", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SJC"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Stanford Marguerite"), "Stanford Marguerite Shuttle", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SCMTD Highway ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro", "Unknown")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(SCMTD|^Santa Cruz)"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "^\\s*Shuttles ") & !str_detect(canonical_name, "Shuttle$"), paste(canonical_name, "Shuttle"), canonical_name)) %>%
  mutate(canonical_name = ifelse(str_detect(canonical_name, "^\\s*Shuttles "), str_replace(canonical_name, "^\\s*Shuttles ", ""), canonical_name)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

caltrain_routes <- caltrain_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "Caltrain",
         survey_year = 2014) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()
  
# Adjust route names within Muni survey
sf_muni_routes <- sf_muni_raw_df %>%
  select_at(vars(contains("route"))) %>%
  select_at(vars(-contains("lat"))) %>%
  select_at(vars(-contains("lon"))) %>%
  select_at(vars(-contains("code"))) %>%
  gather(variable, survey_name) %>%
  # select(survey_name) %>%
  unique() %>% 
  filter(survey_name != "") %>%
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " */ *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("missing", ignore_case = TRUE)), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "-", "Missing", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[0-9]"), "MUNI", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[A-Z]+-"), "MUNI", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^MUNI "), "MUNI", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(canonical_name, "Cable Car"), "MUNI", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ INBOUND \\]", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ OUTBOUND \\]", "")) %>%  
  mutate(canonical_name = ifelse(canonical_name == "California", "California Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_name == "Powell Hyde", "Powell Hyde Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_name == "MASON POWELL", "Powell Mason Cable Car", canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "S Light Rail: Castro Shuttle Metro", "S Castro Shuttle")) %>%
  mutate(canonical_name = ifelse(canonical_operator == "MUNI", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^AC ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^AC "), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Alcatraz "), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Altamont Commuter Express.*", "ACE")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Altamont Commuter Express"), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple bus", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^BART ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "BART---"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Blue & Gold ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Blue & Gold "), "BLUE GOLD FERRY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=Burlingame Trolley Shuttle).*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(^CALTRAIN---)|(^Caltrain)"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor "), "SACRAMENTO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^County Connection ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^County Connection "), "COUNTY CONNECTION", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Emery "), "EMERYVILLE MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%  
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "FAST"), "FAIRFIELD-SUISUN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate "), "GOLDEN GATE TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate Ferry"), "GOLDEN GATE FERRY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, " *Livermore Amadore* (Valley )*(Transit)*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Livermore Amador"), "LAVTA", canonical_operator)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "LAVTA" & canonical_name == "", "Missing", canonical_name)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin[ ]*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Marin[ ]*"), "MARIN TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo Shuttles"), "PRESIDIGO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SamTrans "), "SAMTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Francisco Bay Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Francisco Bay Ferry "), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Stanford Marguerite"), "Stanford Marguerite Shuttle", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Stanford "), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SolTrans "), "SOLTRANS", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Tri Delta "), "TRI-DELTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^UCSF "), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Union City "), "UNION CITY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE 29 ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VINE "), "NAPA VINE", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_name = ifelse(str_detect(survey_name, "DASH"), "201 Downtown Area Shuttle (Dash)", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "VTA", str_to_title(str_to_lower(canonical_name)), canonical_name)) %>% 
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^WestCAT "), "WESTCAT", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "Lynx", "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

bad_references <- sf_muni_routes %>% 
  filter(canonical_operator == "BAD REFERENCE")

sf_muni_routes <- sf_muni_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "SF Muni",
         survey_year = 2014) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique() %>%
  arrange(canonical_operator, canonical_name)

# Review of error_check shows that the only records not in reconciled in ALL
# survey standardizations are records in ONLY one of them.
error_check <- left_join(bind_rows(sf_muni_routes, bart_routes),
                         caltrain_routes,
                         by = c("canonical_name", "canonical_operator")) %>%
  bind_rows(right_join(bind_rows(sf_muni_routes, bart_routes),
                       caltrain_routes,
                       by = c("canonical_name", "canonical_operator"))) %>%
  filter(is.na(survey_name.x) | is.na(survey_name.y)) %>% 
  mutate(group_count = ifelse(!is.na(survey_name.x), 1, 0),
         single_count = ifelse(!is.na(survey_name.y), 1, 0)) %>%
  group_by(canonical_name, canonical_operator) %>%
  summarise(group_uses = sum(group_count),
            single_uses = sum(single_count)) %>%
  ungroup() %>%
  mutate(canonical_name = str_replace(canonical_name, "^ ", ""),
    route = str_extract(canonical_name, "^[[:alnum:]]* "))

group_df <- error_check %>% 
  filter(group_uses != 0) %>%
  select(canonical_operator, route, group_name = canonical_name)

new_df <- error_check %>%
  filter(single_uses != 0) %>% 
  select(canonical_operator, route, new_name = canonical_name)

error_check <- new_df %>%
  full_join(group_df, by = c("canonical_operator", "route")) %>%
  arrange(canonical_operator, route)

canonical_routes <- ac_transit_routes %>% 
  bind_rows(bart_routes, caltrain_routes, sf_muni_routes) %>%
  mutate(canonical_name = paste(canonical_operator, canonical_name, sep = op_delim)) %>%
  select(-canonical_operator) 

write.csv(canonical_routes, canonical_route_path, row.names = FALSE)
  