##################################################################################################
### Script to create a database of canonical route and station names
### Author: John Helsel, October 2018
##################################################################################################

# Libraries and options
list_of_packages <- c(
  "rlang",
  "sf",
  "tidyverse"
)

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

# JWH: somewhat uncomfortable having the geocoding functions so far away from 
# this project, but I would prefer not to duplicate the function script in case 
# of further changes to the main geocoding script.

list_of_helpers <- c(
  "../production/Build Standard Database Functions.R"
)

for (f in list_of_helpers){
  source(f)
}

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

# Output data paths
canonical_route_path <- "../production/canonical_route_crosswalk.csv"


# Read crosswalk files
OP_DELIMITER <- "___"
ROUTE_DELIMITER <- "&&&"

get_rail_names_inputs_df <- read.csv(get_rail_names_inputs_path)

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

# Adding station names for BART and Caltrain is done here to create a complete 
# canonical listing of all route names that will appear in the RMD. The "route" 
# column will NOT be saved or exported in the BART or Caltrain raw files.

bart_raw_df <- bart_raw_df %>% 
  # Replace unknown records with imputed values
  mutate(access_trnsfr_list1 = ifelse(access_trnsfr_list1 == "Unknown", access_trnsfr_list1_imputed, access_trnsfr_list1),
         access_trnsfr_list2 = ifelse(access_trnsfr_list2 == "Unknown", access_trnsfr_list2_imputed, access_trnsfr_list2),
         access_trnsfr_list3 = ifelse(access_trnsfr_list3 == "Unknown", access_trnsfr_list3_imputed, access_trnsfr_list3),
         egress_trnsfr_list1 = ifelse(egress_trnsfr_list1 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list1),
         egress_trnsfr_list2 = ifelse(egress_trnsfr_list2 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list2),
         egress_trnsfr_list3 = ifelse(egress_trnsfr_list3 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list3)) %>%
  mutate(route = paste0("BART", OP_DELIMITER, first_entered_bart, ROUTE_DELIMITER, bart_exit_station))

# Caltrain create internal Route
caltrain_raw_df <- caltrain_raw_df %>%
  mutate(route = paste0("CALTRAIN", OP_DELIMITER, enter_station, ROUTE_DELIMITER, exit_station)) 

# Begin building canonical database

# Adjust route names within AC Transit survey

transfer_names_list <- ac_transit_raw_df %>%
  select(matches("final_trip|final_route")) %>%
  select(-matches("code|stopid|other")) %>%
  colnames()

ac_transit_routes_df <- ac_transit_raw_df %>%
  select(one_of(transfer_names_list)) %>%
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
  mutate(canonical_name = str_replace(canonical_name, "\\s", " ")) %>%
 
  mutate(canonical_name = str_replace(canonical_name, "^ROUTE ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^AC Transit [0-9A-Z]{1,5} ) ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^AC Transit ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[TO.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[CLOCKWISE.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[COUNTERCLOCKWISE.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, '1 San Leandro Bart Dtn Oakland', '1 Berkeley BART to Bay Fair BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '12 Dtn Oakland Dtn Berkeley 4th St Harrison', '12 Berkeley BART to Downtown Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, '14 W Oakland Bart Fruitval Bart', '14 Downtown Oakland to Fruitvale BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '18 San Pablo & Monroe Berkeley Merritt BART', '18 University Village Albany to Montclair')) %>%
  mutate(canonical_name = str_replace(canonical_name, '20 Fruitvale Ave Alameda 11th MLK Jr Wy', '20 Dimond District Oakland to downtown Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, '200 Union City BART Newpark Mall Fremont BART', '200 Union City BART Fremont BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '21 Fruitvale Ave Alameda Oakland Airport', '21 Dimond Dist to Oakland Airport')) %>%
  mutate(canonical_name = str_replace(canonical_name, '210 Union Landing Frmt Blvd Ohlone', '210 Ohlone College to Union Landing Shopping Center')) %>%
  mutate(canonical_name = str_replace(canonical_name, '212 Fremont BART Newpark Mall Pacific Commons', '212 Fremont BART to NewPark Mall')) %>%
  mutate(canonical_name = str_replace(canonical_name, '216 UC BART Niles Fremont BART Ohlone Newark', '216 Ohlone College Newark Campus to Union City BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '215 Fremont BART Mission Warm Springs Industrial Area', '215 Fremont BART to Gateway Blvd & Lakeside Pkwy')) %>%
  mutate(canonical_name = str_replace(canonical_name, '217 Frmt BART Mission Milpitas Alder', '217 Fremont BART to Great Mall')) %>%
  mutate(canonical_name = str_replace(canonical_name, '22 Hayward Bart South Hayward Bart Chabot', '22 Hayward BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '232 Fremont BART UC BART Mission Ohlone Newark', '232 Fremont BART to New Park Mall')) %>%
  mutate(canonical_name = str_replace(canonical_name, '239 Fremont BART Warm Springs', '239 Fremont BART to Warm Springs Blvd & Dixon Landing Rd')) %>%
  mutate(canonical_name = str_replace(canonical_name, '251 Fremont BART Mowry Thornton', '251 Fremont BART to NewPark Mall')) %>%
  mutate(canonical_name = str_replace(canonical_name, '32 Hayward Bart CV Bart Hwd Bart Cherryland', '32 Hayward BART to Castro Valley BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '37 HWD BART WHITMAN SO HWD BART', '37 Hayward BART to South Hayward BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '376 North Richmond Shuttle', '376 El Cerrito Del Norte BART to Richmond Parkway Transit Center')) %>%
  mutate(canonical_name = str_replace(canonical_name, '39 Fruitvale Bart Skyline High School', '39 Fruitvale BART to Skyline High School')) %>%
  mutate(canonical_name = str_replace(canonical_name, '40 Dtn Oakland Eastmont TC Bayfair Bart', '40 Downtown Oakland to Bay Fair BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '45 Eastmont T C Foothill Sq', '45 Eastmont Transit Center to Foothill Square Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, '46 Coliseum Bart Knowland Zoo', '46 Coliseum BART to Oakland Zoo')) %>%
  mutate(canonical_name = str_replace(canonical_name, '46L Coliseum Bart Golf Links Rd Dunkirk Ave', '46L Coliseum BART Mountain Golf Links Rd Dunkirk Ave')) %>%
  mutate(canonical_name = str_replace(canonical_name, '47 Fruitvale Bart Maxwell Park Div 4', '47 Fruitvale BART to Maxwell Park')) %>%
  mutate(canonical_name = str_replace(canonical_name, '48 Bayfair BART Castro Valley Hayward bart', '48 Hayward BART to Bay Fair BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '51A Oakland Alameda Fruitvale Bart', '51A Rockridge BART to Fruitvale BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '51B RockRidge 3RUN', '51B Rockridge BART to Berkeley Amtrak')) %>%
  mutate(canonical_name = str_replace(canonical_name, '52 UC Village UC Campus', '52 University Village to UC Campus (Berkeley BART)')) %>%
  mutate(canonical_name = str_replace(canonical_name, '54 Fruitvale Bart Merritt College', '54 Fruitvale BART to Merritt College')) %>%
  mutate(canonical_name = str_replace(canonical_name, '57 Emeryville Macarthur Blvd Foothill Sq', '57 Emeryville to Foothill Square Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, '60 CSUEB HAYWARD BART SHUTTLE', '60 Hayward BART to Cal State East Bay')) %>%
  mutate(canonical_name = str_replace(canonical_name, '62 W Oakland Bart Fruitvale Bart', '62 West Oakland BART to Fruitvale BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '65 Spruce St Euclid Ave', '65 Berkeley BART to Lawrence Hall of Science')) %>%
  mutate(canonical_name = str_replace(canonical_name, '67 Berkeley Bart Grizzly Peak Blvd', '67 Berkeley BART to Grizzly Peak')) %>%
  mutate(canonical_name = str_replace(canonical_name, '7 DNORTE BART BERKELEY BART', '7 El Cerrito del Norte BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '70 Richmond Pkwy TC Richmond BART', '70 Richmond BART to Richmond Pkwy Transit Center')) %>%
  mutate(canonical_name = str_replace(canonical_name, '71 Richmond Pkwy TC El Cerrito Plaza BART', '71 El Cerrito Plaza BART to Richmond Parkway Transit Center')) %>%
  mutate(canonical_name = str_replace(canonical_name, '72 Richmond Downtown Oakland', '72 Hilltop Mall to Oakland Amtrak')) %>%
  mutate(canonical_name = str_replace(canonical_name, '72M Richmond Downtown Oakland', '72M Point Richmond to Oakland Amtrak')) %>%
  mutate(canonical_name = str_replace(canonical_name, '72R San Pablo Rapid BUS', '72R San Pablo Rapid Contra Costa College to Jack London Square')) %>%
  mutate(canonical_name = str_replace(canonical_name, '73 Eastmont TC Oakland Airport', '73 Eastmont Transit Center to Oakland Airport')) %>%
  mutate(canonical_name = str_replace(canonical_name, '74 CCC Richmond BART Ford Pt', '74 Marina Bay Richmond to San Pablo Dam Rd El Sobrante')) %>%
  mutate(canonical_name = str_replace(canonical_name, '75 Bayfair Bart Washington Manor S L Bart', '75 San Leandro BART to Bay Fair BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '76 Hilltop Mall CCC del Norte BART', '76 El Cerrito del Norte BART to Hilltop Mall')) %>%
  mutate(canonical_name = str_replace(canonical_name, '805 OWL Dtn Oakland Oakland Airport', '805 All Nighter Downtown Oakland to Oakland Airport')) %>%
  mutate(canonical_name = str_replace(canonical_name, '83 Hayward BART S Hayward BART', '83 Hayward BART to South Hayward BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '86 Hayward BART S Hayward BART', '86 Hayward BART to South Hayward BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '88 Berkeley BART Downtown Oakland', '88 Berkeley BART to Lake Merritt BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '93 Hayward Bart Bayfair Bart San Lorenzo', '93 Hayward BART to Bay Fair BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '94 Hayward Bart Hayward Hills', '94 Hayward BART to Hayward High School')) %>%
  mutate(canonical_name = str_replace(canonical_name, '95 Kelly Hill Hayward BART', '95 Hayward BART to Fairview District')) %>%
  mutate(canonical_name = str_replace(canonical_name, '97 Union City BART Hesperian Blvd', '97 Bay Fair BART to Union City BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '98 Coliseum Bart 98th Ave Eastmont TC', '98 Coliseum BART Edgewater Dr')) %>%
  mutate(canonical_name = str_replace(canonical_name, '99 HwdBART UC Bart Fmt Bart', '99 Bay Fair BART to Fremont BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'B San Francisco Trestle Glen', 'B Lakeshore Ave Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'C San Francisco Piedmont', 'C Highland Ave Piedmont')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'CB San Francisco Montclair', 'CB Warren Freeway and Broadway Terr Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'E San Francisco Claremont Parkwood', 'E Caldecott Ln Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'F San Francisco Berkeley', 'F UC Campus Berkeley')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'FS San Francisco Berkeley', 'FS Solano Ave Berkeley')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'G San Francisco El Cerrito', 'G Richmond St & Potrero St El Cerrito')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'H San Francisco Richmond', 'H Barrett Ave & San Pablo Ave El Cerrito')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'J Berkeley San Francisco', 'J Sacramento St and University Ave Berkeley')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'L San Francisco El Sobrante', 'L San Pablo Dam Rd')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'LA San Francisco El Sobrante', 'LA Hilltop Dr Park & Ride')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'M Hayward BART Hillsdale Mall Oracle', 'M Hayward BART to Oracle')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NL San Francisco Eastmont TC', 'NL Eastmont Transit Center Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NX East Oakland San Francisco', 'NX Seminary Ave & MacArthur Blvd')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NX1 San Francisco East Oakland', 'NX1 Fruitvale Ave & MacArthur Blvd')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NX2 San Francisco East Oakland', 'NX2 High St & MacArthur Blvd')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NX3 San Francisco San Leandro', 'NX3 Marlow Dr & Foothill Way Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'NX4 San Francisco Castro Valley', 'NX4 Castro Valley Park & Ride')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'O San Francisco Alameda Fruitvale Bart', 'O Park Ave & Encinal Ave Alameda')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'P San Francisco Piedmont', 'P Highland Ave & Highland Way Piedmont')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'S San Francisco San Lorenzo Hayward', 'S Eden Shores Hayward')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'SB San Francisco Hayward Newark', 'SB Cedar Blvd & Stevenson Blvd Newark')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'U Frmt BART Stanford University', 'U Fremont BART to Stanford University')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'V San Francisco Montclair', 'V Broadway and Broadway Terr Oakland')) %>%
  mutate(canonical_name = str_replace(canonical_name, 'W San Francisco Alameda', 'W Broadway & Blanding Ave Alameda')) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ROUTE "), "AC TRANSIT", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AC Transit"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^BART___", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^CALTRAIN___", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection (Route )*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "COUNTY CONNECTION", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "AMTRAK", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "DHS"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Emery"), "EMERYVILLE MTA", canonical_operator)) %>%
  
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
  
  mutate(canonical_name = str_replace(canonical_name, '85 San Leandro BART Union Landing', '85 San Leandro BART to South Hayward BART')) %>%
  mutate(canonical_name = str_replace(canonical_name, '89 San Leandro Bart S L Marina Bayfair Bart', '89 San Leandro BART to Bay Fair BART')) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Leandro"), "SLTMO", canonical_operator)) %>%
  
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
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Union City"), "UNION CITY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, '10 Airport Service', '10 Metro Airport Ctrn')) %>%
  mutate(canonical_name = str_replace(canonical_name, '180 Express', '180 Fremont Bart San Jose Diridon Sta')) %>%
  mutate(canonical_name = str_replace(canonical_name, '181 Express', '181 San Jose Diridon Fremont Bart')) %>%
  mutate(canonical_name = str_replace(canonical_name, '22 Core', '22 Eastridge Palo Alto Menlo Park')) %>%
  mutate(canonical_name = str_replace(canonical_name, '23 Core', '23 Alum Rock Transit Ctr De Anza Col')) %>%
  mutate(canonical_name = str_replace(canonical_name, '323 Limited', '323 De Anza Col Downtown San Jose')) %>%
  mutate(canonical_name = str_replace(canonical_name, '39 Community Bus', '39 Villages Eastridge')) %>%
  mutate(canonical_name = str_replace(canonical_name, '522 RAPID', '522 Eastridge Palo Alto')) %>%
  mutate(canonical_name = str_replace(canonical_name, '66 Core', '66 Sta Teresa Hosp Milpitas Dixon')) %>%
  mutate(canonical_name = str_replace(canonical_name, '70 Core', '70 Capitol Lrt Stn Great Mall Main')) %>%
  mutate(canonical_name = str_replace(canonical_name, '71 Core', '71 Eastridge Great Mall Main')) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WESTCAT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

ac_transit_routes_df <- ac_transit_routes_df %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "AC Transit",
         survey_year = 2018) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()
  
#Adjust route names within BART survey
transfer_names_list <- bart_raw_df %>%
  select_at(vars(matches("(trnsfr)|(route)"))) %>%
  select_at(vars(-contains("agency"))) %>%
  colnames()

bart_routes_df <- bart_raw_df %>% 
  select(one_of(transfer_names_list)) %>%
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
  mutate(canonical_name = str_replace(canonical_name, "\\s", " ")) %>%
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
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, paste0("BART", OP_DELIMITER)), "BART", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, paste0("BART", OP_DELIMITER), "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Intl", "International")) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bayhill"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bishop Ranch"), "Bishop Ranch", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "\\(Millbrae.*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Broadway"), "CALTRAIN", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "North Burlingame shuttle", "North Burlingame Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain.*", "CALTRAIN___MISSING___MISSING")) %>%
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
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "AC TRANSIT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Emery"), "EMERYVILLE MTA", canonical_operator)) %>%
  
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
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Leandro"), "SLTMO", canonical_operator)) %>%
  
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
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Union City"), "UNION CITY", canonical_operator)) %>%
  
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
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Yahoo"), "Yahoo", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))
  
bart_routes_df <- bart_routes_df %>%
  mutate(survey = "BART",
         survey_year = 2015) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()

# Adjust route names within Caltrain survey
caltrain_names_list <- caltrain_raw_df %>%
  select(route, matches("(transfer_)|route"), -matches("loc")) %>%
  colnames()

caltrain_routes_df <- caltrain_raw_df %>% 
  select(one_of(caltrain_names_list)) %>%
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
  mutate(canonical_name = str_replace(canonical_name, "\\s", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  
  
  mutate(canonical_name = str_replace_all(canonical_name, "AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AC Transit"), "AC TRANSIT", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Amtrak"), "AMTRAK", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "Angel Island.*", "Angel Island Tiburon Ferry")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), "SF BAY FERRY", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^BART[A-Z ]* ", "BART___")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "(?<=BART[_A-Za-z /]{1,50}) [Tt]o ", OP_DELIMITER)) %>%
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
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "AC TRANSIT", canonical_operator)) %>%

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
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Stanford Mar[gq]uerite"), "Stanford Marguerite Shuttle", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Mar[gq]uerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SCMTD Highway ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro", "Unknown")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(SCMTD|^Santa Cruz)"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(canonical_name, "^\\s*Shuttles ") & !str_detect(canonical_name, "Shuttle$"), paste(canonical_name, "Shuttle"), canonical_name)) %>%
  mutate(canonical_name = ifelse(str_detect(canonical_name, "^\\s*Shuttles "), str_replace(canonical_name, "^\\s*Shuttles ", ""), canonical_name)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

caltrain_routes_df <- caltrain_routes_df %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "Caltrain",
         survey_year = 2014) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()
  
# Adjust route names within Muni survey
sf_muni_routes_df <- sf_muni_raw_df %>%
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
  mutate(canonical_name = str_replace(canonical_name, "\\s", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("missing", ignore_case = TRUE)), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "-", "Missing", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[0-9]"), "MUNI", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[A-Z]+-"), "MUNI", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^MUNI "), "MUNI", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ INBOUND \\]", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ OUTBOUND \\]", "")) %>%  
  mutate(canonical_name = ifelse(canonical_name == "California", "California Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_name == "Powell Hyde", "Powell Hyde Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_name == "POWELL MASON", "Powell Mason Cable Car", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(canonical_name, "Cable Car"), "MUNI", canonical_operator)) %>%
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
  mutate(canonical_name = str_replace(canonical_name, "^Brisbain", "Brisbane")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Brisbain"), "BART", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "BART___"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Blue & Gold ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Blue & Gold "), "BLUE GOLD FERRY", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=Burlingame Trolley Shuttle).*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(^CALTRAIN___)|(^Caltrain)"), "CALTRAIN", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor "), "AMTRAK", canonical_operator)) %>%
  
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
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "VTA", str_to_title(str_to_lower(canonical_name)), canonical_name)) %>% 
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^WestCAT "), "WESTCAT", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "Lynx", "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

sf_muni_routes_df <- sf_muni_routes_df %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "SF Muni",
         survey_year = 2017) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique() %>%
  arrange(canonical_operator, canonical_name)

# Create crosswalk of station names based off 

canonical_station_shp <- st_read(canonical_station_path)
st_geometry(canonical_station_shp) <- NULL

# canonical_bart_stations_df <- bart_routes_df %>% 
#   filter(canonical_operator == "BART") %>% 
#   select(canonical_name) %>% 
#   mutate(station_1 = str_replace(canonical_name, "&&&.*", "")) %>% 
#   mutate(station_2 = str_replace(canonical_name, ".*&&&", "")) 
# canonical_bart_stations_df <- canonical_bart_stations_df %>%
#   select(station = station_1) %>%
#   bind_rows(canonical_bart_stations_df %>% select(station = station_2)) %>%
#   unique()

bart_station_df <- canonical_station_shp %>% 
  filter(agencyname == "BART") %>% 
  select(shp_name = station_na) %>%
  mutate(canonical_name = shp_name) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "/", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "'", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=Coliseum).*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oakland Airport", "Oakland International Airport")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Intl", "International")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=Pittsburg ).*", "Bay Point")) 

bart_station_df <- expand.grid(bart_station_df$shp_name, bart_station_df$shp_name) %>%
  rename(station_1 = Var1, station_2 = Var2) %>%
  mutate(survey_name = paste0("BART", OP_DELIMITER, station_1, ROUTE_DELIMITER, station_2)) %>%
  bind_cols(expand.grid(bart_station_df$canonical_name, bart_station_df$canonical_name) %>%
              rename(station_1 = Var1, station_2 = Var2) %>%
              mutate(canonical_name = paste0(station_1, ROUTE_DELIMITER, station_2))) %>%
  mutate(survey = "GEOCODE", 
         survey_year = 2018,
         canonical_operator = "BART"
         ) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator)

# canonical_caltrain_stations_df <- caltrain_routes_df %>% 
#   filter(canonical_operator == "CALTRAIN") %>% 
#   select(canonical_name) %>% 
#   mutate(station_1 = str_replace(canonical_name, "&&&.*", "")) %>% 
#   mutate(station_2 = str_replace(canonical_name, ".*&&&", "")) 
# canonical_caltrain_stations_df <- canonical_caltrain_stations_df %>%
#   select(station = station_1) %>%
#   bind_rows(canonical_caltrain_stations_df %>% select(station = station_2)) %>%
#   unique()

caltrain_station_df <- canonical_station_shp %>% 
  filter(agencyname == "CALTRAIN") %>% 
  select(shp_name = station_na) %>%
  mutate(canonical_name = shp_name) %>%
  mutate(canonical_name = str_replace(canonical_name, " Station", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " *- *", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "/", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " +", " ")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "\\.", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "'", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, " {2,9}", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, "22nd St", "22nd Street")) %>%
  mutate(canonical_name = str_replace(canonical_name, "S San Francisco", "South San Francisco"))

caltrain_station_df <- expand.grid(caltrain_station_df$shp_name, caltrain_station_df$shp_name) %>%
  rename(station_1 = Var1, station_2 = Var2) %>%
  mutate(survey_name = paste0("CALTRAIN", OP_DELIMITER, station_1, ROUTE_DELIMITER, station_2)) %>%
  bind_cols(expand.grid(caltrain_station_df$canonical_name, caltrain_station_df$canonical_name) %>%
              rename(station_1 = Var1, station_2 = Var2) %>%
              mutate(canonical_name = paste0(station_1, ROUTE_DELIMITER, station_2))) %>%
  mutate(survey = "GEOCODE", 
         survey_year = 2018,
         canonical_operator = "CALTRAIN"
  ) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator)


canonical_routes_df <- ac_transit_routes_df %>% 
  bind_rows(bart_routes_df, caltrain_routes_df, sf_muni_routes_df, bart_station_df, caltrain_station_df) %>%
  mutate(canonical_name = paste(canonical_operator, canonical_name, sep = OP_DELIMITER)) 

base_tech_df <- data.frame(canonical_operator = c("AC TRANSIT", "AirTrain", "AMTRAK", "Apple", "BAD REFERENCE", "BART", "Bayview",
                                       "Berkeley Gateway TMA", "Bishop Ranch", "BLUE GOLD FERRY", "CALTRAIN", "COUNTY CONNECTION", "CPMC",
                                       "CSU", "EMERYVILLE MTA", "Facebook", "FAIRFIELD-SUISUN", "Fairmont Hospital", "GOLDEN GATE FERRY",
                                       "GOLDEN GATE TRANSIT", "Harbor Bay", "Highland Hospital", "Kaiser", "LAVTA", "LBL", "MARIN TRANSIT",
                                       "Menlo Park", "Missing", "Monterey-Salinas Transit", "MUNI", "NAPA VINE", "Palo Alto", "PRESIDIGO",
                                       "RIO-VISTA", "SAMTRANS", "Santa Cruz Metro", "Santa Rosa City", "SF BAY FERRY", "SFGH", "SFSU",
                                       "SLTMO", "SOLTRANS", "Stanford", "TRI-DELTA", "UC BERKELEY", "UCSF", "UNION CITY",
                                       "Utah Grand", "VTA", "WESTCAT", "Yahoo"),
                          technology = c("local bus", "heavy rail", "commuter rail", "local bus", "BAD REFERENCE", "heavy rail", "local bus",
                                         "local bus", "local bus", "ferry", "commuter rail", "local bus", "local bus", 
                                         "local bus", "local bus", "local bus", "local bus", "local bus", "ferry",
                                         "local bus", "local bus", "local bus", "local bus", "local bus", "local bus", "local bus",
                                         "local bus", "Missing",  "local bus", "local bus", "local bus", "local bus", "local bus",
                                         "local bus", "local bus", "local bus", "local bus", "ferry",  "local bus", "local bus",
                                         "local bus", "local bus", "local bus", "local bus", "local bus", "local bus", "local bus",
                                         "local bus", "local bus", "local bus", "local bus")
                          
                          )

# Add bespoke tech replacements to handle exceptions from the base tech
canonical_routes_df <- canonical_routes_df %>% 
  left_join(base_tech_df, by = "canonical_operator") %>%
  mutate(technology = ifelse(str_detect(canonical_name,"Light Rail") | str_detect(survey_name, "Light Rail"), "light rail", technology)) %>%
  mutate(technology = ifelse(str_detect(canonical_name, "AC TRANSIT___[A-Z]+ ") & !str_detect(canonical_name, fixed("shuttle", ignore_case = TRUE)), "express bus", technology)) %>%
  mutate(technology = ifelse(canonical_operator == "NAPA VINE" & str_detect(canonical_name, "(21)|(25)|(29)"), "express bus", technology)) %>%
  mutate(technology = ifelse(str_detect(canonical_name, "^MUNI___[A-Z]+ "), "light rail", technology)) %>%
  mutate(technology = ifelse(str_detect(canonical_name, "^VTA___1[0-9]{2}"), "express bus", technology))

write.csv(canonical_routes_df, canonical_route_path, row.names = FALSE)
  
# # The code below can be used to help develop copy paste material for reconciling routes in new surveys
# error_check <- left_join(bind_rows(sf_muni_routes_df, bart_routes_df, caltrain_routes_df),
#                          ac_transit_routes_df,
#                          by = c("canonical_name", "canonical_operator")) %>%
#   bind_rows(right_join(bind_rows(sf_muni_routes_df, bart_routes_df, caltrain_routes_df),
#                        ac_transit_routes_df,
#                        by = c("canonical_name", "canonical_operator"))) %>%
#   filter(is.na(survey_name.x) | is.na(survey_name.y)) %>%
#   mutate(group_count = ifelse(!is.na(survey_name.x), 1, 0),
#          single_count = ifelse(!is.na(survey_name.y), 1, 0)) %>%
#   group_by(canonical_name, canonical_operator) %>%
#   summarise(group_uses = sum(group_count),
#             single_uses = sum(single_count)) %>%
#   ungroup() %>%
#   mutate(canonical_name = str_replace(canonical_name, "^ ", ""),
#          route = str_extract(canonical_name, "^[[:alnum:]]* "))
# 
# group_df <- error_check %>%
#   filter(group_uses != 0) %>%
#   select(canonical_operator, route, group_name = canonical_name)
# 
# new_df <- error_check %>%
#   filter(single_uses != 0) %>%
#   select(canonical_operator, route, new_name = canonical_name)
# 
# error_check <- new_df %>%
#   full_join(group_df, by = c("canonical_operator", "route")) %>%
#   arrange(canonical_operator, route)
# 
# ac_edits <- error_check %>%
#   filter(!is.na(new_name) & !is.na(group_name)) %>%
#   filter(!canonical_operator %in% c("BART", "CALTRAIN")) %>%
#   mutate(script = paste0("mutate(canonical_name = str_replace(canonical_name, '", new_name,"', '", group_name, "')) %>%"))
# write.csv(ac_edits, "ac_edits.csv")
