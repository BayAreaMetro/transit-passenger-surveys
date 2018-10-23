##################################################################################################
### Script to create a database of canonical route and station names
### Author: John Helsel, October 2018
##################################################################################################

# Libraries and optins
library(tidyverse)
library(sf)
options(stringsAsFactors = FALSE)

# User check to assign proper paths for input data and writes
user_list <- data.frame(
  user = c("helseljw"), 
  path = c("../../Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

# Input data paths
sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

bart_path <- paste0(dir_path,
  "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")

caltrain_path <- paste0(dir_path, 
  "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE NO POUND OR SINGLE QUOTE.csv")

canonical_station_path <- paste0(dir_path,
  "Geography Files/Passenger_Railway_Stations_2018.shp")

standard_route_path <- "standard_route_crosswalk.csv"
canonical_route_path <- "canonical_route_names.csv"

# Read raw survey files
sf_muni_raw <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

bart_raw <- read.csv(bart_path) %>%
  rename_all(tolower)

caltrain_raw <- read.csv(caltrain_path) %>%
  rename_all(tolower)

canonical_station <- st_read(canonical_station_path)

# Adjust route names within Muni survey
sf_muni_routes <- sf_muni_raw %>%
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

  mutate(canonical_name = str_replace(canonical_name, "  ", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("missing", ignore_case = TRUE)), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "-", "Missing", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[0-9]"), "SF Muni", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^[A-Z]+-"), "SF Muni", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^MUNI "), "SF Muni", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(canonical_name, "Cable Car"), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ INBOUND \\]", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ OUTBOUND \\]", "")) %>%  
  mutate(canonical_name = ifelse(canonical_name == "California-", "California Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_name == "Powell-Hyde", "Powell Hyde Cable Car", canonical_name)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%

  
  mutate(canonical_name = str_replace(canonical_name, "^AC ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^AC "), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Alcatraz ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Alcatraz "), "Alcatraz", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Altamont Commuter Express \\(ACE\\) Westbound ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Altamont Commuter Express \\(ACE\\)"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), canonical_name, canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple bus", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("BART", ignore_case = TRUE)), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Blue & Gold ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Blue & Gold "), "Blue & Gold", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "CALTRAIN", "Caltrain")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("Caltrain", ignore_case = TRUE)), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Sacramento/San Jose")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor "), "Capitol Corridor", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "^County Connection ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^County Connection "), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Emery "), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%  
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "FAST"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "Caltrain", canonical_operator)) %>%
    
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate "), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Livermore Amadore ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Livermore Amadore "), "Livermore Amadore Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin[ ]*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Marin[ ]*"), "Marin Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo Shuttles"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SamTrans "), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Francisco Bay Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Francisco Bay Ferry "), "San Francisco Bay Ferry", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Stanford "), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SolTrans "), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Tri Delta "), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^UCSF "), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Union City "), "Union City", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE 29 ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VINE "), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "DASH Downtown Area Shuttle", "DOWNTOWN AREA SHUTTLE (DASH)")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA "), "VTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^WestCAT "), "WestCAT", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(survey_name == "Lynx", "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

bad_references <- sf_muni_routes %>% 
  filter(canonical_operator == "BAD REFERENCE")

sf_muni_routes <- sf_muni_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "SFMTA",
         survey_year = 2014) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique() %>%
  arrange(canonical_operator, canonical_name)

# Adjust route names within BART survey
transfer_names <- bart_raw %>%
  select_at(vars(contains("trnsfr"))) %>%
  select_at(vars(-contains("agency"))) %>%
  colnames()

bart_routes <- bart_raw %>% 
  select(one_of(transfer_names)) %>%
  gather(variable, value = survey_name) %>%
  filter(survey_name != "") %>%
  unique() %>%
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "  ", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "illogical"), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Missing"), "Missing", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AC Transit Route "), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^ACE ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE "), "ACE", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "Alameda County", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alta Bates"), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bayhill"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bishop Ranch"), "Bishop Ranch", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain L[A-Z]* ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain (?=B)", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\(unspecified\\)", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "Capitol Corridor", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Childrens Hospital"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CPMC"), "CPMC", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Crocker Park"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CSU"), "CSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "Dumbarton Express", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Estuary Crossing"), "City of Alameda", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairfield and Suisun Transit \\(FAST\\)"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairmont Hospital"), "Fairmont Hospital", canonical_operator)) %>%
           
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Foster City"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Golden Gate Transit"), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Muni"), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni Route 55 16th St.", "55 16th Street")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oshaughnessy", "OShaughnessy")) %>%
  mutate(canonical_name = ifelse(str_detect(canonical_name, "Cable Car"), paste0(canonical_name, " Cable Car"), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "Cable Car - ", "")) %>%

  mutate(canonical_name = str_replace(canonical_name, "^Muni (Route )?", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " - Historic Streetcar", "")) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=.{1,4}Light Rail:.{1,50}) {1,5}Metro", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=[A-Z]{1}) Light Rail: ", " ")) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay Shuttle"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin Transit Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Marin Transit"), "Marin Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Mariners"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Monterey-Salinas Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Monterey-Salinas"), "Monterey-Salinas Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Oyster"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Rio Vista Delta Breeze Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Rio Vista Delta"), "Rio Vista Delta", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans (Route )?", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SamTrans"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Joaquin"), "San Joaquin", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Cruz Metro"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Rosa City[ ]?Bus Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Rosa City"), "Santa Rosa City", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Seton Medical"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFGH"), "SFGH", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SolTrans"), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Sierra Point"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta Transit Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Tri Delta Transit"), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UC Berkeley"), "UC Berkeley", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Union City"), "Union City", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Utah Grand"), "Utah Grand", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route 29 ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "VINE"), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA Route 902", "902 Light Rail")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^VTA.{0,20}):.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VTA Route ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Downtown Area Shuttle", "DOWNTOWN AREA SHUTTLE")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Yahoo"), "Yahoo", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))
  
bart_routes <- bart_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "BART",
         survey_year = 2015) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()

# Adjust route names within Caltrain survey
caltrain_routes <- caltrain_raw %>% 
  select_at(vars(contains("transfer_"))) %>%
  select_at(vars(-contains("loc"))) %>%
  gather(variable, value = survey_name) %>%
  filter(survey_name != "") %>%
  unique() %>% 
  mutate(canonical_name = survey_name) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "  ", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AC Transit"), "AC Transit", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Amtrak"), "Amtrak", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "Angel Island.*", "Angel Island-Tiburon Ferry")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), "Angel Island-Tiburon Ferry", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^ BART ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Bayview"), "Bayview", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Burlingame ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Burlingame"), "Burlingame", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^Caltrain SHUTTLE", "Caltrain Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^County Connection"), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Dumbarton Express Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "Dumbarton Express", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^Golden Gate Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate Ferry"), "Golden Gate Ferry", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Golden Gate Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate Transit"), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Menlo Park"), "Menlo Park", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Muni"), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oshaughnessy", "OShaughnessy")) %>%
  mutate(canonical_name = str_replace(canonical_name, ".*Cable Car.*", "Cable Car")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni Route ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Muni ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " - Historic Streetcar", "")) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", str_replace_all(canonical_name, "[-/]", " "), canonical_name)) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=.{1,4}Light Rail:.{1,50}) {1,5}Metro", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=[A-Z]{1}) Light Rail: ", " ")) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Palo Alto"), "Palo Alto", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Sam *Trans*\\s+(Route )*", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "–", "-")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Sam *Trans*"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA (Route |-)*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^Santa Clara VTA.{0,20} Light Rail):.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Santa Clara VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^(Santa Clara )*VTA"), "VTA", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Stanford"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SCMTD Highway ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro", "Unknown")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(SCMTD|^Santa Cruz)"), "Santa Cruz Metro", canonical_operator))
  

# Create canonical list of station names/locations
coords <- canonical_station %>%
  st_coordinates()

canonical_station <- canonical_station %>% 
  mutate(lat  = coords[ , 1]) %>%
  mutate(long = coords[ , 2])

st_geometry(canonical_station) <- NULL



# Review of error_check shows that the only records not in reconciled in ALL
# survey standardizations are records in ONLY one of them.
error_check <- left_join(bind_rows(sf_muni_routes, bart_routes), 
                         caltrain_routes, 
                         by = c("canonical_name", "canonical_operator")) %>%
  bind_rows(right_join(bind_rows(sf_muni_routes, bart_routes), 
                       caltrain_routes, 
                       by = c("canonical_name", "canonical_operator"))) %>%
  filter(is.na(survey_name.x) | is.na(survey_name.y))
  
standard_routes <- bind_rows(sf_muni_routes, bart_routes, caltrain_routes)
canonical_routes <- standard_routes %>%
  select(canonical_operator, canonical_name) %>%
  unique()
  
  
write.csv(standard_routes, standard_route_path)
write.csv(canonical_routes, canonical_route_path)  
  