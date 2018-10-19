
library(tidyverse)
options(stringsAsFactors = FALSE)

user_list <- data.frame(
  user = c("helseljw"), 
  path = c("../../Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

bart_path <- paste0(dir_path,
  "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")

sf_muni <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

bart <- read.csv(bart_path) %>%
  rename_all(tolower)

# sf_cols <- colnames(sf_muni %>% select_at(vars(contains("transfer"))))
# sf_cols <- c(sf_cols, colnames(sf_muni %>% select_at(vars(contains("route")))))
sf_muni_routes <- sf_muni %>%
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
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^MUNI "), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ INBOUND \\]", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\[ OUTBOUND \\]", "")) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^AC ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^AC "), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Alcatraz ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Alcatraz "), "Alcatraz", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Altamont Commuter Express \\(ACE\\) Westbound ", "")) %>%
  mutate(canonical_operator  = ifelse(str_detect(survey_name, "^Altamont Commuter Express \\(ACE\\)"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), canonical_name, canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple bus", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("^BART", ignore_case = TRUE)), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Blue & Gold ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Blue & Gold "), "Blue & Gold", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Burlingame.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "CALTRAIN", "Caltrain")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("Caltrain", ignore_case = TRUE)), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Sacramento/San Jose")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor "), "Capitol Corridor", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "^County Connection ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^County Connection "), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Emery "), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Facebook.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%  
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "FAST"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Genentech.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "Caltrain", canonical_operator)) %>%
    
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Golden Gate "), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Harbor Bay Shuttle.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "LBL.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Livermore Amadore ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Livermore Amadore "), "Livermore Amadore Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin[ ]*", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Marin[ ]*"), "Marin Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "PresidiGo.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo Shuttles"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SamTrans "), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Francisco Bay Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Francisco Bay Ferry "), "San Francisco Bay Ferry", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*SFSU.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Stanford.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Stanford "), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^SolTrans "), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Tri Delta "), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^UCSF.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^UCSF "), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Union City "), "Union City", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE 29 ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VINE "), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA "), "VTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^WestCAT "), "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*Shuttle.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

bad_references <- sf_muni_routes %>% 
  filter(canonical_operator == "BAD REFERENCE")

sf_muni_routes <- sf_muni_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "SFMTA",
         year = 2014) %>%
  select(survey, year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique() %>%
  arrange(canonical_operator, canonical_name)

# BART
transfer_names <- bart %>%
  select_at(vars(contains("trnsfr"))) %>%
  select_at(vars(-contains("agency"))) %>%
  colnames()

bart_routes <- bart %>% 
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

  mutate(canonical_name = str_replace(canonical_name, "^Alameda.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "Alameda County", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "Alta Bates.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alta Bates"), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*Broadway.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Bayhill.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bayhill"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Bishop Ranch.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Bishop Ranch"), "Bishop Ranch", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*Burlingame.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain L[A-Z]* ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain (?=B)", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, " \\(unspecified\\)", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "Capitol Corridor", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Childrens Hospital.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Childrens Hospital"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "CPMC.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CPMC"), "CPMC", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Crocker Park.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Crocker Park"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "CSU.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "CSU"), "CSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "Dumbarton Express", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Estuary.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Estuary Crossing"), "City of Alameda", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Facebook.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Facebook"), "Facebook", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairfield and Suisun Transit \\(FAST\\)"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Fairmont.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairmont Hospital"), "Fairmont Hospital", canonical_operator)) %>%
           
  mutate(canonical_name = str_replace(canonical_name, "Foster City.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Foster City"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Genentech.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Genentech"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Golden Gate Transit"), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Muni (Route )?", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Muni"), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = ifelse(canonical_operator == "SF Muni", 
                                     str_replace(canonical_name, "(<=?[0-9]){1,2} ", "-"),
                                     canonical_name))

%>%
 
  mutate(canonical_name = str_replace(canonical_name, "Harbor Bay.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Harbor Bay Shuttle"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Highland Hospital.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Kaiser.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "LBL.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin Transit Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Marin Transit"), "Marin Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Mariners.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Mariners"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Monterey-Salinas Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Monterey-Salinas"), "Monterey-Salinas Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Oyster.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Oyster"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "PresidiGo.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Rio Vista Delta Breeze Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Rio Vista Delta"), "Rio Vista Delta", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SamTrans"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Joaquin"), "San Joaquin", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Leandro.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Cruz Metro"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Rosa City[ ]?Bus Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Santa Rosa City"), "Santa Rosa City", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Seton Medical.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Seton Medical"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*SFGH.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFGH"), "SFGH", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, ".*SFSU.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SolTrans"), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Sierra Point.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Sierra Point"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Stanford.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta Transit Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Tri Delta Transit"), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "UC Berkeley.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UC Berkeley"), "UC Berkeley", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "UCSF.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Union City"), "Union City", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Utah Grand.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Utah Grand"), "Utah Grand", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route 29 ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VINE Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "VINE"), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA Route 902", "902 Light Rail")) %>%
  mutate(canonical_name = str_replace(canonical_name, "(?<=^VTA.{0,20}):.*", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "West Berkeley.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WestCat", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Wheels .?LAVTA.? Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LAVTA"), "LAVTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Yahoo.*", "Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Yahoo"), "Yahoo", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))
  
bart_routes <- bart_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "BART",
         year = 2015) %>%
  select(survey, year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()

standard_routes <- left_join(sf_muni_routes,bart_routes, by = c("canonical_name", "canonical_operator")) %>%
  bind_rows(right_join(sf_muni_routes,bart_routes, by = c("canonical_name", "canonical_operator"))) %>%
  filter(is.na(survey.x) | is.na(survey.y))
  
  
  
  
  
  
  
  
  

# write.csv(sf_muni_routes, "standard_route_names.csv")
  
  
  