
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

standard_routes <- data.frame()

sf_muni <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

bart <- read.csv(bart_path) %>%
  rename_all(tolower)

# sf_cols <- colnames(sf_muni %>% select_at(vars(contains("transfer"))))
# sf_cols <- c(sf_cols, colnames(sf_muni %>% select_at(vars(contains("route")))))
sf_muni <- sf_muni %>%
  select_at(vars(contains("route"))) %>%
  select_at(vars(-contains("lat"))) %>%
  select_at(vars(-contains("lon"))) %>%
  select_at(vars(-contains("code")))

sf_muni_routes <- sf_muni %>% 
  gather(variable, entry) %>%
  # select(entry) %>%
  unique() %>% 
  filter(entry != "") %>%
  mutate(canonical_name = entry) %>%
  mutate(canonical_operator = "") %>%
  mutate(canonical_operator = ifelse(str_detect(entry, fixed("missing", ignore_case = TRUE)), "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(entry == "-", "Missing", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^[0-9]"), "SF Muni", canonical_operator)) %>% 
  mutate(canonical_operator = ifelse(str_detect(entry, "^[A-Z]+-"), "SF Muni", canonical_operator)) %>% 
  
  mutate(canonical_name = ifelse(str_detect(entry, "^MUNI "), 
                            str_replace(entry, "^MUNI ", ""), 
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, "^MUNI "), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = ifelse(str_detect(entry, " \\[ INBOUND \\]"),
                            str_replace(entry, " \\[ INBOUND \\]", ""),
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, " \\[ INBOUND \\]"), "SF Muni", canonical_operator)) %>%
  mutate(canonical_name = ifelse(str_detect(entry, " \\[ OUTBOUND \\]"),
                            str_replace(entry, " \\[ OUTBOUND \\]", ""),
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, " \\[ OUTBOUND \\]"), "SF Muni", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^AC "), 
                            str_replace(entry, "^AC ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, "^AC "), "AC", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Alcatraz "), 
                            str_replace(entry, "^Alcatraz ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, "^Alcatraz "), "Alcatraz", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Altamont Commuter Express \\(ACE\\) "),
                            str_replace(entry, "^Altamont Commuter Express \\(ACE\\) ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator  = ifelse(str_detect(entry, "^Altamont Commuter Express \\(ACE\\)"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(entry, "^Angel Island"), canonical_name, canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, fixed("BART", ignore_case = TRUE)), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Blue & Gold "), 
                            str_replace(entry, "^Blue & Gold ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Blue & Gold "), "Blue & Gold", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Burlingame"), "Burlingame", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, fixed("Caltrain", ignore_case = TRUE)), "Caltrain", canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, fixed("Caltrain", ignore_case = TRUE)), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Capitol Corridor "), 
                            str_replace(entry, "^Capitol Corridor ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Capitol Corridor "), "Capitol Corridor", canonical_operator)) %>%

  mutate(canonical_name = ifelse(str_detect(entry, "^County Connection "), 
                            str_replace(entry, "^County Connection ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^County Connection "), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Emery "), 
                            str_replace(entry, "^Emery ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Emery "), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Facebook"), "Facebook", canonical_operator)) %>%  
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Fairfield and Suisun Transit \\(FAST\\) "), 
                            str_replace(entry, "^Fairfield and Suisun Transit \\(FAST\\) ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "FAST"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Genentech"), "Caltrain", canonical_operator)) %>%
    
  mutate(canonical_name = ifelse(str_detect(entry, "^Golden Gate "), 
                            str_replace(entry, "^Golden Gate ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Golden Gate "), "Golden Gate", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Harbor Bay"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Livermore Amadore "), 
                            str_replace(entry, "^Livermore Amadore ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Livermore Amadore "), "Livermore Amadore", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Marin[ ]*"), 
                            str_replace(entry, "^Marin[ ]*", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Marin[ ]*"), "Marin", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "PresidiGo Shuttles"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^SamTrans "), 
                            str_replace(entry, "^SamTrans ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^SamTrans "), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^San Francisco Bay Ferry "), 
                            str_replace(entry, "^San Francisco Bay Ferry ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^San Francisco Bay Ferry "), "San Francisco Bay Ferry", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Stanford "), 
                            str_replace(entry, "^Stanford ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Stanford "), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^SolTrans "), 
                            str_replace(entry, "^SolTrans ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^SolTrans "), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Tri Delta "), 
                            str_replace(entry, "^Tri Delta ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Tri Delta "), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^UCSF "), 
                            str_replace(entry, "^UCSF ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^UCSF "), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^Union City "), 
                            str_replace(entry, "^Union City ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Union City "), "Union City", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^VINE 29 "), 
                            str_replace(entry, "^VINE 29 ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^VINE "), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^VTA "), 
                            str_replace(entry, "^VTA ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^VTA "), "VTA", canonical_operator)) %>%
  
  mutate(canonical_name = ifelse(str_detect(entry, "^WestCAT "), 
                            str_replace(entry, "^WestCAT ", ""),
                            canonical_name)) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^WestCAT "), "WestCAT", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

bad_references <- sf_muni_routes %>% 
  filter(canonical_operator == "BAD REFERENCE")

sf_muni_routes <- sf_muni_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "SFMTA",
         year = 2014) %>%
  select(survey, year, entry, canonical_name, canonical_operator, -variable) %>%
  unique()

standard_routes <- sf_muni_routes %>%
  select(canonical_operator, canonical_name) %>%
  unique() %>% 
  arrange(canonical_operator, canonical_name)

# BART
transfer_names <- bart %>%
  select_at(vars(contains("trnsfr"))) %>%
  select_at(vars(-contains("agency"))) %>%
  colnames()


bart_routes <- bart %>% 
  select(one_of(transfer_names)) %>%
  gather(variable, value = entry) %>%
  filter(entry != "") %>%
  unique()
  
bart_routes <- bart_routes %>%
  mutate(canonical_name = entry) %>%
  mutate(canonical_operator = "") %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "illogical"), "Missing", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(entry, "^AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^AC Transit Route "), "AC", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(entry, "^ACE ta", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^ACE "), "ACE", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_name = str_replace(entry, "Alameda County ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Alameda County"), "Alameda County", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(entry, "Alta Bates"), "AC", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(entry, "Apple ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Apple"), "Apple", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(entry, "Alameda County ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Alameda County"), "Alameda County", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Broadway"), "AC", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Bayhill"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Bishop Ranch"), "Bishop Ranch", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain L[A-Z]* ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain (?=B)", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^Caltrain (?=\\()", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Caltrain"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "^Capitol Corridor"), "Capitol Corridor", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Childrens Hospital"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(entry, "County Connection ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "County Connection"), "County Connection", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "CPMC"), "CPMC", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Crocker Park"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "CSU"), "CSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "^Dumbarton Express"), "Dumbarton Express", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Estuary Crossing"), "City of Alameda", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Facebook"), "Facebook", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Fairfield and Suisun Transit \\(FAST\\)"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Foster City"), "Caltrain MTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Genentech ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Genentech"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate Transit ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Golden Gate Transit"), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Muni ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Muni"), "SF Muni", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Harbor Bay Shuttle"), "Harbor Bay", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Marin Transit", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Marin Transit"), "Marin", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Mariners"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Monterey-Salinas Transit ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Monterey-Salinas"), "Monterey-Salinas Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Oyster"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "PresidiGo"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Rio Vista Delta ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Rio Vista Delta"), "Rio Vista Delta", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "SamTrans"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "SFGH"), "SFGH", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "San Joaquin"), "San Joaquin", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "SamTrans"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Cruz Metro ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Santa Cruz Metro"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Santa Rosa City[ ]?Bus ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Santa Rosa City"), "Santa Rosa City", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(entry, "Seton Medical"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "SolTrans"), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Sierra Point"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta Transit ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Tri Delta Transit"), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "UC Berkeley"), "UC Berkeley", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "Union City"), "Union City", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Utah Grand"), "Utah Grand", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VINE ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "VINE"), "Napa Vine", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(entry, "WestCAT"), "WestCat", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(entry, "Yahoo"), "Yahoo", canonical_operator)) 
  
  
  
  
  
  
  
  
  
  
  

# write.csv(sf_muni_routes, "standard_route_names.csv")
  
  
  