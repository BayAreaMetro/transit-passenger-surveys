
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

standard_routes <- data.frame()

sf_muni <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

# sf_cols <- colnames(sf_muni %>% select_at(vars(contains("transfer"))))
# sf_cols <- c(sf_cols, colnames(sf_muni %>% select_at(vars(contains("route")))))
sf_muni <- sf_muni %>%
  select_at(vars(contains("route"))) %>%
  select_at(vars(-contains("lat"))) %>%
  select_at(vars(-contains("lon"))) %>%
  select_at(vars(-contains("code")))

unique_routes <- sf_muni %>% 
  gather(variable, entry) %>%
  # select(entry) %>%
  unique() %>% 
  filter(entry != "") %>%
  mutate(base_name = entry) %>%
  mutate(operator = "") %>%
  mutate(operator = ifelse(str_detect(entry, fixed("missing", ignore_case = TRUE)), "Missing", operator)) %>%
  mutate(operator = ifelse(entry == "-", "Missing", operator)) %>%
  mutate(operator = ifelse(str_detect(entry, "^[0-9]"), "MUNI", operator)) %>% 
  mutate(operator = ifelse(str_detect(entry, "^[A-Z]+-"), "MUNI", operator)) %>% 
  
  mutate(base_name = ifelse(str_detect(entry, "^MUNI "), 
                            str_replace(entry, "^MUNI ", ""), 
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, "^MUNI "), "MUNI", operator)) %>%
  mutate(base_name = ifelse(str_detect(entry, " \\[ INBOUND \\]"),
                            str_replace(entry, " \\[ INBOUND \\]", ""),
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, " \\[ INBOUND \\]"), "MUNI", operator)) %>%
  mutate(base_name = ifelse(str_detect(entry, " \\[ OUTBOUND \\]"),
                            str_replace(entry, " \\[ OUTBOUND \\]", ""),
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, " \\[ OUTBOUND \\]"), "MUNI", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^AC "), 
                            str_replace(entry, "^AC ", ""),
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, "^AC "), "AC", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Alcatraz "), 
                            str_replace(entry, "^Alcatraz ", ""),
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, "^Alcatraz "), "Alcatraz", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Altamont Commuter Express \\(ACE\\) "),
                            str_replace(entry, "^Altamont Commuter Express \\(ACE\\) ", ""),
                            base_name)) %>%
  mutate(operator  = ifelse(str_detect(entry, "^Altamont Commuter Express \\(ACE\\)"), "ACE", operator)) %>%

  mutate(operator = ifelse(str_detect(entry, "^Angel Island"), base_name, operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Apple"), "Apple", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Blue & Gold "), 
                            str_replace(entry, "^Blue & Gold ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Blue & Gold "), "Blue & Gold", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Burlingame"), "Burlingame", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Caltrain"), "Caltrain", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Capitol Corridor "), 
                            str_replace(entry, "^Capitol Corridor ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Capitol Corridor "), "Capitol Corridor", operator)) %>%

  mutate(base_name = ifelse(str_detect(entry, "^County Connection "), 
                            str_replace(entry, "^County Connection ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^County Connection "), "County Connection", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Emery "), 
                            str_replace(entry, "^Emery ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Emery "), "Emery", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Facebook"), "Facebook", operator)) %>%  
  
  mutate(base_name = ifelse(str_detect(entry, "^Fairfield and Suisun Transit \\(FAST\\) "), 
                            str_replace(entry, "^Fairfield and Suisun Transit \\(FAST\\) ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "FAST"), "FAST", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Genentech"), "Genentech", operator)) %>%
    
  mutate(base_name = ifelse(str_detect(entry, "^Golden Gate "), 
                            str_replace(entry, "^Golden Gate ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Golden Gate "), "Golden Gate", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "Harbor Bay"), "Harbor Bay", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "LBL"), "LBL", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Livermore Amadore "), 
                            str_replace(entry, "^Livermore Amadore ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Livermore Amadore "), "Livermore Amadore", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Marin[ ]*"), 
                            str_replace(entry, "^Marin[ ]*", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Marin[ ]*"), "Marin[ ]*", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "PresidiGo Shuttles"), "PresidiGo", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^SamTrans "), 
                            str_replace(entry, "^SamTrans ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^SamTrans "), "SamTrans", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^San Francisco Bay Ferry "), 
                            str_replace(entry, "^San Francisco Bay Ferry ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^San Francisco Bay Ferry "), "San Francisco Bay Ferry", operator)) %>%
  
  mutate(operator = ifelse(str_detect(entry, "SFSU"), "SFSU", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Stanford "), 
                            str_replace(entry, "^Stanford ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Stanford "), "Stanford", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^SolTrans "), 
                            str_replace(entry, "^SolTrans ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^SolTrans "), "SolTrans", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Tri Delta "), 
                            str_replace(entry, "^Tri Delta ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Tri Delta "), "Tri Delta", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^UCSF "), 
                            str_replace(entry, "^UCSF ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^UCSF "), "UCSF", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^Union City "), 
                            str_replace(entry, "^Union City ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^Union City "), "Union City", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^VINE 29 "), 
                            str_replace(entry, "^VINE 29 ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^VINE "), "Napa Vine", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^VTA "), 
                            str_replace(entry, "^VTA ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^VTA "), "VTA", operator)) %>%
  
  mutate(base_name = ifelse(str_detect(entry, "^WestCAT "), 
                            str_replace(entry, "^WestCAT ", ""),
                            base_name)) %>%
  mutate(operator = ifelse(str_detect(entry, "^WestCAT "), "WestCAT", operator)) %>%
  
  mutate(operator = ifelse(operator == "", "BAD REFERENCE", operator))

unique_routes <- unique_routes %>%
  mutate(survey = "SFMTA") %>%
  select(survey, entry, base_name, operator, -variable) %>%
  unique()


  
  
  