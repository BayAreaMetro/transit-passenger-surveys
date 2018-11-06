##################################################################################################
### Script to create a database of canonical route and station names
### Author: John Helsel, October 2018
##################################################################################################

# Libraries and optins
library(tidyverse)
library(sf)
# library(cluster)
options(stringsAsFactors = FALSE)

# User check to assign proper paths for input data and writes

user_list <- data.frame(
  
  user = c("helseljw", 
           "USDO225024"), 
  path = c("../../Data and Reports/", 
           "~/GitHub/onboard-surveys/Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

# Input data paths
ac_transit_path <- paste0(dir_path,
  "AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_ADD_STANDARD_VARS.csv")

bart_path <- paste0(dir_path,
  "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")

caltrain_path <- paste0(dir_path, 
  "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE NO POUND OR SINGLE QUOTE.csv")

sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

canonical_station_path <- paste0(dir_path,
  "Geography Files/Passenger_Railway_Stations_2018.shp")

standard_route_path <- "standard_route_crosswalk.csv"
canonical_route_path <- "canonical_route_names.csv"

# Read raw survey files
ac_transit_raw <- read.csv(ac_transit_path) %>% 
  rename_all(tolower)

bart_raw <- read.csv(bart_path) %>%
  rename_all(tolower)

caltrain_raw <- read.csv(caltrain_path) %>%
  rename_all(tolower)

sf_muni_raw <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

canonical_station <- st_read(canonical_station_path)


# Rename rail trips to be station to station
# Functions
# JWH: Should the filter for rail operator line become a parameter?
get_nearest_station <- function(station_names, survey_records, 
                                route_name, lat_name, lon_name) {

  # station_names <- station_names
  # survey_records <- survey_records
  # route_name <- "final_trip_first_route"
  # lat_name <- board_lat
  # lon_name <- board_lon
  
  list_of_cols <- c("id", route_name, lat_name, lon_name)
  list_of_col_names <- c("id", "route_name", "lat", "lon")
  
  route_df <- survey_records %>%
    select(one_of(list_of_cols))
  colnames(route_df) <- list_of_col_names
  route_df <- route_df %>%
    filter(route_name != "" & !str_detect(route_name, "Missing"))
  
  route_df <- st_as_sf(x = route_df,
                         coords = c("lon", "lat"),
                         crs = "+proj=longlat +datum=WGS84") %>%
    filter(route_name %in% c("BART"))
  
  closest_neighbor <-  data.frame(index = 0) 
  closest_neighbor <- closest_neighbor %>% 
    filter(index !=0) 
  distances <- route_df %>% 
    st_distance(station_names)
  
  for (i in 1:nrow(distances)) { 
    temp_index = which.min(distances[i,])
    temp_df <- data.frame(index = temp_index)
    closest_neighbor <- bind_rows(closest_neighbor, 
                              temp_df)}
  
  st_geometry(route_df) <- NULL 
  route_df <- route_df %>%
    bind_cols(closest_neighbor) %>%
    left_join(station_names, by = "index") %>%
    select(id, station = station_na)
  
  return(route_df)
}


get_rail_names <- function(station_names, survey_records, route_name, 
                           board_lat, board_lon, alight_lat, alight_lon) {

  station_names <- canonical_station
  survey_records <- sf_muni_raw %>%
    select(id,
            "final_trip_to_third_route",
            "final_transfer_to_third_boarding_lat",
            "final_transfer_to_third_boarding_lon",
            "final_transfer_to_third_alighting_lat",
            "final_transfer_to_third_alighting_lon")
  route_name <- "final_trip_first_route"
  board_lat <- "final_transfer_to_third_boarding_lat"
  board_lon <- "final_transfer_to_third_boarding_lon"
  alight_lat <- "final_transfer_to_third_alighting_lat"
  alight_lon <- "final_transfer_to_third_alighting_lon"
  
  if(survey_records %>% filter(!!route_name == "BART") %>% nrow() > 0) {
  board_names <- get_nearest_station(station_names, survey_records, route_name,
                                     board_lat, board_lon)  
  
  alight_names <- get_nearest_station(station_names, survey_records, route_name,
                                     alight_lat, alight_lon)  
  
  combined_names <- board_names %>% 
    left_join(alight_names, by = "id") %>% 
    mutate(full_name = paste("BART", station.x, station.y, sep = "---")) %>%
    select(id, full_name)
  
  # survey_records <- survey_records %>%
  mutate_exp <- paste0("ifelse(", route_name, " == 'BART', full_name, ", route_name, ")")

  temp <- survey_records %>%
    left_join(combined_names, by = "id") %>%
    mutate_(full_name = mutate_exp) %>%
    select(id, full_name)
  
  survey_records <- survey_records %>% 
    left_join(temp, by = "id") %>%
    mutate(!!route_name := full_name) %>% 
    select(-full_name)
  }
  
  return(survey_records)
  }

# Create index of stations
canonical_station <- canonical_station %>%
  select(station_na, mode) %>%
  mutate(index = 1:nrow(canonical_station))

# Create 
# sf_muni_rail <- sf_muni_raw %>%
#   select_at(vars(matches("(^id$)|(transfer_(from|to))|(final_trip)"))) %>%
#   select(-matches("(code)|(wait)")) 

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_first_route",
                       "final_transfer_from_first_boarding_lat",
                       "final_transfer_from_first_boarding_lon",
                       "final_transfer_from_first_alighting_lat",
                       "final_transfer_from_first_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_second_route",
                       "final_transfer_from_second_boarding_lat",
                       "final_transfer_from_second_boarding_lon",
                       "final_transfer_from_second_alighting_lat",
                       "final_transfer_from_second_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_third_route",
                       "final_transfer_from_third_boarding_lat",
                       "final_transfer_from_third_boarding_lon",
                       "final_transfer_from_third_alighting_lat",
                       "final_transfer_from_third_alighting_lon")

# !!! JWH: I think there should be a column "final_trip_fourth_route", but it 
# !!!      doesn't seem to exist.
# sf_muni_raw <- get_rail_names(canonical_station, 
#                        sf_muni_raw, 
#                        "final_trip_fourth_route",
#                        "final_transfer_from_fourth_boarding_lat",
#                        "final_transfer_from_fourth_boarding_lon",
#                        "final_transfer_from_fourth_alighting_lat",
#                        "final_transfer_from_fourth_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_to_first_route",
                       "final_transfer_to_first_boarding_lat",
                       "final_transfer_to_first_boarding_lon",
                       "final_transfer_to_first_alighting_lat",
                       "final_transfer_to_first_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_to_second_route",
                       "final_transfer_to_second_boarding_lat",
                       "final_transfer_to_second_boarding_lon",
                       "final_transfer_to_second_alighting_lat",
                       "final_transfer_to_second_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_to_third_route",
                       "final_transfer_to_third_boarding_lat",
                       "final_transfer_to_third_boarding_lon",
                       "final_transfer_to_third_alighting_lat",
                       "final_transfer_to_third_alighting_lon")

sf_muni_raw <- get_rail_names(canonical_station, 
                       sf_muni_raw, 
                       "final_trip_to_fourth_route",
                       "final_transfer_to_fourth_boarding_lat",
                       "final_transfer_to_fourth_boarding_lon",
                       "final_transfer_to_fourth_alighting_lat",
                       "final_transfer_to_fourth_alighting_lon")

# dave noodle start ------------------------------------------------------------

# make_replacements <- function(routes_df, replace_df) {
#   
#   # start with for loop, iterate to make better(full join?)
#   regex_vector <- replace_df$route_name_regex
#   operator_vector <- replace_df$operator_name
#   prefix_vector <- replace_df$operator_prefix
#   
#   return_df <- routes_df %>%
#     #survey_name?
#     mutate(canonical_name = survey_name,
#            canonical_operator = "")
#   
#   for(index in 1:length(regex_vector)) {
#     
#     regex_item <- replace_df$route_name_regex[index]
#     operator_item <- replace_df$operator_name[index]
#     prefix_item <- replace_df$operator_prefix[index]
#     
#     if (prefix_item) {
#       
#       return_df <- return_df %>%
#         mutate(canonical_name = str_replace(canonical_name, regex_item, "")) %>%
#         mutate(canonical_operator = ifelse(str_detect(survey_name, regex_item), operator_item, canonical_operator))
#       
#     } else {
#       
#       return_df <- return_df %>%
#         mutate(canonical_operator = ifelse(str_detect(survey_name, operator_item), operator_item, canonical_operator))
#         
#     }
#   
#     
#   }
#   
#   return(return_df)
#   
# }
# 
# key_var_list <- bart_raw %>%
#   select_at(vars(contains("trnsfr"))) %>%
#   select_at(vars(-contains("agency"))) %>%
#   colnames()
# 
# bart_routes_df <- bart_raw %>% 
#   select(one_of(key_var_list)) %>%
#   gather(variable, value = survey_name) %>%
#   filter(survey_name != "") %>%
#   unique() 
# 
# replacement_df <- data.frame(
#   route_name_regex = c("^AC Transit Route ", 
#                        "^ACE ",
#                        NA),
#   
#   operator_name = c("AC Transit", 
#                     "ACE",
#                     "AirTrain"),
#   
#   operator_prefix = c(TRUE,
#                       TRUE,
#                       FALSE)
# )
# 
# test_df <- make_replacements(bart_routes_df, replacement_df)
# 
# table(test_df$canonical_operator)

# dave noodle end --------------------------------------------------------------

# Adjust route names within AC Transit survey



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
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(SCMTD|^Santa Cruz)"), "Santa Cruz Metro", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(canonical_operator == "", "BAD REFERENCE", canonical_operator))

caltrain_routes <- caltrain_routes %>%
  filter(canonical_operator != "BAD REFERENCE") %>%
  mutate(survey = "Caltrain",
         survey_year = 2014) %>%
  select(survey, survey_year, survey_name, canonical_name, canonical_operator, -variable) %>%
  unique()
  
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
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, fixed("^BART", ignore_case = TRUE)), "BART", canonical_operator)) %>%
  
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





# # dave pass start --------------------------------------------------------------
# get_some_lat_lngs <- function(raw_df, var_route, var_lat, var_lng, portion_string) {
#   
#   vars <- c(route = var_route, 
#             lat = var_lat,
#             lng = var_lng)
#   
#   return_df <- raw_df %>%
#     select(id, vars) %>%
#     mutate(portion = portion_string) %>%
#     filter(route %in% c("BART"))
#   
#   return(return_df)
#   
#   
# }

# working_df <- get_some_lat_lngs(sf_muni_raw,
#                                 "final_trip_to_first_route",
#                                 "final_transfer_to_first_alighting_lat",
#                                 "final_transfer_to_first_alighting_lon",
#                                 "to_first_alighting") %>%
#   bind_rows(get_some_lat_lngs(sf_muni_raw,
#                               "final_trip_to_second_route",
#                               "final_transfer_to_second_alighting_lat",
#                               "final_transfer_to_second_alighting_lon",
#                               "to_second_alighting")) %>%
#   bind_rows(get_some_lat_lngs(sf_muni_raw,
#                               "final_trip_to_third_route",
#                               "final_transfer_to_third_alighting_lat",
#                               "final_transfer_to_third_alighting_lon",
#                               "to_third_alighting")) %>%
#   bind_rows(get_some_lat_lngs(sf_muni_raw,
#                               "final_trip_first_route",
#                               "final_transfer_from_first_boarding_lat",
#                               "final_transfer_from_first_boarding_lon",
#                               "from_first_boarding")) %>%
#   bind_rows(get_some_lat_lngs(sf_muni_raw,
#                               "final_trip_second_route",
#                               "final_transfer_from_second_boarding_lat",
#                               "final_transfer_from_second_boarding_lon",
#                               "from_second_boarding")) %>%
#   bind_rows(get_some_lat_lngs(sf_muni_raw,
#                               "final_trip_third_route",
#                               "final_transfer_from_third_boarding_lat",
#                               "final_transfer_from_third_boarding_lon",
#                               "from_third_boarding"))

  

# table(working_df$portion)
# table(working_df$route)

# Create canonical list of station names/locations
# canonical_station <- st_read(canonical_station_path)
# 
# canonical_coordinates <- as.data.frame(st_coordinates(canonical_station)) %>%
#   rename(lat = Y,
#          lon = X)
# 
# canonical_station <- bind_cols(canonical_station, canonical_coordinates)
# 
# st_geometry(canonical_station) <- NULL
# 
# sf_muni_lat <- sf_muni_raw %>% 
#   select(id) %>% 
#   bind_cols(sf_muni_raw %>% 
#               select_at(vars(contains("lat"))) %>% 
#               select(-hisp_lat_spa_code)) 
# sf_muni_lat <- sf_muni_lat %>%
#   gather(variable, value = "lat", -id) %>%
#   rename(var_name = variable) %>%
#   mutate(var_name = str_replace(var_name, "_lat", ""))
# 
# sf_muni_lon <- sf_muni_raw %>% 
#   select(id) %>% 
#   bind_cols(sf_muni_raw %>% 
#               select_at(vars(contains("lon")))) 
# sf_muni_lon <- sf_muni_lon %>%
#   gather(variable, value = "lon", -id) %>%
#   rename(var_name = variable) %>%
#   mutate(var_name = str_replace(var_name, "_lon", ""))
# 
# sf_muni_coords <- sf_muni_lat %>% 
#   left_join(sf_muni_lon, by = c("id", "var_name")) %>%
#   filter(str_detect(var_name, "final")) %>%
#   mutate(lat = as.numeric(lat),
#          lon = as.numeric(lon)) %>%
#   filter(!is.na(lat) & !is.na(lon)) %>% 
#   #Correct one bad record with sign reversed
#   mutate(lon = ifelse(lon > 0, lon * -1, lon)) %>%
#   select(lat, lon) %>% 
#   unique()

# Create primary key of id, trip route name, and give lat/long at each end
# Join trip route name to standard_routes for route/operator
# Filter on rail operators
# Use the resulting lat/long to feed the cluster approach




# pair each with station


# working_df <- working_df %>%
#   mutate(lat = as.numeric(lat),
#          lng = as.numeric(lng))
# 
# for_clara_df <- working_df %>%
#   select(lat, lon = lng)
# 
# clara_results <- clara(for_clara_df,
#                        k = nrow(filter(canonical_station, agencyname == "BART")),
#                        metric = "euclidean",
#                        rngR = TRUE,
#                        pamLike = TRUE)
# 
# medoids_df <- as.data.frame(clara_results$medoids) %>%
#   bind_cols(., as.data.frame(clara_results$clusinfo)) %>%
#   bind_cols(., data.frame(cluster = seq(1:nrow(medoids_df)))) %>%
#   select(cluster, 
#          cluster_lat = lat, 
#          cluster_lng = lon,
#          cluster_size = size,
#          max_diss,
#          av_diss,
#          isolation)
# 
# results_df <- bind_cols(working_df, data.frame(cluster = clara_results$clustering)) %>%
#   left_join(., medoids_df, by = c("cluster")) %>%
#   mutate(error = sqrt((lat - cluster_lat)**2 + (lng - cluster_lng)**2))
# 
# write.csv(results_df, file = "have_a_look.csv", row.names = FALSE)



# next:
# how to build routes? start with just the boarding location?
  
# dave pass end ----------------------------------------------------------------








set.seed(123)
# stat_locations <- clara(sf_muni_coords,
#                         k = 673, 
#                         metric = "euclidean",
#                         rngR = TRUE,
#                         pamLike = TRUE)

round_stations <- data.frame(stat_locations$medoids) %>%
  mutate(lat = round(lat, 4),
         lon = round(lon, 4)) %>%
  mutate(station = 1:nrow(stat_locations$medoids))

# write.csv(round_stations, "clara_station_locations.csv")

canonical_station %>%
  mutate(lat = round(lat, 4),
         lon = round(lon, 4)) %>%
  left_join(round_stations, by = c("lat", "lon")) %>% 
  filter(!is.na(station))


  
write.csv(standard_routes, standard_route_path)
write.csv(canonical_routes, canonical_route_path)  
  