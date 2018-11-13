##################################################################################################
### Script to create a database of canonical route and station names
### Author: John Helsel, October 2018
##################################################################################################

# Libraries and optins
library(tidyverse)
library(rlang)
library(sf)
library(geosphere)
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

# Crosswalk Paths

get_rail_names_inputs_path <- "get_rail_names_inputs.csv"

# Input data paths
ac_transit_path <- paste0(dir_path,
  "AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_ADD_STANDARD_VARS.csv")

bart_path <- paste0(dir_path,
  "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")

caltrain_path <- paste0(dir_path, 
  "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE NO POUND OR SINGLE QUOTE.csv")

marin_path <- paste0(dir_path,
  "Marin Transit/Final Data/marin transit_data file_finalreweighted043018.csv")
  
sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

canonical_station_path <- paste0(dir_path,
  "Geography Files/Passenger_Railway_Stations_2018.shp")

standard_route_path <- "standard_route_crosswalk.csv"
canonical_route_path <- "canonical_route_names.csv"

# Read crosswalk files

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

marin_raw_df <- read.csv(marin_path) %>% 
  rename_all(tolower)

sf_muni_raw_df <- read.csv(sf_muni_path) %>%
  rename_all(tolower)

canonical_station_shp <- st_read(canonical_station_path)


# Rename rail trips to be station to station
# Functions
sfc_as_cols <- function(x, geometry, names = c("x", "y")) {
  if (missing(geometry)) {
    geometry <- st_geometry(x)
  } else {
    geometry <- eval_tidy(enquo(geometry), x)
  }
  stopifnot(inherits(x, "sf") && inherits(geometry, "sfc_POINT"))
  ret <- st_coordinates(geometry)
  ret <- as_tibble(ret)
  stopifnot(length(names) == ncol(ret))
  x <- x[ , !names(x) %in% names]
  ret <- setNames(ret,names)
  bind_cols(x, ret)
}

# JWH: Should the filter for rail operator line become a parameter?
get_nearest_station <- function(station_names_df, survey_records_df, operator_key_string,
                                route_name_string, lat_name_string, lon_name_string) {

  # testing names
  # station_names_df <- station_names
  # survey_records_df <- survey_records
  # route_name_string <- "final_trip_to_first_route"
  # lat_name_string <- "final_transfer_to_first_boarding_lat"
  # lon_name_string <- "final_transfer_to_first_boarding_lon"
  # 
  # operator_key_string <- "BART"
  temp_tech_key_string <- "Rapid Rail"
  
  vars <- c(route = route_name_string,
            lat = lat_name_string,
            lng = lon_name_string)
  
  relevant_records_df <- survey_records_df %>%
    select(id, vars) %>%
    filter(route == operator_key_string) %>%
    mutate(lat = as.numeric(lat),
           lng = as.numeric(lng))
  
  relevant_stations_df <- station_names_df %>%
    filter(agencyname == operator_key_string) %>%
    sfc_as_cols(st_geometry(.), c("sta_lon", "sta_lat")) %>%
    select(index, sta_lon, sta_lat)
  
  st_geometry(station_names_df) <- NULL
  st_geometry(relevant_stations_df) <- NULL

  working_df <- merge(relevant_records_df, relevant_stations_df) %>%
    mutate(distance_meters = mapply(function(r_lng, r_lat, s_lng, s_lat)
      distm(c(r_lng, r_lat), c(s_lng, s_lat), fun = distHaversine), lng, lat, sta_lon, sta_lat))

  return_df <- working_df %>%
    group_by(id) %>%
    mutate(min_distance = min(distance_meters)) %>%
    ungroup() 
  
  # Stop function if minimum distance exceeds threshold
  # stopifnot(return_df$min_distance %>% max() < 1000)
    
  return_df <- return_df %>% 
    filter(distance_meters == min_distance) %>%
    left_join(., station_names_df, by = c("index")) %>%
    mutate(station_na = ifelse(min_distance > 500, "MISSING", station_na)) %>%
    select(id, station_na)
  
  return(return_df)
}

get_rail_names <- function(station_names, survey_records, operator, route_name,
                           board_lat, board_lon, alight_lat, alight_lon) {

  # station_names <- canonical_station_shp
  # survey_records <- ac_transit_raw_df #%>%
  # # select(id,
  # #         "final_trip_first_route",
  # #         "final_transfer_from_first_boarding_lat",
  # #         "final_transfer_from_first_boarding_lon",
  # #         "final_transfer_from_first_alighting_lat",
  # #         "final_transfer_from_first_alighting_lon")
  # operator <- "BART"
  # route_name <- "final_trip_first_route"
  # board_lat <- "final_transfer_from_first_boarding_lat"
  # board_lon <- "final_transfer_from_first_boarding_lon"
  # alight_lat <- "final_transfer_from_first_alighting_lat"
  # alight_lon <- "final_transfer_from_first_alighting_lon"

  filter_check <- paste0(route_name, " == '", operator, "'")
  
  if(survey_records %>% filter_(filter_check) %>% nrow() > 0) {
  board_names <- get_nearest_station(station_names, survey_records, operator, 
                                     route_name, board_lat, board_lon)  
  
  alight_names <- get_nearest_station(station_names, survey_records, operator,
                                      route_name, alight_lat, alight_lon)  
  
  combined_names <- board_names %>% 
    left_join(alight_names, by = "id") %>% 
    mutate(full_name = paste(operator, station_na.x, station_na.y, sep = "---")) %>%
    select(id, full_name)
  
  mutate_exp <- paste0("ifelse(", route_name, " == '", operator, "', full_name, ", route_name, ")")

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
canonical_station_shp <- canonical_station_shp %>%
  select(station_na, agencyname, mode) %>%
  mutate(index = 1:nrow(canonical_station_shp))

# AC Transit Route Name Replacements
inputs <- get_rail_names_inputs %>% 
  filter(survey_name_df == "ac_transit")

for (i in 1:nrow(get_rail_names_inputs %>% filter(survey_name_df == "ac_transit"))) {
  
  ac_transit_raw_df <- get_rail_names(canonical_station_shp, 
                                      ac_transit_raw_df,
                                      inputs$operator_string[[i]],
                                      inputs$route_string[[i]],
                                      inputs$board_lat[[i]],
                                      inputs$board_lon[[i]],
                                      inputs$alight_lat[[i]],
                                      inputs$alight_lon[[i]])
}

# BART does not fit with column scheme from other surveys (and is lacking lat/lon
#   coordinates for most transfers). I will manually recode 
bart_raw_df <- bart_raw_df %>% 
  mutate(route = paste("BART", first_entered_bart, bart_exit_station, sep = "---")) %>%
  # Replace unknown records with imputed values
  mutate(access_trnsfr_list1 = ifelse(access_trnsfr_list1 == "Unknown", access_trnsfr_list1_imputed, access_trnsfr_list1),
         access_trnsfr_list2 = ifelse(access_trnsfr_list2 == "Unknown", access_trnsfr_list2_imputed, access_trnsfr_list2),
         access_trnsfr_list3 = ifelse(access_trnsfr_list3 == "Unknown", access_trnsfr_list3_imputed, access_trnsfr_list3),
         egress_trnsfr_list1 = ifelse(egress_trnsfr_list1 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list1),
         egress_trnsfr_list2 = ifelse(egress_trnsfr_list2 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list2),
         egress_trnsfr_list3 = ifelse(egress_trnsfr_list3 == "Unknown", egress_trnsfr_list2_imputed, egress_trnsfr_list3)) %>% 
  # Replace Caltrain records with missing.
  mutate(access_trnsfr_list1 = ifelse(str_detect(access_trnsfr_list1, "Caltrain"), "CALTRAIN---MISSING---MISSING", access_trnsfr_list1),
         access_trnsfr_list2 = ifelse(str_detect(access_trnsfr_list2, "Caltrain"), "CALTRAIN---MISSING---MISSING", access_trnsfr_list2),
         access_trnsfr_list3 = ifelse(str_detect(access_trnsfr_list3, "Caltrain"), "CALTRAIN---MISSING---MISSING", access_trnsfr_list3),
         egress_trnsfr_list1 = ifelse(str_detect(egress_trnsfr_list1, "Caltrain"), "CALTRAIN---MISSING---MISSING", egress_trnsfr_list1),
         egress_trnsfr_list2 = ifelse(str_detect(egress_trnsfr_list2, "Caltrain"), "CALTRAIN---MISSING---MISSING", egress_trnsfr_list2),
         egress_trnsfr_list3 = ifelse(str_detect(egress_trnsfr_list3, "Caltrain"), "CALTRAIN---MISSING---MISSING", egress_trnsfr_list3))

# Caltrain create internal Route
caltrain_raw_df <- caltrain_raw_df %>%
  mutate(route = paste("CALTRAIN", enter_station, exit_station, sep = "---"))

# SF Muni Route Name Replacements
# Not sure why there's no final_trip_fourth_route.
# For now, it is removed from the list of columns to check
inputs <- get_rail_names_inputs %>% 
  filter(survey_name_df == "sf_muni")

for (i in 1:nrow(inputs)) {
  
  sf_muni_raw_df <- get_rail_names(canonical_station_shp, 
                                      sf_muni_raw_df,
                                      inputs$operator_string[[i]],
                                      inputs$route_string[[i]],
                                      inputs$board_lat[[i]],
                                      inputs$board_lon[[i]],
                                      inputs$alight_lat[[i]],
                                      inputs$alight_lon[[i]])
}

# Adjust route names within AC Transit survey

transfer_names <- ac_transit_raw_df %>%
  select(matches("final_trip")) %>%
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
  mutate(canonical_name = str_replace_all(canonical_name, "  ", " ")) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "(?<=^AC Transit [0-9A-Z]{1,5} )- ", "")) %>%
  mutate(canonical_name = str_replace(canonical_name, "^AC Transit ", "")) %>%
mutate(canonical_name = str_replace(canonical_name, "Dtn. Oakland /Dtn. Berkeley/4th St. Harrison", "Berkeley BART to Downtown Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Pablo & Monroe/Berkeley/Merritt BART", "University Village Albany to Montclair")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fruitvale Ave/Alameda/11th M.L.K. Jr Wy", "Dimond District Oakland to downtown Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fruitvale Ave/Alameda/Oakland Airport", "Dimond Dist. to Oakland Airport")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Union Landing - Frmt. Blvd. - Ohlone", "Ohlone College to Union Landing Shopping Center")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fremont BART-Newpark Mall-Pacific Commons", "Fremont BART to NewPark Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fremont BART-Mission-Warm Springs-Industrial Area", "Fremont BART to Gateway Blvd. & Lakeside Pkwy.")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Frmt BART - Mission - Milpitas - Alder", "Fremont BART to Great Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward Bart -South Hayward Bart - Chabot", "Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fremont BART-U.C. BART-Mission-Ohlone Newark", "Fremont BART to New Park Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fremont BART-Warm Springs", "Fremont BART to Warm Springs Blvd. & Dixon Landing")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fremont BART-Mowry-Thornton", "Fremont BART to NewPark Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward Bart-C.V. Bart-Hwd. Bart-Cherryland", "Hayward BART to Castro Valley BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "HWD. BART-WHITMAN-SO. HWD. BART", "Hayward BART to South Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "North Richmond Shuttle", " El Cerrito Del Norte BART to Richmond Parkway Transit Center")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fruitvale Bart/Skyline High School", "Fruitvale BART to Skyline High School")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Dtn. Oakland/Eastmont T.C./Bayfair Bart", " Downtown Oakland to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Eastmont T. C./Foothill Sq.", "Eastmont Transit Center to Foothill Square Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Coliseum Bart/Knowland Zoo", "Coliseum BART to Oakland Zoo")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Coliseum Bart/Golf Links Rd-Dunkirk Ave", "Coliseum BART - Mountain - Golf Links Rd - Dunkirk Ave")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fruitvale Bart/Maxwell Park/Div. 4.", "Fruitvale BART to Maxwell Park")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Bayfair BART- Castro Valley- Hayward bart", "Hayward BART to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Oakland/Alameda/Fruitvale Bart", "Rockridge BART to Fruitvale BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "RockRidge - 3RUN", "Rockridge BART to Berkeley Amtrak")) %>%
  mutate(canonical_name = str_replace(canonical_name, "U.C. Village - U.C. Campus", "University Village to UC Campus (Berkeley BART).")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Fruitvale Bart/Merritt College", "Fruitvale BART to Merritt College")) %>%
  mutate(canonical_name = str_replace(canonical_name, "CSUEB /HAYWARD BART SHUTTLE", "Hayward BART to Cal State East Bay")) %>%
  mutate(canonical_name = str_replace(canonical_name, "W. Oakland Bart/Fruitvale Bart", "West Oakland BART to Fruitvale BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Richmond Pkwy. TC. - Richmond BART", "Richmond BART to Richmond Pkwy. Transit Center")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Richmond Pkwy TC - El Cerrito Plaza BART", "El Cerrito Plaza BART to Richmond Parkway Transit Center")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Richmond - Downtown Oakland", "Hilltop Mall to Oakland Amtrak")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Richmond - Downtown Oakland", "Point Richmond to Oakland Amtrak")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Pablo Rapid BUS", "San Pablo Rapid -- Contra Costa College to Jack London Square")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Eastmont T.C./Oakland Airport", "Eastmont Transit Center to Oakland Airport")) %>%
  mutate(canonical_name = str_replace(canonical_name, "CCC/Richmond BART/Ford Pt.", "Marina Bay Richmond to San Pablo Dam Rd. El Sobrante")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Bayfair Bart-Washington Manor-S. L. Bart", "San Leandro BART to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hilltop Mall - CCC - del Norte BART", "El Cerrito del Norte BART to Hilltop Mall")) %>%
  mutate(canonical_name = str_replace(canonical_name, "OWL/Dtn. Oakland/Oakland Airport", "All Nighter. Downtown Oakland to Oakland Airport")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward BART - S. Hayward BART", "Hayward BART to South Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward BART - S. Hayward BART", "Hayward BART to South Hayward BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Berkeley BART - Downtown Oakland", "Berkeley BART to Lake Merritt BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward Bart-Bayfair Bart-San Lorenzo", "Hayward BART to Bay Fair BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward Bart-Hayward Hills", "Hayward BART to Hayward High School.")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Kelly Hill - Hayward BART", "Hayward BART to Fairview District")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Coliseum Bart/98th Ave/Eastmont T.C.", "Coliseum BART Edgewater Dr.")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hwd.BART- UC Bart - Fmt Bart", "Bay Fair BART to Fremont BART")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco - Piedmont", "Highland Ave. Piedmont")) %>%
  mutate(canonical_name = str_replace(canonical_name, "DB1 Dumbarton Express", "DB1 Union City to Deer Creek Rd.")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco - Claremont -Parkwood", "Caldecott Ln. Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco - El Sobrante", "Hilltop Dr. Park & Ride")) %>%
  mutate(canonical_name = str_replace(canonical_name, "Hayward BART- Hillsdale Mall- Oracle", "Hayward BART to Oracle")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco/Eastmont T.C.", "Eastmont Transit Center Oakland")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco/Castro Valley", "Castro Valley Park & Ride")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco/Alameda/Fruitvale Bart", "Park Ave. & Encinal Ave. Alameda")) %>%
  mutate(canonical_name = str_replace(canonical_name, "San Francisco - San Lorenzo- Hayward", "Eden Shores Hayward")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AC Transit"), "AC Transit", canonical_operator)) %>%
  
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Alameda County"), "Alameda County", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "Apple.*", "Apple Shuttle")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Apple"), "Apple", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Broadway"), "AC Transit", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^BART---", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^CALTRAIN---", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "County Connection Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "County Connection"), "County Connection", canonical_operator)) %>%

  mutate(canonical_name = str_replace(canonical_name, "^Capitol Corridor.*", "Capitol Corridor")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Capitol Corridor"), "Capitol Corridor", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "DHS"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Dumbarton Express Route", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Dumbarton Express"), "Dumbarton Express", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Emery ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Emery"), "Emeryville MTA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Fairfield and Suisun Transit \\(FAST\\) Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Fairfield and Suisun Transit \\(FAST\\)"), "FAST", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Golden Gate ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Golden Gate"), "Golden Gate Transit", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Highland Hospital"), "Highland Hospital", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Kaiser"), "Kaiser", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "LBL"), "LBL", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^MUNI ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "MUNI"), "SF Muni", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "PresidiGo"), "PresidiGo", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SamTrans (Route )?", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SamTrans"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^San Francisco Bay Ferry ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^San Francisco Bay Ferry "), "San Francisco Bay Ferry", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "San Leandro"), "SLTMO", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SFSU"), "SFSU", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^SolTrans Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "SolTrans"), "SolTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Sierra Point"), "SamTrans", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Stanford Marguerite"), "Stanford", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Tri Delta", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Tri Delta"), "Tri Delta", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UC Berkeley"), "UC Berkeley", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "UCSF"), "UCSF", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Union City Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Union City"), "Union City", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^VTA ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^VTA"), "VTA", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "West Berkeley"), "Berkeley Gateway TMA", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^WestCAT Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "WestCAT"), "WestCAT", canonical_operator)) %>%
  
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
  
  mutate(canonical_name = str_replace_all(canonical_name, "  ", " ")) %>%
  mutate(canonical_name = str_replace(canonical_name, " $", "")) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "AC Transit Route ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "AC Transit"), "AC Transit", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^ACE"), "ACE", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Amtrak"), "Amtrak", canonical_operator)) %>%

  mutate(canonical_operator = ifelse(str_detect(survey_name, "^AirTrain"), "AirTrain", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "Angel Island.*", "Angel Island-Tiburon Ferry")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Angel Island"), "Angel Island-Tiburon Ferry", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "BART [A-Z]*/[A-Z]* ", "BART---")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "(?<=BART---[A-Za-z /]{1,50}) [Tt]o ", "---")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "BART---", "")) %>%
  mutate(canonical_name = str_replace_all(canonical_name, "BART - ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^BART"), "BART", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Bayview"), "Bayview", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace_all(canonical_name, "^Burlingame ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Burlingame"), "Burlingame", canonical_operator)) %>%

  mutate(canonical_name = str_replace_all(canonical_name, "^Caltrain SHUTTLE", "Shuttle")) %>% 
  mutate(canonical_name = str_replace_all(canonical_name, "^Caltrain- ", "")) %>% 
  mutate(canonical_name = str_replace_all(canonical_name, "^CALTRAIN---", "")) %>% 
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Caltrain"), "Caltrain", canonical_operator)) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^CALTRAIN"), "Caltrain", canonical_operator)) %>%
  
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
  
  mutate(canonical_name = str_replace(canonical_name, "^BART ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "BART---"), "BART", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^Blue & Gold ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "^Blue & Gold "), "Blue & Gold", canonical_operator)) %>%
  
  mutate(canonical_operator = ifelse(str_detect(survey_name, "Burlingame"), "Caltrain", canonical_operator)) %>%
  
  mutate(canonical_name = str_replace(canonical_name, "^CALTRAIN ", "")) %>%
  mutate(canonical_operator = ifelse(str_detect(survey_name, "(^CALTRAIN---)|(^Caltrain)"), "Caltrain", canonical_operator)) %>%
  
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
error_check <- left_join(bind_rows(bart_routes, caltrain_routes, sf_muni_routes), 
                         ac_transit_routes,
                         by = c("canonical_name", "canonical_operator")) %>%
  bind_rows(right_join(bind_rows(bart_routes, caltrain_routes, sf_muni_routes), 
                       ac_transit_routes, 
                       by = c("canonical_name", "canonical_operator"))) %>%
  filter(is.na(survey_name.x) | is.na(survey_name.y))

standard_routes <- bind_rows(ac_transit_routes, bart_routes, caltrain_routes, sf_muni_routes)
canonical_routes <- standard_routes %>%
  select(canonical_operator, canonical_name) %>%
  unique()




write.csv(standard_routes, standard_route_path)
write.csv(canonical_routes, canonical_route_path)  
  