# Parameters
OPERATOR_DELIMITER <-  "___"
ROUTE_DELIMITER <- "&&&"

# Geocode Functions
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
  # route_name_string <- "final_trip_from_first_route"
  # lat_name_string <- "final_transfer_from_first_boarding_lat"
  # lon_name_string <- "final_transfer_from_first_boarding_lon"
  # 
  # operator_key_string <- "BART"

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
    select(id, station_na) %>%
    mutate()
  
  return(return_df)
}


get_rail_names <- function(station_names_shp, 
                           survey_records_df, 
                           operator, 
                           route_name,
                           board_lat, 
                           board_lon, 
                           alight_lat, 
                           alight_lon) {
  
  
  # station_names_shp <- canonical_station_shp
  # operator <- "BART"
  # survey_records_df <- input_df
  # route_name <- "final_trip_first_route"
  # board_lat <- "final_transfer_from_first_boarding_lat"
  # board_lon <- "final_transfer_from_first_boarding_lon"
  # alight_lat <- "final_transfer_from_first_alighting_lat"
  # alight_lon <- "final_transfer_from_first_alighting_lon"
  
  filter_expression <- paste0(route_name, " == '", operator, "'")
  
  number_of_relevant_records <- survey_records_df %>%
    filter(filter_expression) %>%
    nrow()
  
  if(number_of_relevant_records > 0) {
    
    board_names <- get_nearest_station(station_names_shp, survey_records_df, operator, 
                                       route_name, board_lat, board_lon)  
    
    alight_names <- get_nearest_station(station_names_shp, survey_records_df, operator,
                                        route_name, alight_lat, alight_lon)  
    
    combined_names <- board_names %>% 
      left_join(alight_names, by = "id") %>% 
      mutate(full_name = paste0(operator, OPERATOR_DELIMITER, station_na.x, ROUTE_DELIMITER, station_na.y)) %>%
      select(id, full_name)
    
    mutate_exp <- paste0("ifelse(", route_name, " == '", operator, "', full_name, ", route_name, ")")
    
    temp_df <- survey_records_df %>%
      left_join(combined_names, by = "id") %>%
      mutate(full_name = mutate_exp) %>%
      select(id, full_name)
    
    return_df <- survey_records_df %>% 
      left_join(temp_df, by = "id") %>%
      mutate(!!route_name := full_name) %>% 
      select(-full_name)
    
  } else {
    return_df <- survey_records_df
  }
  
  return(return_df)
}

# Read Survey Functions
check_dropped_variables <- function(operator_variables_df, external_variables_df) {
  
  # Ensure that all variable levels in the operator specific survey have a generic 
  # equivalent in dictionary_all
  # operator_variables_df <- df_variable_levels
  # external_variables_df <- external_variable_levels
  
  external_levels <- external_variables_df %>%
    group_by(survey_variable, survey_response) %>%
    summarise(count = n()) %>%
    select(-count) %>%
    ungroup()
  
  missing_variables <- operator_variables_df %>%
    filter(survey_variable %in% external_levels$survey_variable) %>%
    filter(!survey_response %in% external_levels$survey_response) %>%
    nrow()
  
  stopifnot(missing_variables == 0)
  
}

check_duplicate_variables <- function(df_duplicates) {
  
  # Check for duplicate rows in dataframe
  ref_count <- df_duplicates %>% 
    group_by(ID, operator, survey_year, survey_tech, survey_variable) %>% 
    summarise(count = n())
  
  mult_ref_count  <- ref_count %>%
    filter(count > 1) %>%
    nrow()
  
  stopifnot(mult_ref_count == 0)
  
}

read_operator <- function(name, 
                          year, 
                          default_tech, 
                          file_path, 
                          variable_dictionary, 
                          rail_names_df,
                          canonical_shp) {
  # 
  # name <- 'AC Transit'
  # year <- 2018
  # default_tech <- 'local bus'
  # file_path <- f_actransit_survey_path
  # variable_dictionary <- dictionary_all
  # rail_names_df <- rail_names_inputs_df
  # canonical_shp <- canonical_station_shp
  
  variables_vector <- variable_dictionary %>%
    filter(operator == name) %>%
    .$survey_variable %>%
    unique()
  
  input_df <- read.csv(file_path, header = TRUE, comment.char = "", quote = "\"") 
  
  updated_df <- input_df
  
  if (name %in% rail_names_df$survey_name) {
    
    relevant_rail_names_df <- rail_names_df %>% 
      filter(survey_name == name)
    
    for (i in 1:nrow(relevant_rail_names_df)) {
      
      updated_df <- get_rail_names(canonical_shp, 
                                   updated_df,
                                   relevant_rail_names_df$operator_string[[i]],
                                   relevant_rail_names_df$route_string[[i]],
                                   relevant_rail_names_df$board_lat[[i]],
                                   relevant_rail_names_df$board_lon[[i]],
                                   relevant_rail_names_df$alight_lat[[i]],
                                   relevant_rail_names_df$alight_lon[[i]])
    }
  } 
  
  df_variable_levels <- updated_df %>%
    gather(survey_variable, survey_response) %>%
    group_by(survey_variable, survey_response) %>%
    summarise(count = n()) %>%
    select(-count) %>% 
    ungroup()
  
  external_variable_levels <- variable_dictionary %>%
    filter(operator == name & generic_response != "NONCATEGORICAL")
  
  # check_dropped_variables(df_variable_levels, 
  #                         external_variable_levels)
  
  return_df <- updated_df %>%
    select(one_of(variables_vector)) %>%
    rename_at(vars(contains('id')), funs(sub('id', 'ID', .))) %>%
    gather(survey_variable, survey_response, -ID) %>%
    mutate(ID = as.character(ID),
           operator = name,
           survey_year = year,
           survey_tech = default_tech)
  
  check_duplicate_variables(return_df)
  
  return(return_df)
  
}

## Method library for standardization
# Set Operator Name
set_operator_name <- function(input_vector) {
  input_df <- as.data.frame(input_vector) %>%
    rename(input_field = input_vector)
  
  output_df <- input_df %>%
    mutate(output_field = "None") %>%
    mutate(output_field = str_extract(input_field, "^[A-Za-z- ]*"))
}

# Deprecated Set Operator Name
# set_operator_name <- function(input_vector){
#   
#   input_df <- as.data.frame(input_vector) %>%
#     rename(input_field = input_vector)
#   
#   output_df <- input_df %>%
#     mutate(output_field = "None") %>%
#     
#     # BART, AMTRAK, and Caltrain may be named in route names, so do first, then overwrite
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Amtrak", ignore_case = TRUE)), "AMTRAK", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("BART", ignore_case = TRUE)), "BART", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Caltrain", ignore_case = TRUE)), "CALTRAIN", output_field)) %>%
#     
#     # Transit Agencies
#     mutate(output_field = ifelse(str_detect(input_field, fixed("AC ", ignore_case = TRUE)), "AC TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("ACE", ignore_case = TRUE)), "ACE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("AC Transit", ignore_case = TRUE)), "AC TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("AirBART", ignore_case = TRUE)), "AC TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("AirTrain", ignore_case = TRUE)), "BART", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Capitol Corridor", ignore_case = TRUE)), "AMTRAK", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("County Connection", ignore_case = TRUE)),"COUNTY CONNECTION", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Fairfield and", ignore_case = TRUE)),"FAIRFIELD-SUISUN",output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Golden Gate ", ignore_case = TRUE)), "GOLDEN GATE TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Golden Gate Tran", ignore_case = TRUE)), "GOLDEN GATE TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Golden Gate Ferry", ignore_case = TRUE)), "GOLDEN GATE FERRY", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Marin Transit", ignore_case = TRUE)), "MARIN TRANSIT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Muni", ignore_case = TRUE)), "MUNI", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Rio Vista Delta Breeze", ignore_case = TRUE)), "RIO-VISTA", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("SamTrans", ignore_case = TRUE)), "SAMTRANS", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Sam Trans", ignore_case = TRUE)), "SAMTRANS", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Santa Rosa CityBus", ignore_case = TRUE)), "SANTA ROSA CITYBUS", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("SF Bay Ferry", ignore_case = TRUE)), "SF BAY FERRY", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Soltrans", ignore_case = TRUE)), "SOLTRANS", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Tri Delta", ignore_case = TRUE)), "TRI-DELTA", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Union City", ignore_case = TRUE)), "UNION CITY", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("WestCAT", ignore_case = TRUE)), "WESTCAT", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("VINE", ignore_case = TRUE)), "NAPA VINE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("VTA", ignore_case = TRUE)), "VTA", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("DASH Downtown Area Shuttle", ignore_case = TRUE)), "VTA", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Tiburon FERRY Tiburon To Angel Island State Park", ignore_case = TRUE)), "BLUE GOLD FERRY", output_field)) %>%
#     
#     # Correct AC route with 'union city' in the name
#     mutate(output_field = ifelse(str_detect(input_field, fixed("AC 200 Union City BART", ignore_case = TRUE)), "AC TRANSIT", output_field)) %>%
#     
#     # PRIVATE SHUTTLE
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Stanford", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Commuter shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Corinthian lines shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Caltrain- Shuttles Genentech", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Kaiser shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("San Jose airport shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("American Canyon Safeway", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Shuttles Broadway - Millbrae", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Genentech Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Sierra Point/Brisbane Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("San Francisco General Hospital (SFGH) Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Seton Medical Center Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("UCSF Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("CPMC Hospital Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("West Berkeley Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Highland Hospital Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("San Francisco General Hospital (SFGH) Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Seton Medical Center Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Oyster Point Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Utah Grand Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Childrens Hospital Oakland", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Apple Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Facebook Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Alta Bates Shuttles", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Harbor Bay Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Alameda County employee shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("San Leandro LINKS", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("North Burlingame shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Fairmont Hospital / Juvenile Justice Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Yahoo Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Bayhill Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Bishop Ranch Shuttle", ignore_case = TRUE)), "PRIVATE SHUTTLE", output_field)) %>%
#     
#     # OTHER
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Calistoga Handy Van", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Lake Transit", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Saint Helena Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Burlingame Trolley Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("SCMTD Highway 17", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Santa Cruz Metro", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Menlo Park Shuttle Midday Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Palo Alto E Embarcadero Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("CSU East Bay Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("UC Berkeley Campus Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Shuttle - other or unspecified", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Unknown", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("LBL / Lawrence Berkeley Lab Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Agency provided, but route not spec", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("San Francisco State (SFSU) Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("PresidiGO", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Emery Go-Round", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("B on Broadway", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Estuary Crossing - College of Alameda Shuttle", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Other", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Monterey-Salinas Transit", ignore_case = TRUE)), "OTHER", output_field)) %>%
#     
#     # Vague 'Ferry'
#     mutate(output_field = ifelse(str_detect(input_field, fixed("Ferry", ignore_case = TRUE)), "SF BAY FERRY", output_field)) %>%
#     mutate(output_field = ifelse(is.na(input_field), NA, output_field))
#   
#   return(output_df$output_field)
# }


