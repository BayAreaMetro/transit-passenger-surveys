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

read_operator <- function(name, year, default_tech, file_path, variable_dictionary) {
  
  # name <- 'AC Transit'
  # year <- 2018
  # default_tech <- 'local bus'
  # file_path <- f_actransit_survey_path
  # variable_dictionary <- dictionary_all
  
  variables_vector <- variable_dictionary %>%
    filter(operator == name) %>%
    .$survey_variable %>%
    unique()
  
  input_df <- read.csv(file_path, header = TRUE, comment.char = "", quote = "\"") 
  
  if (name %in% rail_names_inputs$survey_name) {
    inputs <- rail_names_inputs %>% 
      filter(survey_name_df == name)
    
    for (i in 1:nrow(rail_names_inputs %>% filter(survey_name_df == name))) {
      
      input_df <- get_rail_names(canonical_station_shp, 
                                 input_df,
                                 inputs$operator_string[[i]],
                                 inputs$route_string[[i]],
                                 inputs$board_lat[[i]],
                                 inputs$board_lon[[i]],
                                 inputs$alight_lat[[i]],
                                 inputs$alight_lon[[i]])}}
  
  df_variable_levels <- input_df %>%
    gather(survey_variable, survey_response) %>%
    group_by(survey_variable, survey_response) %>%
    summarise(count = n()) %>%
    select(-count) %>% 
    ungroup()
  
  external_variable_levels <- variable_dictionary %>%
    filter(operator == name & generic_response != "NONCATEGORICAL")
  
  # check_dropped_variables(df_variable_levels, 
  #                         external_variable_levels)
  
  return_df <- input_df %>%
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