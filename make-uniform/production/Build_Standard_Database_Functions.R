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


# get_rail_names <- function(station_names_shp, 
#                            survey_records_df, 
#                            operator, 
#                            route_name,
#                            board_lat, 
#                            board_lon, 
#                            alight_lat, 
#                            alight_lon) {
#   
#   
#   # station_names_shp <- canonical_station_shp
#   # operator <- "BART"
#   # survey_records_df <- input_df
#   # route_name <- "final_trip_first_route"
#   # board_lat <- "final_transfer_from_first_boarding_lat"
#   # board_lon <- "final_transfer_from_first_boarding_lon"
#   # alight_lat <- "final_transfer_from_first_alighting_lat"
#   # alight_lon <- "final_transfer_from_first_alighting_lon"
#   
#   filter_expression <- paste0(route_name, " == '", operator, "'")
#   
#   number_of_relevant_records <- survey_records_df %>%
#     filter(eval(parse(text = filter_expression))) %>%
#     nrow()
#   
#   if(number_of_relevant_records > 0) {
#     
#     board_names <- get_nearest_station(station_names_shp, survey_records_df, operator, 
#                                        route_name, board_lat, board_lon)  
#     
#     alight_names <- get_nearest_station(station_names_shp, survey_records_df, operator,
#                                         route_name, alight_lat, alight_lon)  
#     
#     combined_names <- board_names %>% 
#       left_join(alight_names, by = "id") %>% 
#       mutate(full_name = paste0(operator, OPERATOR_DELIMITER, station_na.x, ROUTE_DELIMITER, station_na.y)) %>%
#       select(id, full_name)
#     
#     mutate_exp <- paste0("ifelse(", route_name, " == '", operator, "', full_name, ", route_name, ")")
#     
#     temp_df <- survey_records_df %>%
#       left_join(combined_names, by = "id") %>%
#       mutate(full_name = eval(parse(text = mutate_exp))) %>%
#       select(id, full_name)
#     
#     return_df <- survey_records_df %>% 
#       left_join(temp_df, by = "id") %>%
#       mutate(!!route_name := full_name) %>% 
#       select(-full_name)
#     
#   } else {
#     return_df <- survey_records_df
#   }
#   
#   return(return_df)
# }


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
                         # rail_names_df,
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
    filter((operator == name) & (survey_year == year)) %>%
    .$survey_variable %>%
    unique()
  
  input_df <- read.csv(file_path, header = TRUE, comment.char = "", quote = "\"") 
  
  updated_df <- input_df
  
  # if (name %in% rail_names_df$survey_name) {
  #   
  #   relevant_rail_names_df <- rail_names_df %>% 
  #     filter(survey_name == name)
  #   
  #   for (i in 1:nrow(relevant_rail_names_df)) {
  #     
  #     updated_df <- get_rail_names(canonical_shp, 
  #                                  updated_df,
  #                                  relevant_rail_names_df$operator_string[[i]],
  #                                  relevant_rail_names_df$route_string[[i]],
  #                                  relevant_rail_names_df$board_lat[[i]],
  #                                  relevant_rail_names_df$board_lon[[i]],
  #                                  relevant_rail_names_df$alight_lat[[i]],
  #                                  relevant_rail_names_df$alight_lon[[i]])
  #   }
  # } 
  # 
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


## Function to add a labeling column indicating if certain technology 
## is present in each tour of the survey
technology_present <- function(survey_data_df,
                               technology,
                               new_col_name) {
  
  # input: - dataframe of survey responses
  #        - technology string
  #        - name of the new column in string format
  # output: updated dateframe with a new column indicating
  #         if the technology is available
  
  transfer_tech_cols = c('first_before_technology',
                         'second_before_technology',
                         'third_before_technology', 
                         'first_after_technology',
                         'second_after_technology',
                         'third_after_technology')
  
  survey_data_df[new_col_name] = FALSE
  
  for (i in transfer_tech_cols){
    # check if the transfer technology column exists
    stopifnot(i %in% colnames(survey_data_df))
    
    # update the value of the labeling to "TRUE" if the technology exisit
    survey_data_df[new_col_name][survey_data_df[i] == technology] = TRUE 
  }
  
  print(table(survey_data_df[new_col_name], useNA = 'ifany'))
  
  return(survey_data_df)
}
