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
    group_by(ID, survey_name, survey_year, survey_tech, survey_variable) %>% 
    summarise(count = n())
  
  mult_ref_count  <- ref_count %>%
    filter(count > 1) %>%
    nrow()
  
  stopifnot(mult_ref_count == 0)
  
}

# see https://github.com/BayAreaMetro/modeling-website/wiki/TransitModes
TECHNOLOGY_OPTIONS <- c(
  "commuter rail",
  "heavy rail",
  "light rail",
  "ferry",
  "express bus",
  "local bus"
)
# Reads the survey data and transforms it according to the given data dictionary
# Parameters:
# * p_survey_name: typically operator when surveys are operator-based, but "Regional Snapshot" was done in 2023
# * p_survey_year: year in which survey was conducted
# * p_operator: the operator for the survey; 
#         If NA is passed, canonical_operator must be included as a variable in the survey data
# * p_default_tech: the predominant tech for the survey; one of TECHNOLOGY_OPTIONS
#         If NA is passed, survey_tech must be included as a variable in the survey data
# * p_file_path: the path of the survey data file to read
# * p_variable_dictionary: the data dictionary with columns
#        survey_name, survey_year, survey_variable, survey_response, generic_variable, generic_response
#
# Returns dictionary with columns: 
#  ID, survey_name, survey_year, canonical_oeprator, survey_tech,
#  survey_variable, survey_response, 
read_survey_data <- function(
  p_survey_name, 
  p_survey_year, 
  p_operator,
  p_default_tech, 
  p_file_path, 
  p_variable_dictionary)
{
  # filter to the rows relevant to this survey dataset
  p_variable_dictionary <- filter(p_variable_dictionary,
    (survey_name == p_survey_name) & (survey_year == p_survey_year))

  # this is a vector of the survey variable names
  variables_vector <- p_variable_dictionary$survey_variable %>% unique()

  input_df <- read.csv(p_file_path, header = TRUE, comment.char = "", quote = "\"")
  print(paste("Read",nrow(input_df),"rows from",p_file_path))

  if (is.na(p_default_tech) & is.na(p_operator)) {
    # make sure survey_tech & canonical_operator is explicitly included in the dataset
    stopifnot("survey_tech" %in% colnames(input_df))
    stopifnot("canonical_operator" %in% colnames(input_df))

    # include survey_tech & canonical_operator in variables_vector even if it's not in the dictionary
    if (!"survey_tech" %in% variables_vector) {
      variables_vector <- c(variables_vector, "survey_tech")
    }
    if (!"canonical_operator" %in% variables_vector) {
      variables_vector <- c(variables_vector, "canonical_operator")
    }
  } else if (!is.na(p_default_tech) & !is.na(p_operator)) {
    # both are specified, thisis ok
  } else {
    # on is NA and the other isn't -- this isn't supported
    abort(message="read_survey_data(): Mix of NA and non-NA not supported for p_default_tech and p_operator")
  }

  # TODO: why is check_dropped_variables() commented out?
  df_variable_levels <- input_df %>%
    gather(survey_variable, survey_response) %>%
    group_by(survey_variable, survey_response) %>%
    summarise(count = n()) %>%
    select(-count) %>% 
    ungroup()
  
  external_variable_levels <- p_variable_dictionary %>%
    filter(generic_response != "NONCATEGORICAL")
  
  # check_dropped_variables(df_variable_levels, 
  #                         external_variable_levels)
  
  # select to variables vector
  return_df <- input_df %>%
    select(one_of(variables_vector)) %>%
    rename_at(vars(contains('id')), ~sub('id', 'ID', .))
  
  if (is.na(p_default_tech) & is.na(p_operator)) {
    # survey_tech is included as survey_variable so exclude from the gather
    return_df <- return_df %>%
      gather(survey_variable, survey_response, -c("ID", "survey_tech", "canonical_operator")) %>%
      mutate(ID = as.character(ID),
             survey_name = p_survey_name,
             survey_year = p_survey_year)
  } else {
    return_df <- return_df %>%
      gather(survey_variable, survey_response, -ID) %>%
      mutate(ID = as.character(ID),
             survey_name        = p_survey_name,
             survey_year        = p_survey_year,
             canonical_operator = p_operator,
             survey_tech        = p_default_tech)
  }
  
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
    
    # update the value of the labeling to "TRUE" if the technology exists
    survey_data_df[new_col_name][survey_data_df[i] == technology] = TRUE 
  }
  
  print(table(survey_data_df[new_col_name], useNA = 'ifany'))
  
  return(survey_data_df)
}
