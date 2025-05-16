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
  
  if (mult_ref_count > 0) {
    print("Found duplicate rows in dataframe:")
    print(ref_count %>% filter(count > 1))
  }
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

# Add data from PUMS datasets by survey year for background population comparisons

create_PUMS_data_in_TPS_format <- function(survey_year,inflation_year){
  suffix <- substr(survey_year, 3, 4)  # Extract last two digits from survey year
  
  pums_path <- file.path("M:/Data/Census/PUMS", 
                         paste0("PUMS ", survey_year))
  
  hh_file     <- file.path(pums_path, paste0("hbayarea", suffix, ".Rdata"))
  person_file <- file.path(pums_path, paste0("pbayarea", suffix, ".Rdata"))
  
  # Check file existence for PUMS HH and person files, load files if available
  if (!file.exists(hh_file)) {
    stop(glue("Household PUMS file not found: {hh_file}"))
  }
  
  if (!file.exists(person_file)) {
    stop(glue("Person PUMS file not found: {person_file}"))
  }
  
  hh_obj_name     <- load(hh_file)
  person_obj_name <- load(person_file)
  
  hh_pums         <- get(hh_obj_name)
  person_pums     <- get(person_obj_name) 
  
  # Survey years prior to 2020 have a different variable name for TYPEHUGQ (household vs. GQ type): TYPE; make them match
  if (survey_year < 2020 && "TYPE" %in% names(hh_pums)) {
    hh_pums <- hh_pums %>% rename(TYPEHUGQ = TYPE)
  }
  
  # Keep relevant PUMS variables
  # Recode value of NA income for GQ (people under 15) to 0
  hh_pums <- hh_pums %>% 
    select(SERIALNO,TYPEHUGQ,HINCP) 
  
  person_pums <- person_pums %>% 
    select(SERIALNO,PINCP,ADJINC,RAC1P,HISP,PWGTP) %>% 
    mutate(PINCP=if_else(is.na(PINCP),0,PINCP))
  
  # Inflate incomes to "inflation_year" passed into the function
  # Bring in CPI table from MTC modeling Wiki
  # Keep rows from 2010 and later and rename for local use
  url <- "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"
  page <- read_html(url)
  inflation_table <- page %>%
    html_node("table") %>%
    html_table() %>%
    select(CPI_year = Year, CPI_2010_Ref = "Consumer Price Index(2010 Reference)") %>%
    filter(CPI_year >= 2010)
  
  print("inflation_table:")
  print(inflation_table)
  
  # Check and extract CPI value
  CPI_row <- inflation_table %>%
    filter(CPI_year == !!inflation_year)
  
  survey_row <- inflation_table %>%
    filter(CPI_year == !!survey_year)
  
  if (nrow(CPI_row) == 0) {
    stop(glue("No CPI value found for inflation_year: {inflation_year}")) # Check if inflation_year is in table, survey_year definitely is
  }
  
  # CPI is year inflated to, survey year is inflated from, CPI_ratio reflects this
  CPI_placeholder    <- CPI_row$CPI_2010_Ref
  survey_placeholder <- survey_row$CPI_2010_Ref
  CPI_ratio = CPI_placeholder / survey_placeholder
  
  # Print inflation information for review
  print(glue("CPI_placeholder {inflation_year}: {CPI_placeholder}"))
  print(glue("survey_placeholder {survey_year}: {survey_placeholder}"))
  print(glue("CPI ratio: {CPI_ratio}"))
  
  # Create categories to match with TPS
  # For income, use PINCP for non-institutional group quarters and HINCP for households
  combined <- left_join(person_pums, hh_pums, by = "SERIALNO") %>% 
    filter(TYPEHUGQ %in% c(1, 3)) %>% 
    mutate( 
      race = case_when(
        RAC1P==1  ~ "WHITE",
        RAC1P==2  ~ "BLACK",
        RAC1P==3  ~ "OTHER",
        RAC1P==4  ~ "OTHER",
        RAC1P==5  ~ "OTHER",
        RAC1P==6  ~ "ASIAN",
        RAC1P==7  ~ "OTHER",
        RAC1P==8  ~ "OTHER",
        RAC1P==9  ~ "OTHER",
        TRUE      ~ "Mistaken coding"
      ),
      hispanic = case_when(
        HISP==1   ~ "NOT HISPANIC/LATINO OR OF SPANISH ORIGIN",
        HISP>1    ~ "HISPANIC/LATINO OR OF SPANISH ORIGIN",
        TRUE      ~ "Mistaken coding"
      ),
      temp_income = case_when(
        TYPEHUGQ==3          ~ PINCP,
        TYPEHUGQ==1          ~ HINCP
      ),
      temp2_income = temp_income * ADJINC/1000000 * CPI_ratio,
      household_income = case_when(
        temp2_income < 50000                             ~ "under $50,000",
        temp2_income >= 50000 & temp2_income  < 100000   ~ "$50,000 to $99,999",
        temp2_income >= 100000 & temp2_income < 150000   ~ "$100,000 to $149,999",
        temp2_income >= 150000 & temp2_income < 200000   ~ "$150,000 to $200,000",
        temp2_income >= 200000                           ~ "$200,000 or higher",
      )
    )
  
  # Race and income summary
  # Append pums source and inflation_year (just for income) columns for later joining
  
  race <- combined %>% 
    group_by(race,hispanic) %>% 
    summarize(weight=sum(PWGTP),.groups = "drop") %>% 
    mutate(
      source=paste(survey_year,"pums1")
    )
  
  income <- combined %>% 
    group_by(household_income) %>% 
    summarize(weight=sum(PWGTP),.groups = "drop") %>% 
    mutate(
      source=paste(survey_year,"pums1"),
      inflation_year=!!inflation_year
    )
  
  # Combine datasets for export
  
  joined <- bind_rows(race,income)
  
  # Return the final dataframe
  return(joined)
}
