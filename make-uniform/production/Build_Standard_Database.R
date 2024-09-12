## Administration

#### Purpose
# Procedure to translate any number of on-board survey data sets into a single
# dataset with common variables and common responses. In this script, we put in place
# procedures to process surveys into a standard database.  See
# `Extract Variables from Legacy Surveys.Rmd` for procedures to extract
# variables from legacy surveys (i.e., those with SAS summary scripts).
#
# This script does the following:
# 1. Reads a bunch of survey data input files. Most of these are operator-specific, but not all.
#    Each dataset needs accompanying information on how the variables are standardized,
#    which can be found in `Dictionary_for_Standard_Database.csv`
#
# 2. Clean up some messy variables
#    [todo: add more info]
#
# 3. Geocoding - for location variables (home, work and school location, 
#    trip origin and destination, survey board and alight location,
#    first board and last alight location), use spatial join to assign those
#    locations to:
#    - TM1 TAZ, TM2 TAZ, TM2 MAZ
#    - Census 2020 PUMA, county, tract
#
# 4. [More stuff here]
#
# 5. Combine with legacy data and write a bunch output files; see the README.md in
#    this directory for detail.
#
# To use these scripts, analysts must intervene at specific locations. To assist
# in this work, we've added notes where interventions are necessary.


#### Libraries

list_of_packages <- c(
  "geosphere",
  "sf",
  "tidyverse",
  "tigris",
  "reldist"
)
new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
for (p in list_of_packages){
  library(p, character.only = TRUE)
}

# The working directory is set as the location of the script. All other paths will be relative.

tryCatch(
  { wd <- paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/"); setwd(wd) },
  error = function(e) {}
)

# Define standard database functions

source("Build_Standard_Database_Functions.R")
source("Combine_Legacy_Standard_Surveys.R")

#### Parameters
OPERATOR_DELIMITER = "___"
ROUTE_DELIMITER = "&&&"

CRS_NAD83_CAZ6_FT <- 2230    # https://epsg.io/2230
CRS_WGS84 <- 4326            # https://epsg.io/4326
# Set radius of the earth for Haversine distance calculation
# https://www.space.com/17638-how-big-is-earth.html
# Distance is calculated in miles (3963.20 mi.)
# Alternate distance in meters would be 6378137 m. 
EARTH_RADIUS_MILES <- 3963.2

# Use megaregion for census tract aggregation
MEGAREGION <- c(
  "Alameda","Contra Costa","Marin","Napa","San Francisco","San Mateo","Santa Clara","Solano","Sonoma",
  "Santa Cruz","San Benito","Monterey","San Joaquin","Stanislaus","Merced","Yuba","Placer","El Dorado",
  "Sutter","Yolo","Sacramento","Lake","Mendocino"
)


# _User Intervention_
# The code uses r-user-name-specific relative directories. Add your name and your
# relative (to this directory) path to the `Data and Reports` directory to the two
# vectors in the below code block. Run `Sys.getenv('USERNAME')` to determine your R
# user name.

#### Remote file names
user_list <- data.frame(
  user = c("helseljw",
           "ywang",
           "SIsrael",
           "lzorn"),
  path = c("~/GitHub/onboard-surveys/Data and Reports",
           "M:/Data/OnBoard/Data and Reports",
           "M:/Data/OnBoard/Data and Reports", 
           "M:/Data/OnBoard/Data and Reports"
  )
)

today = Sys.Date()
TPS_SURVEY_PATH <- user_list %>%
  filter(user == Sys.getenv("USERNAME")) %>%
  .$path
TPS_SURVEY_STANDARDIZED_PATH <- file.path(
  TPS_SURVEY_PATH,
  "_data_Standardized",
  sprintf("standardized_%s",today)
)

if (!file.exists(TPS_SURVEY_STANDARDIZED_PATH)) {
  dir.create(TPS_SURVEY_STANDARDIZED_PATH)
  print(paste("Created",TPS_SURVEY_STANDARDIZED_PATH))
}

# Setup the log file
run_log <- file.path(TPS_SURVEY_STANDARDIZED_PATH,
  "Build_Standard_Database.log")
print(paste("Writing log to",run_log))
# print wide since it's to a log file
options(width = 10000)
options(dplyr.width = 10000)
options(datatable.print.nrows = 1000)
options(warn=2) # error on warning
# don't warn: "summarise()` has grouped output by ... You can override using the `.groups` argument."
options(dplyr.summarise.inform=F) 
# enable caching
options(tigris_use_cache = TRUE)

sink(run_log, append=FALSE, type = c('output', 'message'))

# Inputs - dictionary and other utils
f_dict_standard <- "Dictionary_for_Standard_Database.csv"
f_canonical_station_path <- file.path(TPS_SURVEY_PATH,"Geography Files","Passenger_Railway_Stations_2018.shp")
f_shapefile_paths <- data.frame(
  shape     = character(),
  shapefile = character(),
  shape_col = character()
)
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "tm1_taz",
  shapefile = "M:/Data/GIS layers/TM1_taz/bayarea_rtaz1454_rev1_WGS84.shp",
  shape_col = "TAZ1454",
)
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "tm2_taz",
  shapefile = "M:/Data/GIS layers/TM2_maz_taz_v2.2/tazs_TM2_v2_2.shp",
  shape_col = "taz",
)
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "tm2_maz",
  shapefile = "M:/Data/GIS layers/TM2_maz_taz_v2.2/mazs_TM2_v2_2.shp",
  shape_col = "maz",
)
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "tract_GEOID",
  shapefile = "tigris",
  shape_col = "GEOID",
)
# Initially, this used the tigris library
# But it's helpful to have the shapefiles on disk to use for joins to see other fields
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "county_GEOID",
  shapefile = "M:/Data/GIS layers/Census/2020/tl_2020_us_county/tl_2020_us_county.shp",
  shape_col = "GEOID",
)
f_shapefile_paths <- f_shapefile_paths %>% add_row(
  shape     = "PUMA_GEOID20",
  shapefile = "M:/Data/GIS layers/Census/2020/tl_2020_06_puma20/",
  shape_col = "GEOID20",  # "NAMELSAD10 is very long...
)
f_canonical_routes_path <- "canonical_route_crosswalk.csv"

# Inputs - survey data

# _User Intervention_
# When adding a new survey, the user must: add the relevant metadata for the survey below
# For surveys that provide data across multiple technologies, enter the dominant technology
# here and add route-specific # changes to the `canonical` route database 
# (e.g., SF Muni Metro routes are `light rail`)

TEST_MODE_OPERATORS = c()
print("TEST_MODE_OPERATORS:")
print(TEST_MODE_OPERATORS)

survey_input_df <- data.frame(
  survey_name      = character(),
  survey_year      = numeric(),
  operator         = character(),
  default_tech     = character(),
  raw_data_path    = character(),
  stringsAsFactors = FALSE
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'AC Transit',
  survey_year     = 2018,
  operator        = 'AC TRANSIT',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "AC Transit","2018","As CSV",
    "OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_fixTransfers_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'BART',
  survey_year     = 2015,
  operator        = 'BART',
  default_tech    = 'heavy rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "BART","As CSV",
    "BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Caltrain',
  survey_year     = 2014,
  operator        = 'Caltrain',
  default_tech    = 'commuter rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Caltrain","As CSV",
    "Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE_modifyTransfer_fixRouteNames_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'SF Muni',
  survey_year     = 2017,
  operator        = 'MUNI',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Muni","As CSV",
    "MUNI_DRAFTFINAL_20171114_fixedTransferNum_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Marin Transit',
  survey_year     = 2017,
  operator        = 'MARIN TRANSIT',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Marin Transit","As CSV",
    "marin transit_data file_final01222021_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Napa Vine',
  survey_year     = 2014,
  operator        = 'NAPA VINE',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Napa Vine","As CSV",
    "Napa Vine Transit OD Survey Data_Dec10_Submitted_toAOK_with_transforms NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'VTA',
  survey_year     = 2017,
  operator        = 'VTA',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
  TPS_SURVEY_PATH,
  "VTA","As CSV",
  "VTA_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'FAST',
  survey_year     = 2017,
  operator        = 'FAST',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Solano County","As CSV",
    "FAST_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Delta Breeze',
  survey_year     = 2017,
  operator        = 'RIO-VISTA',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Solano County","As CSV",
    "Rio Vista Delta Breeze_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'City Coach',
  survey_year     = 2017,
  operator        = 'VACAVILLE CITY COACH',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Solano County","As CSV",
    "Vacaville City Coach_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Soltrans',
  survey_year     = 2017,
  operator        = 'SOLTRANS',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Solano County","As CSV",
    "SolTrans_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Union City Transit',
  survey_year     = 2017,
  operator        = 'UNION CITY',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Union City","2017","As CSV",
    "Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'WestCAT',
  survey_year     = 2017,
  operator        = 'WESTCAT',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "WestCAT","As CSV",
    "WestCAT_addCols_recodeRoute_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Sonoma County Transit',
  survey_year     = 2018,
  operator        = 'Sonoma County Transit',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Sonoma County",
    "2018","As CSV",
    "sc transit_data file_final_spring 2018_addRoutesCols NO POUND NO SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Sonoma-Marin Area Rail Transit',
  survey_year     = 2018,
  operator        = 'SMART',
  default_tech    = 'commuter rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "SMART","As CSV",
    "SMART Standardized Final Data_addRouteCols_NO POUND NO SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'LAVTA',
  survey_year     = 2018,
  operator        = 'LAVTA',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "LAVTA","2018","As CSV",
    "OD_20181207_LAVTA_Submittal_FINAL_addCols_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Golden Gate Transit',
  survey_year     = 2018,
  operator        = 'GOLDEN GATE TRANSIT',
  default_tech    = 'express bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Golden Gate Transit","2018","As CSV",
    "20180907_OD_GoldenGate_allDays_addCols_modifyTransfer_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Petaluma Transit',
  survey_year     = 2018,
  operator        = 'PETALUMA TRANSIT',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Petaluma","2018","As CSV",
    "20180530_OD_Petaluma_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Santa Rosa CityBus',
  survey_year     = 2018,
  operator        = 'Santa Rosa CityBus',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Santa Rosa CityBus","2018","As CSV",
    "20180522_OD_SantaRosa_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'ACE',
  survey_year     = 2019,
  operator        = 'ACE',
  default_tech    = 'commuter rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "ACE","2019","As CSV",
    "ACE19_Final Data_AddCols_RecodeRoute_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'WETA',
  survey_year     = 2019,
  operator        = 'SF BAY FERRY',
  default_tech    = 'ferry',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "WETA","WETA 2018","As CSV",
    "WETA-Final Weighted Data-Standardized_addCols_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'TriDelta',
  survey_year     = 2019,
  operator        = 'TRI-DELTA',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Tri Delta","2019","As CSV",
    "TriDelta_ODSurvey_Dataset_Weights_03272019_FinalDeliv_addCols_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'County Connection',
  survey_year     = 2019,
  operator        = 'COUNTY CONNECTION',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "County Connection","2019","As CSV",
    "OD_20191105_CCCTA_Submittal_FINAL Expanded_Revised_05192021_addCols_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Napa Vine',
  survey_year     = 2019,
  operator        = 'NAPA VINE',
  default_tech    = 'local bus',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Napa Vine","2019","As CSV",
    "Napa Vine_FINAL Data_addCols_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Capitol Corridor',
  survey_year     = 2019,
  operator        = 'CAPITOL CORRIDOR',
  default_tech    = 'commuter rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Capitol Corridor","OD Survey 2019","As CSV",
    "CAPCO19 Data-For MTC_NO POUND OR SINGLE QUOTE.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'ACE',
  survey_year     = 2023,
  operator        = 'ACE',
  default_tech    = 'commuter rail',
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "ACE","2023",
    "ACE_Onboard_preprocessed.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Golden Gate Transit',
  survey_year     = 2023,
  operator        = NA,
  default_tech    = NA,
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Golden Gate Transit","2023",
    "GoldenGate_Transit_Ferry_preprocessed.csv"
  )
)
survey_input_df <- survey_input_df %>% add_row(
  survey_name     = 'Regional Snapshot',
  survey_year     = 2023,
  operator        = NA,
  default_tech    = NA,
  raw_data_path   = file.path(
    TPS_SURVEY_PATH,
    "Snapshot Survey",
    "mtc_snapshot_preprocessed.csv"
  )
)

# Inputs - legacy survey data
f_legacy_rdata_path = file.path(TPS_SURVEY_PATH,"_data_Standardized","survey_legacy.RData")

# Outputs
f_output_rds_path <- file.path(TPS_SURVEY_STANDARDIZED_PATH,"survey_standard.RDS")
f_output_csv_path <- str_replace(f_output_rds_path, ".RDS", ".csv")

f_ancillary_output_rdata_path <- file.path(TPS_SURVEY_STANDARDIZED_PATH,"ancillary_variables.RDS")
f_ancillary_output_csv_path   <- str_replace(f_ancillary_output_rdata_path, ".RDS", ".csv")

f_output_decom_rdata_path <- file.path(TPS_SURVEY_STANDARDIZED_PATH,"survey_decomposition.RDS")
f_output_decom_csv_path   <- str_replace(f_output_decom_rdata_path, ".RDS", ".csv")

f_combined_rdata_path <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "survey_combined.Rdata")
f_combined_csv_path   <- str_replace(f_combined_rdata_path,".Rdata", ".csv")

# _User Intervention_
# When adding a new survey, the user must update the dictionary files that translate
# the usually-bespoke survey coding to the standard coding. Edits to the dictionary should be made in
# the file `Dictionary for Standard Database.csv`. The existing entries in the
# dictionary *should* explicate the expected task.

## Prepare dictionaries
# > str(dictionary_all)
# 'data.frame':   6208 obs. of  6 variables:
#  $ survey_name     : chr  "AC Transit" "AC Transit" "ACE" "ACE" ...
#  $ survey_year     : int  2018 2018 2019 2019 2014 2017 2017 2017 2017 2017 ...
#  $ survey_variable : chr  "final_suggested_access_mode" "final_suggested_access_mode" "Access_mode_final" "Access_mode_final" ...
#  $ survey_response : chr  "Personal Bike" "BIKE SHARE" "2" "9" ...
#  $ generic_variable: chr  "access_mode" "access_mode" "access_mode" "access_mode" ...
#  $ generic_response: chr  "bike" "bike" "bike" "bike" ...
dictionary_all <- read.csv(f_dict_standard,
                           header = TRUE) %>%
  rename_all(tolower) %>% 
  mutate(generic_variable=str_trim(generic_variable))        # Remove outside whitespace
print(paste("Read", nrow(dictionary_all),"rows from",f_dict_standard))

# Prepare separate dictionaries for categorical and non-categorical variables
dictionary_non <- dictionary_all %>%
  filter(generic_response == 'NONCATEGORICAL') %>%
  select(survey_name, survey_year, survey_variable, generic_variable)

dictionary_cat <- dictionary_all %>%
  filter(generic_response != 'NONCATEGORICAL') %>%
  mutate(survey_response = as.character(survey_response))


# _User Intervention_
# Each route in the Bay Area has a `canonical` or reference name. This name is stored
# in the database referenced in `f_canonical_routes_path`. The user must match the route
# names from a survey being added to the `canonical` route names. These route names
# are used to assign technologies to each of the routes collected in the survey,
# which allows travel model path labels to be assigned to each trip.
#
# Please note that the `canonical` station names for BART and Caltrain are stored
# in the `f_canonical_station_path` shape file and appended via spatial matching
# to other surveys.


# Add canonical route crosswalk
canonical_routes_crosswalk <- read.csv(f_canonical_routes_path)

## Add surveys
print('Read and combine survey raw data from multiple operators')

survey_combine <- data.frame()
for( i in rownames(survey_input_df) ) {
  # if we are only processing TEST_MODE_OPERATORS then skip others
  if ((length(TEST_MODE_OPERATORS) > 0) & 
        !(survey_input_df[i, "survey_name"] %in% TEST_MODE_OPERATORS))
  {
    next
  }
  print(paste("Processing", survey_input_df[i, "survey_name"], 
              "for", survey_input_df[i, "survey_year"],
              "and", survey_input_df[i, "default_tech"]))

  survey_data_df <- read_survey_data(
     p_survey_name  = survey_input_df[i, "survey_name"],
     p_survey_year  = survey_input_df[i, "survey_year"],
     p_operator     = survey_input_df[i, "operator"],
     p_default_tech = survey_input_df[i, "default_tech"],
     p_file_path    = survey_input_df[i, "raw_data_path"],
     p_variable_dictionary = dictionary_all
  )
  survey_combine <- rbind(survey_combine, survey_data_df)
  print(paste("survey_combine has", nrow(survey_combine),"rows"))
  remove(survey_data_df)
}

## Flatten
print('Join standard_variable and standard_response to raw data')

# Join the dictionary and prepare the categorical variables
print("str(survey_combine)")
str(survey_combine)
print("str(dictionary_cat)")
str(dictionary_cat)

survey_cat <- survey_combine %>%
  left_join(dictionary_cat, by = c("survey_name", "survey_year", "survey_variable", "survey_response")) %>%
  filter(!is.na(generic_variable))

# Join the dictionary and prepare the non-categorical variables

rail_crosswalk_df <- canonical_routes_crosswalk %>%
  filter(survey_name == "GEOCODE") %>%
  select(survey_route_name, canonical_route)

survey_non <- survey_combine %>%
  left_join(dictionary_non, by = c("survey_name", "survey_year", "survey_variable")) %>%
  filter(!is.na(generic_variable)) %>%
  mutate(generic_response = survey_response)

# This transforms generic_response to the canonical_routes_crosswalk's canonical_route if:
# * survey_name/survey_year match and 
# * the generic variable name contains 'route' and
# * the survey_response matches survey_route_name
survey_non <- survey_non %>%
  left_join(canonical_routes_crosswalk %>% select(-technology, -technology_detail, -canonical_operator, -operator_detail),
            by = c("survey_name", "survey_year", "survey_response" = "survey_route_name")) %>%
  mutate(generic_response = ifelse(str_detect(generic_variable, "route") & !is.na(canonical_route), canonical_route, generic_response)) %>%
  select(-canonical_route)

# This transforms generic_response to the canonical_routes_crosswalk's canonical_route if:
# * canonical_routes_crosswalk's survey_name == GEOCODE
# * generic_response == canonical_routes_crosswalk's survey_route_name
survey_non <- survey_non %>%
  left_join(rail_crosswalk_df, by = c("generic_response" = "survey_route_name")) %>%
  mutate(generic_response = ifelse(!is.na(canonical_route), canonical_route, generic_response)) %>%
  select(-canonical_route)

# Combine the categorical and non-categorical survey data and prepare to flatten
survey_flat <- bind_rows(survey_cat, survey_non) %>%
  select(-survey_variable, -survey_response) %>%
  spread(generic_variable, generic_response) %>%
  arrange(survey_name, survey_year, ID) %>%
  mutate(route = ifelse(survey_name == "BART", paste0("BART", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  mutate(route = ifelse(survey_name == "Caltrain", paste0("CALTRAIN", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  mutate(route = ifelse(survey_name == "ACE", paste0("ACE", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  mutate(route = ifelse(survey_name == "Sonoma-Marin Area Rail Transit", paste0("SMART", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  mutate(route = ifelse(survey_name == "Capitol Corridor", paste0("CAPITOL CORRIDOR", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  left_join(rail_crosswalk_df, by = c("route" = "survey_route_name")) %>%
  mutate(route = ifelse(!is.na(canonical_route), canonical_route, route)) %>%
  select(-canonical_route)

# for summarizing
survey_flat <- mutate(survey_flat,
  survey_name_year = paste(survey_name, survey_year))

# Cast certain columns to numeric - hour, lat/lon, weight
# don't warn on NAs introduced by coercion
suppressWarnings(
  survey_flat <- survey_flat %>%
    mutate_at(vars(contains("hour")), as.numeric) %>%
    mutate(across(ends_with('_lat'), as.double)) %>%
    mutate(across(ends_with('_lon'), as.double)) %>%
    mutate(weight = as.numeric(weight))
)

print("str(survey_flat):")
str(survey_flat)

remove(survey_cat,
       survey_non)


## Update survey technology
print('Update technology for multiple-tech surveys')

print('Initial tabulation on technology by survey:')
print(nrow(survey_flat))
table(survey_flat$survey_name_year, 
      survey_flat$survey_tech, useNA = 'ifany')

print('Tabulation of canonical_operator by survey:')
survey_flat %>% count(survey_name_year, canonical_operator)

# _User Intervention_
# As noted above, when the survey data is read in, it assumes every route in the survey uses
# the same technology (e.g., all Muni routes are local bus). In fact, some surveys operate
# multiple technologies. These bespoke technologies are added here. These changes are recorded
# in the `canonical route name database` and must be updated manually.

# columns from canonical_routes_crosswalk:
#  survey_name, survey_year, canonical_route, canonical_operator, operator_detail, technology
#  so this is adding: technology based on the route matching canonical_route
#  But the join is using canonical_route rather than survey_route_name so does it succeed for that many?

survey_flat <- survey_flat %>%
  left_join(canonical_routes_crosswalk %>% 
            select(survey_name, survey_year, canonical_route, technology) %>% 
            unique(),
            by = c("survey_name", "route" = "canonical_route", "survey_year"))

# for multi-tech surveys, survey_tech = technology
survey_flat <- survey_flat %>%
  mutate(survey_tech = ifelse(!is.na(technology),
                              technology,
                              survey_tech))
# we're done with technology so remove it; survey_tech is the one to use
survey_flat <- select(survey_flat, -technology)

print('Final tabulation of survey_tech by survey:')
print(nrow(survey_flat))
table(survey_flat$survey_name_year, 
      survey_flat$survey_tech, useNA = 'ifany')

# _User Intervention_
# User should run each of the `Steps` below individually and make sure the results make sense.
# In addition, the `debug_transfers` dataframe should be empty. If it's not, the code
# has failed to identify an operator or technology for a route that is being
# transferred to or from.

## Build standard variables

# Step 1:  Age-related transformations ----
print('Clean up age-related info')

# Standardize year born
survey_standard <- survey_flat %>%
  mutate(year_born = ifelse(
    str_detect(year_born_four_digit,"Missing") | 
    str_detect(year_born_four_digit,"Not Provided") | 
    str_detect(year_born_four_digit,'REFUSED'),
    NA,
    year_born_four_digit))

# don't warn on NAs introduced by coercion
suppressWarnings(
  survey_standard <- survey_standard %>%
    mutate(year_born = ifelse(is.na(year_born), NA, as.numeric(year_born)))
)
survey_standard <- survey_standard %>%
  select(-year_born_four_digit)

# Manual fixes to year born
survey_standard <- survey_standard %>%
  mutate(survey_year = as.numeric(survey_year)) %>%
  mutate(year_born = ifelse(year_born == 1900, 2000, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1901, 2001, year_born)) %>%
  mutate(year_born = ifelse(year_born == 3884, 1984, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1899, NA, year_born))

print('Tabulation of year_born:')
table(survey_standard$year_born, useNA = 'ifany')

# Compute approximate respondent age
survey_standard <- survey_standard %>%
  mutate(approximate_age = ifelse(!is.na(year_born) & survey_year >= year_born, survey_year - year_born, NA)) %>%
  mutate(approximate_age = ifelse(approximate_age < 0, NA, approximate_age))

print('Tabulation of approximate_age:')
table(survey_standard$approximate_age, useNA = 'ifany')


# Step 2:  Trip- and tour-purpose-related transformations ------------------------------
print('Build tour purpose variable')

# Recode key variables from NA to 'missing'
survey_standard <- survey_standard %>%
  mutate(orig_purp = ifelse(is.na(orig_purp), 'missing', orig_purp)) %>%
  mutate(dest_purp = ifelse(is.na(dest_purp), 'missing', dest_purp)) %>%
  mutate(work_status = ifelse(is.na(work_status), 'missing', work_status)) %>%
  mutate(student_status = ifelse(is.na(student_status), 'missing', student_status)) %>%
  mutate(approximate_age = ifelse(is.na(approximate_age), 'missing', approximate_age)) %>%
  mutate(at_work_prior_to_orig_purp = ifelse(is.na(at_work_prior_to_orig_purp), 'not relevant', at_work_prior_to_orig_purp)) %>%
  mutate(at_work_after_dest_purp = ifelse(is.na(at_work_after_dest_purp), 'not relevant', at_work_after_dest_purp)) %>%
  mutate(at_school_prior_to_orig_purp = ifelse(is.na(at_school_prior_to_orig_purp), 'not relevant', at_school_prior_to_orig_purp)) %>%
  mutate(at_school_after_dest_purp = ifelse(is.na(at_school_after_dest_purp), 'not relevant', at_school_after_dest_purp))


# Refine school purpose
survey_standard <- survey_standard %>%
  mutate(orig_purp = ifelse(orig_purp == "school", "high school", orig_purp)) %>%
  mutate(orig_purp = ifelse(orig_purp == "school" & approximate_age < 14,
                            "grade_school", orig_purp)) %>%
  mutate(dest_purp = ifelse(dest_purp == "school", "high school", dest_purp)) %>%
  mutate(dest_purp = ifelse(dest_purp == "school" & approximate_age < 14,
                            "grade_school", dest_purp))

# for Capitol Corridor 2019 survey, use 'trip_purp'
if ('trip_purp' %in% colnames(survey_standard)) {
  survey_standard <- survey_standard %>%
    mutate(trip_purp = ifelse(trip_purp == "school", "high school", trip_purp)) %>%
    mutate(trip_purp = ifelse(trip_purp == "school" & approximate_age < 14,
                              "grade_school", trip_purp)) %>%
    mutate(trip_purp = ifelse(trip_purp == "school" & approximate_age > 18,
                              "college", trip_purp))
}

# (Approximate) Tour purpose
# Create temporary tour purpose variable that includes both (approximate) tour purpose and imputation name
# Tour purpose and imputation name are separated by "_"
# Then separate into two fields and delete the temporary variable at last step
# 'a'=after, 'b'=before, 'b+a'=before and after, 'o'=origin, 'd'=destination, 'o/d'=origin or destination, 'nw'=non-worker, 'w'=worker or missing

survey_standard <- survey_standard %>% mutate(
  temp_tour=case_when(
    orig_purp == 'home' & dest_purp == 'work'                            ~ 'work_home to work',                            # Work, H to W
    
    orig_purp == 'work' & dest_purp == 'home'                            ~ 'work_work to home',                            # Work, W to H
    
    orig_purp == 'grade school' | dest_purp == 'grade school'            ~ 'grade school_grade school o or d',             # Grade school
    
    orig_purp == 'high school' | dest_purp == 'high school'              ~ 'high school_high school o or d',               # High school
    
    work_status == 'non-worker' &
      (orig_purp == 'college' | dest_purp == 'college')                  ~ 'university_non-worker university o or d',      # Non-worker university origin or destination
    
    work_status == 'non-worker' & 
      student_status == 'non-student' &
      orig_purp == 'home'                                                ~ paste0(dest_purp,'_home to destination nw'),    # Home to destination, non-worker
    
    work_status == 'non-worker' & 
      student_status == 'non-student' & 
      dest_purp == 'home'                                                ~ paste0(orig_purp,'_origin to home nw'),         # Origin to home, non-worker
    
    work_status == 'non-worker' & 
      student_status == 'non-student' & 
      orig_purp == dest_purp                                             ~ paste0(orig_purp,'_orig=destination'),          # Origin=destination
    
    work_status == 'non-worker' & 
      student_status == 'non-student' & 
      (orig_purp == 'escorting' | dest_purp == 'escorting')              ~ 'escorting_non-home escorting o or d',          # Non-home-based escorting 
    
    at_work_prior_to_orig_purp == 'not at work before surveyed trip' & 
      at_work_after_dest_purp == 'not at work after surveyed trip' & 
      (orig_purp == 'college' | dest_purp == 'college')                  ~ 'university_univ present, no work b+a',         # University present, no work
    
    at_work_prior_to_orig_purp == 'at work before surveyed trip' & 
      dest_purp == 'home'                                                ~ 'work_work before, home destination',           # Work before trip, home after
    
    at_work_after_dest_purp == 'at work after surveyed trip' & 
      orig_purp == 'home'                                                ~ 'work_home origin, work after',                 # Home before, work after
    
    work_status == 'non-worker' & 
      at_school_prior_to_orig_purp == 'at school before surveyed trip' & 
      approximate_age > 18 & 
      dest_purp == 'home'                                               ~ 'university_non-wrkr, school b, home d',         # Non-worker, school before trip, home destination, >18
    
    work_status == 'non-worker' & 
      at_school_after_dest_purp == 'at school after surveyed trip' & 
      approximate_age > 18 & 
      orig_purp == 'home'                                               ~ 'university_non-wrkr, school a, home o',         # Non-worker, school after trip, home origin, >18
    
    work_status == 'non-worker' & 
      at_school_prior_to_orig_purp == 'at school before surveyed trip' & 
      approximate_age <= 18 & 
      approximate_age >= 14 & 
      dest_purp == 'home'                                               ~ 'high school_non-wrkr, 14-18, school b, home d', # Non-worker, school before trip, home destination, 14-18
    
    work_status == 'non-worker' & 
      at_school_after_dest_purp == 'at school after surveyed trip' &  
      approximate_age <= 18 & 
      approximate_age >= 14 & 
      orig_purp == 'home'                                               ~ 'high school_non-wrkr, 14-18, school a, home o', # Non-worker, school after trip, home origin, 14-18
    
    at_work_prior_to_orig_purp == 'not at work before surveyed trip' & 
      at_work_after_dest_purp == 'not at work after surveyed trip' & 
      (orig_purp == 'work' | dest_purp == 'work')                       ~ 'work_work o or d',                              # Work origin or destination, not before or after
    
    at_work_prior_to_orig_purp == 'at work before surveyed trip' & 
      dest_purp == 'work'                                               ~ 'at work_at work subtour work b, work d',  # Work before origin and work destination
    
    at_work_after_dest_purp == 'at work after surveyed trip' & 
      orig_purp == 'work'                                               ~ 'at work_at work subtour work a, work o',  # Work after destination and work origin
    
    at_work_after_dest_purp == 'at work after surveyed trip' & 
      at_work_prior_to_orig_purp == 'at work before surveyed trip'      ~ 'work_at work subtour work a, work b',     # Work before origin and work after destination
    
    orig_purp == 'home'                                                 ~ paste0(dest_purp,'_home to destination w'),# Home to destination, worker or missing information
    
    dest_purp == 'home'                                                 ~ paste0(orig_purp,'_origin to home w'),     # Origin to home, worker or missing information                                                                            
    
    TRUE                                                                ~ paste0(orig_purp,'_default origin'))) %>%  # Remaining cases default to the origin purpose
  
# Now separate tour purpose and tour purpose case designation into two columns

  separate(temp_tour,c("tour_purp", "tour_purp_case"), sep="_") %>%

# for surveys that include 'trip_purp' instead of 'orig/dest_purp', assume that's the tour purpose
  mutate(tour_purp = ifelse(
    (tour_purp == 'missing') & (!is.na(trip_purp)), trip_purp, tour_purp)) %>%
  mutate(tour_purp_case = ifelse(
    (tour_purp == 'missing') & (!is.na(trip_purp)), 'trip_purp', tour_purp_case)) %>%
  
# finally, if work-related or business apt, categorize as 'other maintenance'

  mutate(tour_purp = ifelse(tour_purp %in% c('work-related','business apt'), 'other maintenance', tour_purp))
      
# Output frequency file, test file to review missing cases, and test of duplicates

print('Tabulation of tour_purp by survey:')
table(survey_standard$survey_name_year, survey_standard$tour_purp, useNA = 'ifany')

print('Examine interim output check_missing_tour.csv for records with missing tour_purp')
missing_tour_df <- survey_standard %>% 
  filter(tour_purp=='missing') %>% 
  select(survey_name, survey_year, ID, orig_purp,dest_purp,tour_purp,at_school_after_dest_purp,at_school_prior_to_orig_purp,at_work_after_dest_purp,at_work_prior_to_orig_purp,approximate_age)
f_check_missing_tour_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "check_missing_tour.csv")
write.csv(missing_tour_df, f_check_missing_tour_file, row.names = FALSE)


# Step 3:  Update Key locations and Status Flags --------------------------------------
print('Clean up work/student status info')

# Home
survey_standard <- survey_standard %>%
  mutate(home_lat = ifelse(orig_purp == 'home' & is.na(home_lat),
                           orig_lat,
                           home_lat)) %>%
  mutate(home_lon = ifelse(orig_purp == 'home' & is.na(home_lon),
                           orig_lon,
                           home_lon)) %>%
  mutate(home_lat = ifelse(dest_purp == 'home' & is.na(home_lat),
                           dest_lat,
                           home_lat)) %>%
  mutate(home_lon = ifelse(dest_purp == 'home' & is.na(home_lon),
                           dest_lon,
                           home_lon) ) %>%

  # Work
  mutate(workplace_lat = ifelse(orig_purp == 'work' & is.na(workplace_lat),
                                orig_lat,
                                workplace_lat)) %>%
  mutate(workplace_lon = ifelse(orig_purp == 'work' & is.na(workplace_lon),
                                orig_lon,
                                workplace_lon)) %>%
  mutate(workplace_lat = ifelse(dest_purp == 'work' & is.na(workplace_lat),
                                dest_lat,
                                workplace_lat)) %>%
  mutate(workplace_lon = ifelse(dest_purp == 'work' & is.na(workplace_lon),
                                dest_lon,
                                workplace_lon)) %>%

  # School
  mutate(school_lat = ifelse(orig_purp %in% c('grade school', 'high school', 'college') &
                               is.na(school_lat),
                             orig_lat,
                             school_lat)) %>%
  mutate(school_lon = ifelse(orig_purp %in% c('grade school', 'high school', 'college') &
                               is.na(school_lon),
                             orig_lon,
                             school_lon)) %>%
  mutate(school_lat = ifelse(dest_purp %in% c('grade school', 'high school', 'college') &
                               is.na(school_lat),
                             dest_lat,
                             school_lat)) %>%
  mutate(school_lon = ifelse(orig_purp %in% c('grade school', 'high school', 'college') &
                               is.na(school_lon),
                             dest_lon,
                             school_lon))

# Work and Student status
survey_standard <- survey_standard %>%
  mutate(work_status = ifelse(orig_purp == 'work' | dest_purp == 'work',
                              'full- or part-time',
                              work_status)) %>%
  mutate(student_status = ifelse(orig_purp == 'grade school' |
                                   dest_purp == 'grade school',
                                 'full- or part-time',
                                 student_status)) %>%
  mutate(student_status = ifelse(orig_purp == 'high school' |
                                   dest_purp == 'high school',
                                 'full- or part-time',
                                 student_status)) %>%
  mutate(student_status = ifelse(orig_purp == 'college' |
                                   dest_purp == 'college',
                                 'full- or part-time',
                                 student_status))

print('Stats on work_status')
table(survey_standard$work_status, useNA = 'ifany')
print('Stats on student_status')
table(survey_standard$student_status, useNA = 'ifany')


# Step 4:  Automobile vs Workers ------------------------------------------------------
print('Calculate automobiles vs workers')

# Transform vehicles and workers to standard scale
survey_standard <- survey_standard %>%
  mutate(vehicles = ifelse((vehicles == 'other' & 'vehicles_other' %in% colnames(survey_standard)),
                            vehicles_other, vehicles)) %>%
  mutate(workers = ifelse((workers == 'other' & 'workers_other' %in% colnames(survey_standard)),
                           workers_other,  workers)) %>%
  mutate(persons = ifelse((persons == 'other' & 'persons_other' %in% colnames(survey_standard)),
                           persons_other,  persons)) %>%
  select(-persons_other,
         -vehicles_other,
         -workers_other)

print('Stats on vehicles/workers/persons for debug:')
table(survey_standard$vehicles, useNA = 'ifany')
table(survey_standard$workers, useNA = 'ifany')
table(survey_standard$persons, useNA = 'ifany')

# consolidate categorical and noncategorical values for 'persons', 'vehicles' and 'workers' - the values originally from
# the 'persons'/'vehicles'/'workers' fields are categorical (one, two, etc.) whereas the values originally from 'persons_other'/
# 'vehicles_other'/'workers_other' are numeric (5, 6, etc.). Convert the latter to the former's format
survey_standard <- survey_standard %>%
  mutate(vehicles = recode(vehicles,
                           '5' = "five", '6' = "six", '7' = "seven",
                           '8' = "eight", '9' = "nine", '10' = "ten")) %>%
  mutate(workers = recode(workers,
                          '7' = "seven", '11' = "eleven")) %>%
  mutate(persons = recode(persons,
                          '7' = "seven", '8' = "eight", '9' = "nine",
                          '10' = "ten", '11' = 'eleven', '27' = 'twenty-seven'))

# map vehicles and workers counts to numeric values in order to calculate autos vs workers
vehicles_dictionary <- data.frame(
  vehicles = c('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
               'eight', 'nine', 'ten', 'eleven', 'twelve', 'four or more', 'six or more'),
  vehicle_numeric_cat = c(0, 1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4))

workers_dictionary <- data.frame(
  workers = c('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
              'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen',
              'fifteen', 'six or more'),
  worker_numeric_cat = c(0, 1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4))

survey_standard <- left_join(survey_standard, vehicles_dictionary, by = c("vehicles"))
survey_standard <- left_join(survey_standard, workers_dictionary, by = c("workers"))
# some surveys have numeric values in 'vehicles_other' and 'workers_other' fields that cannot join with the
# count dictionary, therefore fill in the na using the raw numeric value
survey_standard <- survey_standard %>%
  mutate(vehicle_numeric_cat = ifelse(is.na(vehicle_numeric_cat), vehicles, vehicle_numeric_cat)) %>%
  mutate(worker_numeric_cat = ifelse(is.na(worker_numeric_cat), workers, worker_numeric_cat))

print('Stats on vehicle_numeric_cat/worker_numeric_cat for debug:s')
table(survey_standard$vehicle_numeric_cat, useNA = 'ifany')
table(survey_standard$worker_numeric_cat, useNA = 'ifany')

survey_standard <- survey_standard %>%
  mutate(autos_vs_workers = ifelse(vehicle_numeric_cat == 0, 'zero autos', 'missing')) %>%
  mutate(autos_vs_workers = ifelse(vehicle_numeric_cat > 0 &
                              worker_numeric_cat > 0  &
                              worker_numeric_cat >  vehicle_numeric_cat,
                            'workers > autos',
                            autos_vs_workers)) %>%
  mutate(autos_vs_workers = ifelse(vehicle_numeric_cat > 0 &
                              worker_numeric_cat >= 0 &
                              worker_numeric_cat <= vehicle_numeric_cat,
                            'workers <= autos',
                            autos_vs_workers)) %>%
  mutate(autos_vs_workers = ifelse((vehicle_numeric_cat == 'missing') | (
                             worker_numeric_cat == 'missing') | (
                             vehicle_numeric_cat == 'Missing') | (
                             worker_numeric_cat == 'Missing') | (
                             vehicle_numeric_cat == 'Ref') | (
                             worker_numeric_cat == 'Ref') | (
                             vehicle_numeric_cat == "DON'T KNOW") | (
                             worker_numeric_cat == "DON'T KNOW") | (
                             is.na(vehicle_numeric_cat)) | (
                             is.na(worker_numeric_cat)),
                            'missing',
                            autos_vs_workers))

print('Stats on autos_vs_workers:')
table(survey_standard$autos_vs_workers, useNA = 'ifany')

remove(vehicles_dictionary,
       workers_dictionary)


# Step 5:  Operator and Technology sequence --------------------------------------------
print('Configure operator and technology for transfer routes')

# Set operator for each of six legs (three before, three after)
# - remove Dummy Records
survey_standard <- survey_standard %>%
  mutate(first_route_before_survey_board = ifelse(first_route_before_survey_board == "Missing___Missing Dummy Record",
                                                  "",
                                                  first_route_before_survey_board)) %>%
  mutate(second_route_before_survey_board = ifelse(second_route_before_survey_board == "Missing___Missing Dummy Record",
                                                   "",
                                                   second_route_before_survey_board)) %>%
  mutate(third_route_before_survey_board = ifelse(third_route_before_survey_board == "Missing___Missing Dummy Record",
                                                  "",
                                                  third_route_before_survey_board))

survey_standard <- survey_standard %>%
  mutate(first_route_after_survey_alight = ifelse(first_route_after_survey_alight == "Missing___Missing Dummy Record",
                                                  "",
                                                  first_route_after_survey_alight)) %>%
  mutate(second_route_after_survey_alight = ifelse(second_route_after_survey_alight == "Missing___Missing Dummy Record",
                                                   "",
                                                   second_route_after_survey_alight)) %>%
  mutate(third_route_after_survey_alight = ifelse(third_route_after_survey_alight == "Missing___Missing Dummy Record",
                                                  "",
                                                  third_route_after_survey_alight))

survey_standard <- survey_standard %>%
  mutate(first_before_operator_detail  = str_extract(first_route_before_survey_board,  "^[A-z -]+?(?=_)"),
         second_before_operator_detail = str_extract(second_route_before_survey_board, "^[A-z -]+?(?=_)"),
         third_before_operator_detail  = str_extract(third_route_before_survey_board,  "^[A-z -]+?(?=_)"),
         first_after_operator_detail   = str_extract(first_route_after_survey_alight,  "^[A-z -]+?(?=_)"),
         second_after_operator_detail  = str_extract(second_route_after_survey_alight, "^[A-z -]+?(?=_)"),
         third_after_operator_detail   = str_extract(third_route_after_survey_alight,  "^[A-z -]+?(?=_)"))

# convert detailed transfer operator to less granular categories
operator_canonical_detail_crosswalk <- canonical_routes_crosswalk %>%
  select(canonical_route, canonical_operator, operator_detail) %>%
  unique() %>%
  rename(transfer_route_name = canonical_route,
         transfer_operator_canonical = canonical_operator,
         transfer_operator_detail = operator_detail)
str(operator_canonical_detail_crosswalk)

# > str(operator_canonical_detail_crosswalk)
# 'data.frame':	4792 obs. of  3 variables:
#  $ transfer_route_name        : chr  "AC TRANSIT___1 Berkeley BART to Bay Fair BART" "AC TRANSIT___10 San Leandro BART Hayward BART" "AC TRANSIT___12 Berkeley BART to Downtown Oakland" "AC TRANSIT___14 Downtown Oakland to Fruitvale BART" ...
#  $ transfer_operator_canonical: chr  "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" ...
#  $ transfer_operator_detail   : chr  "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" ...

survey_standard <- survey_standard %>%
  left_join(operator_canonical_detail_crosswalk,
            by = c('first_route_before_survey_board' = 'transfer_route_name',
                   'first_before_operator_detail' = 'transfer_operator_detail')) %>%
  rename(first_before_operator = transfer_operator_canonical) %>%
  
  left_join(operator_canonical_detail_crosswalk,
            by = c('second_route_before_survey_board' = 'transfer_route_name',
                   'second_before_operator_detail' = 'transfer_operator_detail')) %>%
  rename(second_before_operator = transfer_operator_canonical) %>%
  
  left_join(operator_canonical_detail_crosswalk,
            by = c('third_route_before_survey_board' = 'transfer_route_name',
                   'third_before_operator_detail' = 'transfer_operator_detail')) %>%
  rename(third_before_operator = transfer_operator_canonical) %>%
  
  left_join(operator_canonical_detail_crosswalk,
            by = c('first_route_after_survey_alight' = 'transfer_route_name',
                   'first_after_operator_detail' = 'transfer_operator_detail')) %>%
  rename(first_after_operator = transfer_operator_canonical) %>%
  
  left_join(operator_canonical_detail_crosswalk,
            by = c('second_route_after_survey_alight' = 'transfer_route_name',
                   'second_after_operator_detail' = 'transfer_operator_detail')) %>%
  rename(second_after_operator = transfer_operator_canonical) %>%
  
  left_join(operator_canonical_detail_crosswalk,
            by = c('third_route_after_survey_alight' = 'transfer_route_name',
                   'third_after_operator_detail' = 'transfer_operator_detail')) %>%
  rename(third_after_operator = transfer_operator_canonical)

# %>%
#   select(-first_before_operator_detail, -second_before_operator_detail, -third_before_operator_detail,
#          -first_after_operator_detail,  -second_after_operator_detail,  -third_after_operator_detail)


# Set the technology for each of the six legs
tech_crosswalk_df <- canonical_routes_crosswalk %>%
  select(-survey_route_name) %>%
  rename(temp_tech = technology) %>%
  unique()
print("str(tech_crosswalk_df)")
str(tech_crosswalk_df)

tech_crosswalk_expansion_list <- tech_crosswalk_df %>%
  select(survey_name, survey_year) %>%
  unique() %>%
  filter(!survey_name %in% c("BART", "Caltrain", "ACE", "Sonoma-Marin Area Rail Transit", "Capitol Corridor", "GEOCODE"))
print("str(tech_crosswalk_expansion_list)")
str(tech_crosswalk_expansion_list)

tech_crosswalk_expansion_df <- tech_crosswalk_df %>%
  filter(survey_name == "GEOCODE") %>%
  select(-survey_name, -survey_year)
print("str(tech_crosswalk_expansion_df)")
str(tech_crosswalk_expansion_df)

tech_crosswalk_expansion_df <- tech_crosswalk_expansion_list %>%
  merge(tech_crosswalk_expansion_df, by = NULL)
print("str(tech_crosswalk_expansion_df)")
str(tech_crosswalk_expansion_df)

tech_crosswalk_df <- tech_crosswalk_df %>%
  bind_rows(tech_crosswalk_expansion_df) %>%
  filter(survey_name != "GEOCODE") %>%
  select(-operator_detail, -technology_detail)
print("str(tech_crosswalk_df)")
str(tech_crosswalk_df)

remove(tech_crosswalk_expansion_df, tech_crosswalk_expansion_list)

#> str(tech_crosswalk_df)
#'data.frame':	82777 obs. of  5 variables:
# $ survey            : chr  "AC Transit" "AC Transit" "AC Transit" "FAST" ...
# $ survey_year       : int  2018 2018 2018 2017 2018 2018 2018 2018 2018 2018 ...
# $ canonical_route    : chr  "AC TRANSIT___1 Berkeley BART to Bay Fair BART" "AC TRANSIT___10 San Leandro BART Hayward BART" "AC TRANSIT___12 Berkeley BART to Downtown Oakland" "AC TRANSIT___12 Berkeley BART to Downtown Oakland" ...
# $ canonical_operator: chr  "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" "AC TRANSIT" ...
# $ temp_tech         : chr  "local bus" "local bus" "local bus" "local bus" ...


survey_standard <- survey_standard %>%
  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "first_before_operator" = "canonical_operator",
                                     "first_route_before_survey_board" = "canonical_route")) %>%
  rename(first_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "second_before_operator" = "canonical_operator",
                                     "second_route_before_survey_board" = "canonical_route")) %>%
  rename(second_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "third_before_operator" = "canonical_operator",
                                     "third_route_before_survey_board" = "canonical_route")) %>%
  rename(third_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "first_after_operator" = "canonical_operator",
                                     "first_route_after_survey_alight" = "canonical_route")) %>%
  rename(first_after_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "second_after_operator" = "canonical_operator",
                                     "second_route_after_survey_alight" = "canonical_route")) %>%
  rename(second_after_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("survey_name", "survey_year",
                                     "third_after_operator" = "canonical_operator",
                                     "third_route_after_survey_alight" = "canonical_route")) %>%
  rename(third_after_technology = temp_tech)

print('Stats on first_before_technology/first_after_technology for debug:')
table(survey_standard$first_before_technology, useNA = 'ifany')  #### check if there is "Missing"
table(survey_standard$first_after_technology, useNA = 'ifany')


# Step 6:  Transfer details -----------------------------------------------

# Transfer to and from
survey_standard <- survey_standard %>%
  mutate(transfer_from = as.character(first_before_operator)) %>%
  mutate(transfer_from = ifelse(!is.na(second_before_operator),
                                as.character(second_before_operator),
                                transfer_from)) %>%

  mutate(transfer_from = ifelse(!is.na(third_before_operator),
                                as.character(third_before_operator),
                                transfer_from)) %>%

  mutate(transfer_to = as.character(first_after_operator))

# First boarding and last alighting technology
survey_standard <- survey_standard %>%
  mutate(first_board_tech = as.character(survey_tech)) %>%
  mutate(first_board_tech = ifelse(!is.na(first_before_technology),
                                   first_before_technology,
                                   first_board_tech)) %>%

  mutate(last_alight_tech = as.character(survey_tech)) %>%
  mutate(last_alight_tech = ifelse(!is.na(first_after_technology),
                                   first_after_technology,
                                   last_alight_tech)) %>%

  mutate(last_alight_tech = ifelse(!is.na(second_after_technology),
                                   second_after_technology,
                                   last_alight_tech)) %>%

  mutate(last_alight_tech = ifelse(!is.na(third_after_technology),
                                   third_after_technology,
                                   last_alight_tech))

print('Tabulation of transfer_from by survey:')
table(survey_standard$survey_name_year, survey_standard$transfer_from, useNA = 'ifany')
print('Tabulation of transfer_to by survey:')
table(survey_standard$survey_name_year, survey_standard$transfer_to, useNA = 'ifany')
print('Tabulation of survey_tech by survey:')
table(survey_standard$survey_name_year, survey_standard$survey_tech, useNA = 'ifany')
print('Tabulation of first_board_tech by survey:')
table(survey_standard$survey_name_year, survey_standard$first_board_tech, useNA = 'ifany')
print('Tabulation of last_alight_tech by survey:')
table(survey_standard$survey_name_year, survey_standard$last_alight_tech, useNA = 'ifany')


# Technology present calculations
survey_standard <- survey_standard %>%
  mutate(first_before_technology = ifelse(is.na(first_before_technology),
                                          "Missing",
                                          first_before_technology)) %>%
  mutate(second_before_technology = ifelse(is.na(second_before_technology),
                                           "Missing",
                                           second_before_technology)) %>%
  mutate(third_before_technology = ifelse(is.na(third_before_technology),
                                          "Missing",
                                          third_before_technology)) %>%
  mutate(first_after_technology = ifelse(is.na(first_after_technology),
                                         "Missing",
                                         first_after_technology)) %>%
  mutate(second_after_technology = ifelse(is.na(second_after_technology),
                                          "Missing",
                                          second_after_technology)) %>%
  mutate(third_after_technology = ifelse(is.na(third_after_technology),
                                         "Missing",
                                         third_after_technology))

technology_exist_labels = list('commuter rail' = 'commuter_rail_present', 
                               'heavy rail' = 'heavy_rail_present',
                               'express bus' = 'express_bus_present',
                               'ferry' = 'ferry_present',
                               'light rail' = 'light_rail_present')

for (technology_type in names(technology_exist_labels)) {
  print(paste0('technology present for: ', technology_type))
  new_col_name <- technology_exist_labels[[technology_type]]
  survey_standard <- technology_present(survey_standard,
                                        technology_type,
                                        new_col_name)
}


# Boardings 

# Figure out for which surveys we have no transfer information at all
transfer_from_df <- as.data.frame.matrix(table(survey_standard$survey_name_year, survey_standard$transfer_from, useNA = 'ifany'))
transfer_to_df   <- as.data.frame.matrix(table(survey_standard$survey_name_year, survey_standard$transfer_to,   useNA = 'ifany'))
# the last column is NA -- rename it
names(transfer_from_df)[length(names(transfer_from_df))] <-"no_xfer_from"
names(transfer_to_df  )[length(names(transfer_to_df  ))] <-"no_xfer_to"

# summarize to two columns: has transfer operator from or not, and has transfer operator to or not
transfer_from_df <- transfer_from_df %>%
  mutate(has_xfer_from = select(., -no_xfer_from) %>% apply(1, sum)) %>%
  select(has_xfer_from, no_xfer_from)
transfer_to_df <- transfer_to_df %>%
  mutate(has_xfer_to = (select(., -no_xfer_to) %>% apply(1, sum))) %>%
  select(has_xfer_to, no_xfer_to)

# make survey_name_year a column instead of the names
transfer_from_df <- cbind(survey_name_year = rownames(transfer_from_df), transfer_from_df)
transfer_to_df   <- cbind(survey_name_year = rownames(transfer_to_df  ), transfer_to_df  )
rownames(transfer_from_df) <- 1:nrow(transfer_from_df)
rownames(transfer_to_df  ) <- 1:nrow(transfer_to_df  )

# put them together and determine if transfers were surveyed
transfer_operator_df <- full_join(transfer_from_df, transfer_to_df, by = join_by(survey_name_year))
transfer_operator_df <- transfer_operator_df %>%
  mutate(transfers_surveyed = (has_xfer_from + has_xfer_to) > 0)

print("transfer_operator_df:")
print(transfer_operator_df)

# set boardings = 1 IFF transfers were surveyed; otherwise leave as NA
survey_standard <- survey_standard %>%
  left_join(select(transfer_operator_df, survey_name_year, transfers_surveyed),
            by = join_by(survey_name_year)) %>%
  mutate(boardings = ifelse(transfers_surveyed, 1L, NA))

survey_standard <- survey_standard %>%
  mutate(boardings = ifelse(!(first_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_before_technology == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(first_after_technology   == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_after_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_after_technology   == "Missing"), boardings + 1, boardings))

print('Tabulation of boardings by survey:')
table(survey_standard$survey_name_year, survey_standard$boardings, useNA = 'ifany')

# If missing, compute number_transfers_orig_board and number_transfers_alight_dest
survey_standard <- survey_standard %>%
  mutate(first  = ifelse(str_length(first_route_before_survey_board)  == 0L, 0, 1)) %>%
  mutate(second = ifelse(str_length(second_route_before_survey_board) == 0L, 0, 1)) %>%
  mutate(third  = ifelse(str_length(third_route_before_survey_board)  == 0L, 0, 1)) %>%
  mutate(number_transfers_orig_board = ifelse(is.na(number_transfers_orig_board),
                                              first + second + third, number_transfers_orig_board)) %>%
  mutate(first  = ifelse(str_length(first_route_after_survey_alight)  == 0L, 0, 1)) %>%
  mutate(second = ifelse(str_length(second_route_after_survey_alight) == 0L, 0, 1)) %>%
  mutate(third  = ifelse(str_length(third_route_after_survey_alight)  == 0L, 0, 1)) %>%
  mutate(number_transfers_alight_dest = ifelse(is.na(number_transfers_alight_dest),
                                               first + second + third, number_transfers_alight_dest)) %>%
  select(-first, -second, -third)

print('Tabulation of number_transfers_orig_board by survey:')
table(survey_standard$survey_name_year, survey_standard$number_transfers_orig_board, useNA = 'ifany')
print('Tabulation of number_transfers_alight_dest by survey:')
table(survey_standard$survey_name_year, survey_standard$number_transfers_alight_dest, useNA = 'ifany')

survey_standard <- survey_standard %>%
  mutate(survey_boardings = 1L +
           as.numeric(number_transfers_orig_board) +
           as.numeric(number_transfers_alight_dest) )

print('Tabulation of survey_boardings by survey:')
table(survey_standard$survey_name_year, survey_standard$survey_boardings, useNA = 'ifany')

print('Stats on boardings/survey_boardings for debug:')
table(survey_standard$boardings, survey_standard$survey_boardings, useNA = 'ifany')

# Build debug data frame to find odds and ends
# "boardings" is calculated from transfer technologies. "survey_boarding" is calculated from
# "number_transfers_orig_board" and "number_transfers_alight_dest"; some surveys contain these two variables
# in the raw data, others have them calculated based on detailed transfer routes.
# Ideally, debug_transfers has 0 record. However, there are outliers, where 'boarding' and 'survey_boardings' are different.

#   CASE 1: (Nov 10, 2020): SF Muni 2017 survey (ID 25955, 31474) and AC Transit 2018 survey (ID 959) track 4 transfers
#   before and after the surveyed route, therefore "number_transfers_orig_board"/"number_transfers_alight_dest"
#   maxes at 4, and but the standard database only tracks 3 before/after transfers, so transfer technology counts
#   maxes at 3 on each end, causing inconsistency between boardings and survey_boardings.
#   (Feb 22, 2021): the .csv survey data for Muni and AC Transit was modified to change "number_transfers_alight_dest == 4" to 3

#   CASE 2: one or more of the transfers are routes with "Missing" canonical operator, e.g. "Missing___missing",
#   then "survey_boarding" is larger than "boardings". This occurs in WestCAT 2017 survey (ID 391).

#   CASE 3: The "number_transfers_orig_board" or "number_transfers_alight_dest" values in the raw data
#   are inconsistent with the detailed transfer routes. Napa Vine 2019 (ID 14, 77, 306, 9142, 9216), VTA 2017 (ID 44469).

#   CASE 4: Capitol Corridor (2019) survey doesn't have detailed transfer operator/route information except for BART,
#   Caltrain, Amtrak.

debug_transfers <- survey_standard %>%
  filter(!(boardings == survey_boardings)) %>%
  select(ID, survey_name, route, direction,
         boardings, survey_boardings,
         first_route_before_survey_board,  first_before_operator,  first_before_technology,
         second_route_before_survey_board, second_before_operator, second_before_technology,
         third_route_before_survey_board,  third_before_operator,  third_before_technology,
         first_route_after_survey_alight,   first_after_operator,  first_after_technology,
         second_route_after_survey_alight, second_after_operator, second_after_technology,
         third_route_after_survey_alight,  third_after_operator,  third_after_technology)
f_check_transfers_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "check_transfers.csv")
write.csv(debug_transfers, f_check_transfers_file, row.names = FALSE)

print('Examine interim output check_transfers.csv for records with boardings/survey_boardings mismatch')

survey_standard <- survey_standard %>%
  select(-survey_boardings)



# Step 7:  Standardize Demographics ----------------------------------------------------
print('Configure demographic variables')

# fill n/a in race columns with 0 so that they won't affect race_dmy_sum calculation
suppressWarnings(
  survey_standard <- survey_standard %>%
    mutate(race_dmy_ind = as.numeric(race_dmy_ind),
           race_dmy_asn = as.numeric(race_dmy_asn),
           race_dmy_blk = as.numeric(race_dmy_blk),
           race_dmy_hwi = as.numeric(race_dmy_hwi),
           race_dmy_wht = as.numeric(race_dmy_wht),
           race_dmy_mdl_estn = as.numeric(race_dmy_mdl_estn))
)
survey_standard <- survey_standard %>%
  replace_na(list(
    race_dmy_ind = 0,
    race_dmy_asn = 0,
    race_dmy_blk = 0,
    race_dmy_hwi = 0,
    race_dmy_wht = 0,
    race_dmy_mdl_estn = 0,
    # str_length checks > 2
    race_other_string = 'NA',
    race_cat = 'NA'
  ))

print('Tabulation of race_cat by survey')
table(survey_standard$survey_name_year, survey_standard$race_cat, useNA = 'ifany')

# Note that the "race_categories" variable codes race_cat strings (0=na, 1=specified)
#      And then that tally is added to race_dmy_sum
# Therefore race_cat OR race_dmy_[asn,blk,ind,hwi,wht,mdl_estn,oth] should be used but NOT BOTH
# Or respondents will be counted as multi-racial and tabulated as OTHER just from the same race twice
survey_standard <- survey_standard %>%
  # Race
  mutate(race_dmy_oth = ifelse(str_length(as.character(race_other_string)) > 2, 1L, 0L)) %>%
  mutate(race_categories = ifelse(str_length(as.character(race_cat)) > 2, 1L, 0L)) %>%
  mutate(race_dmy_sum = race_dmy_ind + race_dmy_asn + race_dmy_blk + race_dmy_hwi + race_dmy_wht + race_dmy_mdl_estn + race_dmy_oth + race_categories)

print('Tabulation of race_categories by survey')
table(survey_standard$survey_name_year, survey_standard$race_categories, useNA = 'ifany')

print(head(filter(survey_standard, survey_name=="Regional Snapshot") %>% 
  select(race_dmy_ind, race_dmy_asn, race_dmy_blk, race_dmy_hwi, race_dmy_wht, race_dmy_mdl_estn,
         race_other_string, race_dmy_oth, 
         race_categories, race_cat, 
         race_dmy_sum), n=30))

survey_standard <- survey_standard %>%
  mutate(race = 'OTHER') %>%
  mutate(race = ifelse((race_dmy_sum == 1) & ((race_dmy_asn == 1) | (race_cat == 'ASIAN')), 'ASIAN', race)) %>%
  mutate(race = ifelse((race_dmy_sum == 1) & ((race_dmy_blk == 1) | (race_cat == 'BLACK')), 'BLACK', race)) %>%
  mutate(race = ifelse((race_dmy_sum == 1) & ((race_dmy_wht == 1) | (race_cat == 'WHITE')), 'WHITE', race)) %>%
  mutate(race = ifelse((race_dmy_sum == 1) & (race_dmy_mdl_estn == 1), 'WHITE', race)) %>%
  mutate(race = ifelse((race_dmy_sum == 2) & ((race_dmy_wht == 1) | (race_cat == 'WHITE')) & (race_dmy_mdl_estn == 1), 'WHITE', race))

print('Tabulation of race_cat by race')
table(survey_standard$race_cat, survey_standard$race, useNA = 'ifany')


print('Tabulation of race by survey')
table(survey_standard$survey_name_year, survey_standard$race, useNA = 'ifany')

survey_standard <- survey_standard %>% 
select(-race_dmy_ind,
       -race_dmy_asn,
       -race_dmy_blk,
       -race_dmy_hwi,
       -race_dmy_wht,
       -race_dmy_mdl_estn,
       -race_dmy_oth,
       -race_dmy_sum,
       -race_cat,
       -race_categories,
       -race_other_string)

  # Language at home
survey_standard <- survey_standard %>%
  mutate(language_at_home = ifelse(as.character(language_at_home_binary) == 'OTHER',
                                   as.character(language_at_home_detail),
                                   as.character(language_at_home_binary))) %>%
  mutate(language_at_home = ifelse(as.character(language_at_home) == 'other',
                                   as.character(language_at_home_detail_other),
                                   as.character(language_at_home))) %>%
  mutate(language_at_home = toupper(language_at_home)) %>%
  select(-language_at_home_binary,
         -language_at_home_detail,
         -language_at_home_detail_other)
         

# check if all records have the "race" variable filled, race_chk should be empty
race_chk <- survey_standard[which(is.na(survey_standard$race)),]

# Update fare medium for surveys with clipper detail
survey_standard <- survey_standard %>%
  mutate(fare_medium = ifelse(is.na(clipper_detail), fare_medium, clipper_detail)) %>%
  # conver all to lower case
  mutate(fare_medium = tolower(fare_medium)) %>%
  select(-clipper_detail)

# consolidate 'fare_medium' with 'fare_medium_other', and 'fare_category' with 'fare_category_other'
survey_standard <- survey_standard %>%
  mutate(fare_medium = ifelse(fare_medium == 'other', fare_medium_other, fare_medium)) %>%
  mutate(fare_category = ifelse(fare_category == 'other', fare_category_other, fare_category)) %>%
  # conver all to lower case
  mutate(fare_category = tolower(fare_category)) %>%
  select(-fare_medium_other,
         -fare_category_other)

# consolidate missing household income into 'missing' 
survey_standard <- survey_standard %>%
  mutate(household_income = ifelse(household_income == "DON'T KNOW", "Missing", household_income))

print('Stats on work_status:')
table(survey_standard$work_status, useNA = 'ifany')
print('Stats on student_status:')
table(survey_standard$student_status, useNA = 'ifany')
print('Stats on fare_medium:')
table(survey_standard$fare_medium, useNA = 'ifany')
print('Stats on fare_category:')
table(survey_standard$fare_category, useNA = 'ifany')
print('Stats on hispanic:')
table(survey_standard$hispanic, useNA = 'ifany')
print('Stats on race:')
table(survey_standard$race, useNA = 'ifany')
print('Stats on language_at_home:')
table(survey_standard$language_at_home, useNA = 'ifany')
print('Stats on household_income:')
table(survey_standard$household_income, useNA = 'ifany')
print('Stats on eng_proficient:')
table(survey_standard$eng_proficient, useNA = 'ifany')


# Step 8:  Set dates and times ---------------------------------------------------------
print('Configure date- and time-related variables')

# Deal with date and time
survey_standard <- survey_standard %>%
  mutate(date_string = ifelse(str_length(date_string) == 0, NA, date_string)) %>%
  mutate(time_string = ifelse(str_length(time_string) == 0, NA, time_string))

# Deal with BART's missing (currently only have weekday data, update when we add in weekend)
survey_standard <- survey_standard %>%
  mutate(date_string = ifelse(date_string == "Missing - Dummy Record", NA, date_string)) %>%
  mutate(date_string = ifelse(date_string == "Missing - Question Not Asked", NA, date_string)) %>%
  mutate(date_string = ifelse(date_string == "Paper Survey", NA, date_string)) %>%
  mutate(date_string = ifelse(date_string == "Unknown", NA, date_string)) %>%
  mutate(date_string = ifelse(date_string == "UNKNOWN", NA, date_string)) %>%
  # first, label survey responses that are missing both 'date_string' and 'weekpart' values
  mutate(weekpart = ifelse((is.na(date_string)) & (is.na(weekpart)), "Missing", weekpart)) %>%
  # second, add fixing for BART
  mutate(weekpart = ifelse((is.na(date_string) & survey_name == "BART"), "WEEKDAY", weekpart))

# print('Count of Regional Snapshot 2023 date_string for debug:')
# print(survey_standard %>% filter(survey_name_year == "Regional Snapshot 2023") %>% count(date_string))

# Get day of the week from date
survey_standard <- survey_standard %>%
  mutate(date1 = as.Date(date_string, format = "%m/%d/%Y")) %>%
  mutate(date2 = as.Date(date_string, format = "%Y-%m-%d")) %>%
  mutate(date = as.Date(ifelse(!is.na(date1), date1,
                                              ifelse(!is.na(date2), date2, NA)),
                        origin="1970-01-01")) %>%
  mutate(day_of_the_week = toupper(weekdays(date))) %>%
  mutate(day_of_the_week = ifelse(is.na(date), "Missing", day_of_the_week))

print('Stats on day_of_the_week for debug:')
table(survey_standard$survey_name_year, 
      survey_standard$day_of_the_week, useNA = 'ifany')

# Fill in missing weekpart
survey_standard <- survey_standard %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "SUNDAY",   "WEEKEND", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "MONDAY",   "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "TUESDAY",  "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "WEDNESDAY","WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "THURSDAY", "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "FRIDAY",   "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "SATURDAY", "WEEKEND", weekpart))

print('Stats on final weekpart:')
table(survey_standard$survey_name_year, 
      survey_standard$weekpart, useNA = 'ifany')

# Get field dates from date
field_dates <- survey_standard %>%
  group_by(survey_name, survey_year) %>%
  filter(!is.na(date)) %>%
  summarise(field_start = min(date), field_end = max(date))

print('Field_dates')
print(field_dates)

survey_standard <- survey_standard %>%
  left_join(field_dates, by = c("survey_name", "survey_year"))

# Deal with time
survey_standard <- survey_standard %>%
  mutate(time_string = ifelse(time_string == "Missing - Dummy Record", NA, time_string)) %>%
  mutate(time_string = ifelse(time_string == "Missing - Question Not Asked", NA, time_string)) %>%
  mutate(time_string = ifelse(time_string == "missing", NA, time_string)) %>%
  # interpret the time string
  mutate(time1 = as.POSIXct(strptime(time_string, format = "%l:%M:%S %p"))) %>%
  mutate(time2 = as.POSIXct(strptime(time_string, format = "%l:%M %p"))) %>%
  mutate(time3 = as.POSIXct(strptime(time_string, format = "%H:%M:%S"))) %>%
  mutate(time4 = as.POSIXct(strptime(time_string, format = "%H:%M"))) %>%
  mutate(survey_time_posix = as.POSIXct(ifelse(!is.na(time1), time1,
                                               ifelse(!is.na(time2),
                                                      time2,
                                                      ifelse(!is.na(time3),
                                                             time3,
                                                             time4))),
                                        origin="1970-01-01")
  ) %>%
  # create a time_start (in hours) for day_part_temp
  mutate(time_start = as.numeric(format(survey_time_posix,"%H"))) %>%
  mutate(day_part_temp = 'EVENING') %>%
  mutate(day_part_temp = ifelse(time_start >= 3  & time_start < 6,  'EARLY AM', day_part_temp)) %>%
  mutate(day_part_temp = ifelse(time_start >= 6  & time_start < 10, 'AM PEAK' , day_part_temp)) %>%
  mutate(day_part_temp = ifelse(time_start >= 10 & time_start < 15, 'MIDDAY'  , day_part_temp)) %>%
  mutate(day_part_temp = ifelse(time_start >= 15 & time_start < 19, 'PM PEAK' , day_part_temp)) %>%
  # keep survey_time to output
  mutate(survey_time=format(survey_time_posix, format="%H:%M:%S"))

# table(survey_standard$field_start, useNA = 'ifany')
# table(survey_standard$field_end, useNA = 'ifany')
# table(survey_standard$time_start, useNA = 'ifany')

# examine 'time_period(strata)' data
# first, standardize the time_period name
survey_standard <- survey_standard %>%
  mutate(time_period = ifelse(time_period == "Missing - Dummy Record", NA, time_period)) %>%
  mutate(time_period = recode(time_period,
                              'Early AM'='EARLY AM', 'VERY EARLY'   ='EARLY AM', 'AMO'         ='EARLY AM',
                              'AM Peak' ='AM PEAK',  'AMP'          ='AM PEAK',  'AM COMMUTE'  ='AM PEAK',
                              'Midday'  ='MIDDAY',   'MID'          ='MIDDAY',   'Mid'         ='MIDDAY',
                              'PM Peak' ='PM PEAK',  'PMP'          ='PM PEAK',  'PM COMMUTE'  ='PM PEAK', 'Pm Peak'='PM PEAK',
                              'Evening' ='EVENING',  'Early Evening'='EVENING',  'Late Evening'='EVENING',
                              'Owl'     ='EVENING',  'LATE NIGHT'   ='EVENING',  'PMO'         ='EVENING',
                              'SAT'     ='WEEKEND',  'SUN'          ='WEEKEND'))

# compare time_period (based on 'strata') and day_part_temp (based on 'time_string') values
print('Examine time_period and day_part_temp variables to debug:')
table(survey_standard$survey_name_year,
      survey_standard$time_period, useNA = 'ifany')
table(survey_standard$survey_name_year,
      survey_standard$day_part_temp, useNA = 'ifany')

# the final 'day_part' variable defaults to time_period/strata, and use day_part_temp when time_period is na
survey_standard <- survey_standard %>%
  mutate(day_part = ifelse(is.na(time_period), day_part_temp, time_period))

print('Tabulations of final day_part variable:')
table(survey_standard$survey_name_year,
      survey_standard$day_part, useNA = 'ifany')

# recode 'depart_time/return_time' of ACE 2019 survey to 'depart_hour/return_hour'
# to be consistent with other surveys
survey_standard <- survey_standard %>%
  mutate(depart_time_stamp = as.POSIXct(strptime(depart_time, format = "%l:%M:%S %p"))) %>%
  mutate(depart_hour_ace = as.numeric(format(depart_time_stamp,"%H"))) %>%
  mutate(depart_hour = ifelse(survey_name == 'ACE',         # ACE 2019 generates 'depart_hour' from 'depart_time' 
                              depart_hour_ace,
                              depart_hour)) %>%
  mutate(return_time_stamp = as.POSIXct(strptime(return_time, format = "%l:%M:%S %p"))) %>%
  mutate(return_hour_ace = as.numeric(format(return_time_stamp,"%H"))) %>%
  mutate(return_hour = ifelse(survey_name == 'ACE',         # ACE 2019 generates 'return_hour' from 'return_time' 
                              return_hour_ace,
                              return_hour))

survey_standard <- survey_standard %>%
  select(-date_string, -time_string, -time1, -time2,
         -time3, -time4, -survey_time_posix,
         -depart_time, -return_time,
         -depart_time_stamp, -return_time_stamp,
         -depart_hour_ace, -return_hour_ace)


# Step 9:  Geocode XY to travel model geographies---------------------------------------
print('Geocode XY')
print("survey_standard size:")
print(object.size(survey_standard), units="auto")

# set first_board_[lat,lon] and last_alight_[lat,lon]
survey_standard <- survey_standard %>%
  mutate(first_board_lat  = ifelse(number_transfers_orig_board == 0,  survey_board_lat,  first_board_lat)) %>%
  mutate(first_board_lon  = ifelse(number_transfers_orig_board == 0,  survey_board_lon,  first_board_lon)) %>%
  mutate(last_alight_lat  = ifelse(number_transfers_alight_dest == 0, survey_alight_lat, last_alight_lat)) %>%
  mutate(last_alight_lon  = ifelse(number_transfers_alight_dest == 0, survey_alight_lon, last_alight_lon))

# get nation lat/lon bounding box for later basic fixing
USA <- tigris::nation(year = 2020, progress_bar=FALSE)
USA_bounding_box <- st_bbox(st_transform(USA, crs=CRS_WGS84))
print("USA_bounding_box:")
str(USA_bounding_box)

# convert outside of USA bounding box to NA
print("NAs before bounding box check")
print(survey_standard %>% summarise(across(ends_with('_lat'), ~ sum(is.na(.)))))
print(survey_standard %>% summarise(across(ends_with('_lon'), ~ sum(is.na(.)))))

survey_standard <- survey_standard %>% mutate(
  across(ends_with('_lat'), ~ifelse(. < USA_bounding_box$ymin, NA, .)),
  across(ends_with('_lat'), ~ifelse(. > USA_bounding_box$ymax, NA, .)),
  across(ends_with('_lon'), ~ifelse(. < USA_bounding_box$xmin, NA, .)),
  across(ends_with('_lon'), ~ifelse(. > USA_bounding_box$xmax, NA, .)),
)
print("NAs after USA bounding box check")
print(survey_standard %>% summarise(across(ends_with('_lat'), ~ sum(is.na(.)))))
print(survey_standard %>% summarise(across(ends_with('_lon'), ~ sum(is.na(.)))))

# Prepare and write locations that need to be geo-coded to disk
survey_standard <- survey_standard %>%
  mutate(unique_ID = paste(ID, survey_name, survey_year, sep = "___"))

survey_lat <- survey_standard %>% 
  select(unique_ID, ends_with("_lat")) %>%
  pivot_longer(
    cols = ends_with("_lat"), 
    names_to="location", 
    values_to="y_coord",
    values_drop_na = TRUE) %>% mutate(
      location = str_sub(location, 0, -5))
print("survey_lat")
print(survey_lat)

survey_lon <- survey_standard %>% 
  select(unique_ID, ends_with("_lon")) %>%
  pivot_longer(
    cols = ends_with("_lon"), 
    names_to="location", 
    values_to="x_coord",
    values_drop_na = TRUE) %>% mutate(
      location = str_sub(location, 0, -5))
print("survey_lon")
print(survey_lon)

survey_coords <- inner_join(survey_lat, survey_lon, by = c("unique_ID", "location")) 
remove(survey_lat, survey_lon)

## Geocode All Locations to shapes

survey_coords_spatial <- sf::st_as_sf(survey_coords, coords = c("x_coord", "y_coord"), crs = CRS_WGS84)
survey_coords_spatial <- sf::st_transform(survey_coords_spatial, crs = CRS_NAD83_CAZ6_FT)
survey_coords_spatial <- rowid_to_column(survey_coords_spatial, var = "spatial_id")
num_coords <- nrow(survey_coords_spatial)

# this is a lot of rows, 1M+
print("survey_coords_spatial:")
print(paste("num_coords: ",num_coords))
str(survey_coords_spatial)
print(head(survey_coords_spatial))

for (i in rownames(f_shapefile_paths)) {
  shape_name = f_shapefile_paths[i, "shape"]
  shape_col = f_shapefile_paths[i, "shape_col"]
  print(paste("======= Mapping locations to",shape_name,"======="))

  # read shapefile
  if (f_shapefile_paths[i, "shapefile"] == "tigris") {
    shapefile <- tigris::tracts(state = "CA", county=MEGAREGION, cb=FALSE, year=2020, progress_bar=FALSE)
  } else {
    shapefile <- sf::st_read(f_shapefile_paths[i, "shapefile"])
  }
  shapefile <- select(shapefile, !!shape_col, geometry)
  # transform to match survey_coords_spatial
  shapefile <- st_transform(shapefile, crs = st_crs(survey_coords_spatial))
  # create area column and sort descending
  shapefile <- shapefile %>% 
    mutate(area = st_area(shapefile)) %>%
    arrange(desc(area))
  # create row_id from rowid
  shapefile <- rowid_to_column(shapefile, var = "row_id")

  print(paste("Read",nrow(shapefile),"rows from",f_shapefile_paths[i, "shapefile"]))
  print(shapefile)
  str(shapefile)

  # Use st_intersects() first.  This is very fast!  less than one minute
  start.time <- Sys.time()
  print(paste("Starting st_intersects at",format(start.time, "%a %b %d %X %Y")))
  shapefile_intersect <- st_intersects(x=survey_coords_spatial, y=shapefile, sparse=TRUE)

  end.time <- Sys.time()
  print(paste("Finished st_intersects at",format(end.time, "%a %b %d %X %Y")))
  time.taken <- round((end.time - start.time)/60.0,2)
  print(paste("This took",time.taken,"minutes"))

  # print("shapefile_intersect:")
  # this is a list of lists of ints
  # print(shapefile_intersect)
  # str(shapefile_intersect)

  # convert list of lists to dataframe
  shapefile_intersect_df <- data.frame(
    spatial_id = rep(seq_along(shapefile_intersect), lengths(shapefile_intersect)),
    row_id = unlist(shapefile_intersect)
  )
  print("shapefile_intersect_df:")
  print(head(shapefile_intersect_df))
  str(shapefile_intersect_df)

  # is there more than one row for spatial_id ?  log it:
  dup_spatial_id <- shapefile_intersect_df %>% add_count(spatial_id) %>% filter(n > 1)
  print("dup_spatial_id:")
  print(dup_spatial_id)

  # keep only first row for each spatial_id
  shapefile_intersect_df <- dplyr::distinct(shapefile_intersect_df, spatial_id, .keep_all=TRUE)

  print("survey_coords_spatial:")
  print(paste("Before left_join, nrow=",nrow(survey_coords_spatial)))

  # left_join to survey_coords_spatial
  survey_coords_spatial <- left_join(
    survey_coords_spatial,
    shapefile_intersect_df,
    by = "spatial_id"
  )

  # and again to shapefile
  survey_coords_spatial <- left_join(
    survey_coords_spatial,
    st_drop_geometry(shapefile),
    by = "row_id"
  ) %>% select(-row_id, -area)

  # rename from shape_col to shape_name
  names(survey_coords_spatial)[names(survey_coords_spatial) == shape_col] <- shape_name

  print(paste("After left_joins, nrow=",nrow(survey_coords_spatial)))
  print(head(survey_coords_spatial))
  stopifnot(nrow(survey_coords_spatial)==num_coords)

  print(st_drop_geometry(survey_coords_spatial) %>% dplyr::count(!!as.name(shape_name)))

  null_shape <- filter(survey_coords_spatial, is.na(!!shape_name))
  print(paste("Number of rows with null",shape_name,":",nrow(null_shape)))

  # use nearest: sf::st_nearest_feature
}

# go back to wide form
survey_coords <- st_drop_geometry(survey_coords_spatial) %>%
  pivot_wider(
    id_cols = unique_ID,
    names_from = location,
    values_from = f_shapefile_paths$shape,
    names_glue = "{location}_{.value}",
  )
print("head(survey_coords):")
print(head(survey_coords))

# Joins
survey_standard <- survey_standard %>%
  left_join(survey_coords, by = c("unique_ID"))

print("survey_standard size:")
print(object.size(survey_standard), units="auto")
print("str(survey_standard):")
str(survey_standard, list.len=ncol(survey_standard))

remove(survey_coords,
       survey_coords_spatial)

# calculate some distances between points
survey_standard <- survey_standard %>% 
  rowwise() %>% 
  mutate(distance_orig.dest = distHaversine(
    c(orig_lon,orig_lat),
    c(dest_lon,dest_lat),
    r=EARTH_RADIUS_MILES
  ),
  distance_board.alight = distHaversine(
    c(survey_board_lon, survey_board_lat),
    c(survey_alight_lon,survey_alight_lat),
    r=EARTH_RADIUS_MILES
  ),
  distance_orig.first_board = distHaversine(
    c(orig_lon,       orig_lat),
    c(first_board_lon,first_board_lat),
    r=EARTH_RADIUS_MILES
  ),
  distance_orig.survey_board = distHaversine(
    c(orig_lon,        orig_lat),
    c(survey_board_lon,survey_board_lat),
    r=EARTH_RADIUS_MILES
  ),
  distance_survey_alight.dest = distHaversine(
    c(survey_alight_lon,survey_alight_lat),
    c(dest_lon,         dest_lat),
    r=EARTH_RADIUS_MILES
  ),
  distance_last_alight.dest = distHaversine(
    c(last_alight_lon,last_alight_lat),
    c(dest_lon,       dest_lat),
    r=EARTH_RADIUS_MILES
  )
) %>% ungroup()
print("survey_standard distance variables:")
print(head(survey_standard %>% select(starts_with("distance_"))))

# Step 10:  Clean up data types and export Standard Survey files------------------------
print('Final cleanup')

print("survey_standard size:")
print(object.size(survey_standard), units="auto")
print("str(survey_standard):")
str(survey_standard, list.len=ncol(survey_standard))

# Create an ancillary data set for requested variables
ancillary_df <- survey_standard %>%
  select(unique_ID,
         at_school_after_dest_purp,
         at_school_prior_to_orig_purp,
         at_work_after_dest_purp,
         at_work_prior_to_orig_purp)

# Create an ancillary data set for decomposition analysis
survey_decomposition <- survey_standard %>%
  mutate(weight = ifelse(is.na(weight), 0.0, weight)) %>%
  mutate(trip_weight = ifelse(boardings > 0, weight / boardings, weight))%>%
  select(unique_ID,
         access_mode,
         dest_purp,
         direction,
         egress_mode,
         first_route_after_survey_alight,
         first_route_before_survey_board,
         number_transfers_alight_dest,
         number_transfers_orig_board,
         onoff_enter_station,
         onoff_exit_station,
         orig_purp,
         route,
         second_route_after_survey_alight,
         second_route_before_survey_board,
         survey_alight_lat,
         survey_alight_lon,
         survey_board_lat,
         survey_board_lon,
         survey_type,
         third_route_after_survey_alight,
         third_route_before_survey_board,
         weekpart,
         weight,
         ID,
         survey_name,
         survey_year,
         survey_tech,
         first_before_operator,
         second_before_operator,
         third_before_operator,
         first_after_operator,
         second_after_operator,
         third_after_operator,
         first_before_technology,
         second_before_technology,
         third_before_technology,
         first_after_technology,
         second_after_technology,
         third_after_technology,
         transfer_from,
         transfer_to,
         first_board_tech,
         last_alight_tech,
         boardings,
         day_of_the_week,
         field_start,
         field_end,
         day_part,
         trip_weight)

sprintf('Export %d rows and %d columns of survey_decomposition data to %s and %s',
        nrow(survey_decomposition),
        ncol(survey_decomposition),
        f_output_decom_rdata_path,
        f_output_decom_csv_path)
saveRDS(survey_decomposition, file = f_output_decom_rdata_path)
write.csv(survey_decomposition, file = f_output_decom_csv_path,  row.names = FALSE)

# Drop variables we don't want to carry forward to standard dataset
survey_standard <- survey_standard %>%
  select(-survey_name_year,
         -at_school_after_dest_purp,
         -at_school_prior_to_orig_purp,
         -at_work_after_dest_purp,
         -at_work_prior_to_orig_purp,
         -date,
         -date1,
         -date2,
         -time_start,
         -day_part_temp,
         -year_born,
         -number_transfers_alight_dest,
         -number_transfers_orig_board,
         -first_route_before_survey_board,
         -first_route_after_survey_alight,
         -second_route_before_survey_board,
         -second_route_after_survey_alight,
         -third_route_before_survey_board,
         -third_route_after_survey_alight,
         -alt_weight,
         -rate_conductors,
         -rate_overall,
         -rate_schedules,
         -rate_station,
         -rate_value,
         -wcode,
         -tweight)


## Write RDS to disk

# Compute trip weight, replace missing weights with zero, and set field language to interview language
survey_standard <- survey_standard %>%
  mutate(weight = ifelse(is.na(weight), 0.0, weight)) %>%
  mutate(trip_weight = ifelse(boardings > 0, weight / boardings, weight)) %>%
  mutate(field_language = interview_language)

# put new column, survey_time, at the end
survey_standard_cols <- survey_standard %>%
  select(-survey_time) %>%
  colnames()
survey_standard <- survey_standard %>%
  select(all_of(survey_standard_cols), survey_time)

sprintf('Export %d rows and %d columns of survey_standard data to %s and %s',
        nrow(survey_standard),
        ncol(survey_standard),
        f_output_rds_path,
        f_output_csv_path)
saveRDS(survey_standard, file = f_output_rds_path)
write.csv(survey_standard, file = f_output_csv_path, row.names = FALSE)

sprintf('Export %d rows and %d columns of ancillary data to %s and %s',
        nrow(ancillary_df),
        ncol(ancillary_df),
        f_ancillary_output_rdata_path,
        f_ancillary_output_csv_path)

saveRDS(ancillary_df, file = f_ancillary_output_rdata_path)
write.csv(ancillary_df, file = f_ancillary_output_csv_path, row.names = FALSE)


# Step 11:  Combine with legacy survey data and export----------------------------------

# load legacy data
load(f_legacy_rdata_path)
sprintf('Load %d rows of legacy data', nrow(survey.legacy))
print("str(survey.legacy):")
str(survey.legacy)
sprintf('Combine with %d rows of standard data', nrow(survey_standard))

survey_combine <- combine_data(survey_standard,
                               survey.legacy)
print("survey_combine size:")
print(object.size(survey_combine), units="auto")
print("head(survey_combine):")
print(head(survey_combine))
print("survey_combine (standard + legacy):")
str(survey_combine, list.len=ncol(survey_combine))

# export combined data
sprintf('Export %d rows and %d columns of legacy-standard combined data to %s and %s',
        nrow(survey_combine),
        ncol(survey_combine),
        f_combined_rdata_path,
        f_combined_csv_path)
save(survey_combine, file = f_combined_rdata_path)
write.csv(survey_combine, f_combined_csv_path, row.names = FALSE)
