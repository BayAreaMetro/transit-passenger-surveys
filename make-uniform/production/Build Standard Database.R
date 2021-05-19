## Administration

#### Purpose
# Procedure to translate any number of on-board survey data sets into a single
# dataset with common variables and common responses. In this script, we put in place
# procedures to process surveys into a standard database.  See
# `Extract Variables from Legacy Surveys.Rmd` for procedures to extract
# variables from legacy surveys (i.e., those with SAS summary scripts).

# To use these scripts, analysts must intervene at specific locations. To assist
# in this work, we've added notes where interventions are necessary.


#### Libraries

list_of_packages <- c(
  "geosphere",
  "ggplot2",
  "knitr",
  "rlang",
  "sf",
  "tidyverse"
)

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

# The working directory is set as the location of the script. All other paths will be relative.

wd <- paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")
setwd(wd)

# Run standard database functions

source("Build Standard Database Functions.R")

#### Parameters
OPERATOR_DELIMITER = "___"
ROUTE_DELIMITER = "&&&"


# _User Intervention_
# The code uses r-user-name-specific relative directories. Add your name and your
# relative (to this directory) path to the `Data and Reports` directory to the two
# vectors in the below code block. Run `Sys.getenv('USERNAME')` to determine your R
# user name.

#### Remote file names
user_list <- data.frame(
  user = c("helseljw",
           "ywang",
           "SIsrael"),
  path = c("~/GitHub/onboard-surveys/Data and Reports/",
           "M:/Data/OnBoard/Data and Reports/",
           "M:/Data/OnBoard/Data and Reports/"
  )
)

# _User Intervention_
# When adding a new operator, the user must: add the path to the survey data in
# the code block below, e.g., `f_bart_survey_path`

dir_path <- user_list %>%
  filter(user == Sys.getenv("USERNAME")) %>%
  .$path

f_dict_standard <- "Dictionary for Standard Database.csv"
f_canonical_station_path <- paste0(dir_path,"Geography Files/Passenger_Railway_Stations_2018.shp")
f_taps_coords_path <- paste0(dir_path, "_geocoding Standardized/TAPs/TM2 TAPS/TM2 tap_node.csv")
f_tm1_taz_shp_path <- paste0(dir_path, "_geocoding Standardized/TM1_taz/bayarea_rtaz1454_rev1_WGS84.shp")
f_tm2_taz_shp_path <- paste0(dir_path, "_geocoding Standardized/TM2_Zones/tazs.shp")
f_tm2_maz_shp_path <- paste0(dir_path, "_geocoding Standardized/TM2_Zones/mazs.shp")
f_geocode_column_names_path <- "bespoke_survey_station_column_names.csv"
f_canonical_routes_path <- "canonical_route_crosswalk.csv"

f_actransit_survey_path <- paste0(dir_path,
                                  "AC Transit/2018/As CSV/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_fixTransfers_NO POUND OR SINGLE QUOTE.csv")
f_bart_survey_path <- paste0(dir_path,
                             "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")
f_caltrain_survey_path <- paste0(dir_path,
                                 "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")
f_marin_survey_path <- paste0(dir_path,
                              "Marin Transit/As CSV/marin transit_data file_final01222021_NO POUND OR SINGLE QUOTE.csv")
f_muni_survey_path <- paste0(dir_path,
                             "Muni/As CSV/MUNI_DRAFTFINAL_20171114_fixedTransferNum_NO POUND OR SINGLE QUOTE.csv")
f_napa_survey_path <- paste0(dir_path,
                             "Napa Vine/As CSV/Napa Vine Transit OD Survey Data_Dec10_Submitted_toAOK_with_transforms NO POUND OR SINGLE QUOTE.csv")
f_vta_survey_path <- paste0(dir_path,
                            "VTA/As CSV/VTA_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")
f_fast_survey_path <- paste0(dir_path,
                            "Solano County/As CSV/FAST_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_rvdb_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/Rio Vista Delta Breeze_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_vcc_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/Vacaville City Coach_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_soltrans_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/SolTrans_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_ace_survey_path <- paste0(dir_path,
                             "ACE/2019/As CSV/ACE19_Final Data_AddCols_RecodeRoute_NO POUND OR SINGLE QUOTE.csv")
f_unioncity_survey_path <- paste0(dir_path,
                                  "Union City/2017/As CSV/Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv")
f_sonomact_survey_path <- paste0(dir_path,
                                 "Sonoma County/2018/As CSV/sc transit_data file_final_spring 2018_addRoutesCols NO POUND NO SINGLE QUOTE.csv")
f_smart_survey_path <- paste0(dir_path,
                              "SMART/As CSV/SMART Standardized Final Data_addRouteCols_NO POUND NO SINGLE QUOTE.csv")
f_weta_survey_path <- paste0(dir_path,
                             "WETA/WETA 2018/As CSV/WETA-Final Weighted Data-Standardized_addCols_NO POUND OR SINGLE QUOTE.csv")
f_westcat_survey_path <- paste0(dir_path,
                                "WestCAT/As CSV/WestCAT_addCols_recodeRoute_NO POUND OR SINGLE QUOTE.csv")
f_lavta_survey_path <- paste0(dir_path,
                              "LAVTA/2018/As CSV/OD_20181207_LAVTA_Submittal_FINAL_addCols_NO POUND OR SINGLE QUOTE.csv")
f_tridelta2019_survey_path <- paste0(dir_path,
                                     "Tri Delta/2019/As CSV/TriDelta_ODSurvey_Dataset_Weights_03272019_FinalDeliv_addCols_NO POUND OR SINGLE QUOTE.csv")
f_cccta2019_survey_path <- paste0(dir_path,
                                  "County Connection/2019/As CSV/OD_20191105_CCCTA_Submittal_FINAL Expanded_addCols_NO POUND OR SINGLE QUOTE.csv")
f_ggtransit_survey_path <- paste0(dir_path,
                                  "Golden Gate Transit/2018/As CSV/20180907_OD_GoldenGate_allDays_addCols_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")
f_napavine2019_survey_path <- paste0(dir_path,
                                     "Napa Vine/2019/As CSV/Napa Vine_FINAL Data_addCols_NO POUND OR SINGLE QUOTE.csv")
f_petaluma2018_survey_path <- paste0(dir_path,
                                     "Petaluma/2018/As CSV/20180530_OD_Petaluma_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv")
f_SantaRosaCityBus2018_survey_path <- paste0(dir_path,
                                             "Santa Rosa CityBus/2018/As CSV/20180522_OD_SantaRosa_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv")
f_capitolcorridor2019_survey_path <- paste0(dir_path,
                                             "Capitol Corridor/OD Survey 2019/As CSV/CAPCO19 Data-For MTC_NO POUND OR SINGLE QUOTE.csv")

today = Sys.Date()
f_output_rds_path <- paste0(dir_path,
                            "_data Standardized/survey_standard_", today, ".RDS")
f_output_csv_path <- paste0(dir_path,
                            "_data Standardized/survey_standard_", today, ".csv")
f_ancillary_output_rdata_path <- paste0(dir_path,
                                        "_data Standardized/ancillary_variable_", today, ".RDS")
f_ancillary_output_csv_path <- paste0(dir_path,
                                      "_data Standardized/ancillary_variables_", today, ".csv")
f_output_decom_rdata_path <- paste0(dir_path,
                                    "_data Standardized/decomposition/survey_decomposition_", today, ".RDS")
f_output_decom_csv_path <- paste0(dir_path,
                                  "_data Standardized/decomposition/survey_decomposition_", today, ".csv")

# Setup the log file
run_log <- file(sprintf("%s_data Standardized/Build_Standard_Database_%s.log",dir_path,today))
sink(run_log, append=TRUE, type = 'output')
sink(run_log, append=TRUE, type = "message")


# _User Intervention_
# When adding a new operator, the user must update the dictionary files that translate
# the usually-bespoke survey coding to the standard coding. Edits to the dictionary should be made in
# the file `Dictionary for Standard Database.csv`. The existing entries in the
# dictionary *should* explicate the expected task.

## Prepare dictionaries
dictionary_all <- read.csv(f_dict_standard,
                           header = TRUE) %>%
  rename_all(tolower) %>% 
  mutate(generic_variable=str_trim(generic_variable))        # Remove outside whitespace

# Prepare separate dictionaries for categorical and non-categorical variables
dictionary_non <- dictionary_all %>%
  filter(generic_response == 'NONCATEGORICAL') %>%
  select(operator, survey_year, survey_variable, generic_variable)

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


## Add canonical station locations and crosswalk for recode
canonical_station_shp <- st_read(f_canonical_station_path)

# Create index of stations
canonical_station_shp <- canonical_station_shp %>%
  select(station_na, agencyname, mode) %>%
  mutate(index = 1:nrow(canonical_station_shp))

# Add canonical route crosswalk
canonical_routes_crosswalk <- read.csv(f_canonical_routes_path)

## Add surveys
print('Read and combine survey raw data from multiple operators')

# _User Intervention_
# When adding a new operator, create a `read_operator` call and add the resulting
# dataframe to `survey_combine` in the last step. For operators that provide service
# across multiple technologies, enter the dominant technology here and add route-specific
# changes to the `canonical` route database (e.g., SF Muni Metro routes are `light rail`)

ac_transit_df <- read_operator('AC Transit',
                               2018,
                               'local bus',
                               f_actransit_survey_path,
                               dictionary_all,
                               canonical_station_shp)

bart_df <- read_operator('BART',
                         2015,
                         'heavy rail',
                         f_bart_survey_path,
                         dictionary_all,
                         canonical_station_shp)

caltrain_df <- read_operator('Caltrain',
                             2014,
                             'commuter rail',
                             f_caltrain_survey_path,
                             dictionary_all,
                             canonical_station_shp)

muni_df <- read_operator('SF Muni',
                         2017,
                         'local bus',
                         f_muni_survey_path,
                         dictionary_all,
                         canonical_station_shp)

marin_df <- read_operator('Marin Transit',
                          2017,
                          'local bus',
                          f_marin_survey_path,
                          dictionary_all,
                          canonical_station_shp)

napa_vine_df <- read_operator('Napa Vine',
                              2014,
                              'local bus',
                              f_napa_survey_path,
                              dictionary_all,
                              canonical_station_shp)

vta_df <- read_operator('VTA',
                        2017,
                        'local bus',
                        f_vta_survey_path,
                        dictionary_all,
                        canonical_station_shp)

fast_df <- read_operator('FAST',
                         2017,
                         'local bus',
                         f_fast_survey_path,
                         dictionary_all,
                         canonical_station_shp)

rvdb_df <- read_operator('Delta Breeze',
                         2017,
                         'local bus',
                         f_rvdb_survey_path,
                         dictionary_all,
                         canonical_station_shp)

vcc_df <- read_operator('City Coach',
                        2017,
                        'local bus',
                        f_vcc_survey_path,
                        dictionary_all,
                        canonical_station_shp)

soltrans_df <- read_operator('Soltrans',
                             2017,
                             'local bus',
                             f_soltrans_survey_path,
                             dictionary_all,
                             canonical_station_shp)

ace_df <- read_operator('ACE',
                        2019,
                        'commuter rail',
                        f_ace_survey_path,
                        dictionary_all,
                        canonical_station_shp)

unioncity_df <- read_operator('Union City Transit',
                              2017,
                              'local bus',
                              f_unioncity_survey_path,
                              dictionary_all,
                              canonical_station_shp)

sonomact_df <- read_operator('Sonoma County Transit',
                              2018,
                              'local bus',
                              f_sonomact_survey_path,
                              dictionary_all,
                              canonical_station_shp)

smart_df <- read_operator('Sonoma-Marin Area Rail Transit',
                          2018,
                          'commuter rail',
                          f_smart_survey_path,
                          dictionary_all,
                          canonical_station_shp)

weta_df <- read_operator('WETA',
                          2019,
                          'ferry',
                          f_weta_survey_path,
                          dictionary_all,
                          canonical_station_shp)

westcat_df <- read_operator('WestCAT',
                             2017,
                             'local bus',
                             f_westcat_survey_path,
                             dictionary_all,
                             canonical_station_shp)

lavta_df <- read_operator('LAVTA',
                          2018,
                          'local bus',
                          f_lavta_survey_path,
                          dictionary_all,
                          canonical_station_shp)

tridelta2019_df <- read_operator('TriDelta',
                                 2019,
                                 'local bus',
                                 f_tridelta2019_survey_path,
                                 dictionary_all,
                                 canonical_station_shp)

cccta2019_df <- read_operator('County Connection',
                              2019,
                              'local bus',
                              f_cccta2019_survey_path,
                              dictionary_all,
                              canonical_station_shp)

ggtransit_df <- read_operator('Golden Gate Transit',
                              2018,
                              'express bus',
                              f_ggtransit_survey_path,
                              dictionary_all,
                              canonical_station_shp)

napavine2019_df <- read_operator('Napa Vine',
                                 2019,
                                 'local bus',
                                 f_napavine2019_survey_path,
                                 dictionary_all,
                                 canonical_station_shp)

petaluma2018_df <- read_operator('Petaluma Transit',
                                 2018,
                                 'local bus',
                                 f_petaluma2018_survey_path,
                                 dictionary_all,
                                 canonical_station_shp)

SantaRosaCityBus2018_df <- read_operator('Santa Rosa CityBus',
                                         2018,
                                         'local bus',
                                         f_SantaRosaCityBus2018_survey_path,
                                         dictionary_all,
                                         canonical_station_shp)

capitolcorridor2019_df <- read_operator('Capitol Corridor',
                                        2019,
                                        'commuter rail',
                                        f_capitolcorridor2019_survey_path,
                                        dictionary_all,
                                        canonical_station_shp)

survey_combine <- bind_rows(
  ac_transit_df,
  bart_df,
  caltrain_df,
  muni_df,
  marin_df,
  napa_vine_df,
  vta_df,
  fast_df,
  rvdb_df,
  vcc_df,
  soltrans_df,
  ace_df,
  unioncity_df,
  sonomact_df,
  smart_df,
  weta_df,
  westcat_df,
  lavta_df,
  tridelta2019_df,
  cccta2019_df,
  ggtransit_df,
  napavine2019_df,
  petaluma2018_df,
  SantaRosaCityBus2018_df,
  capitolcorridor2019_df
)

remove(
       ac_transit_df,
       bart_df,
       caltrain_df,
       muni_df,
       marin_df,
       napa_vine_df,
       vta_df,
       fast_df,
       rvdb_df,
       vcc_df,
       soltrans_df,
       ace_df,
       unioncity_df,
       sonomact_df,
       smart_df,
       weta_df,
       westcat_df,
       lavta_df,
       tridelta2019_df,
       cccta2019_df,
       ggtransit_df,
       napavine2019_df,
       petaluma2018_df,
       SantaRosaCityBus2018_df,
       capitolcorridor2019_df
      )


## Flatten
print('Join standard_variable and standard_response to raw data')

# Join the dictionary and prepare the categorical variables

survey_cat <- survey_combine %>%
  left_join(dictionary_cat, by = c("operator", "survey_year", "survey_variable", "survey_response")) %>%
  filter(!is.na(generic_variable))

# Join the dictionary and prepare the non-categorical variables

rail_crosswalk_df <- canonical_routes_crosswalk %>%
  filter(survey == "GEOCODE") %>%
  select(survey_name, canonical_name)

survey_non <- survey_combine %>%
  left_join(dictionary_non, by = c("operator", "survey_year", "survey_variable")) %>%
  filter(!is.na(generic_variable)) %>%
  mutate(generic_response = survey_response) %>%
  left_join(canonical_routes_crosswalk %>% select(-technology, -technology_detail, -operator_detail),
            by = c("operator" = "survey", "survey_year", "survey_response" = "survey_name")) %>%
  mutate(generic_response = ifelse(str_detect(generic_variable, "route") & !is.na(canonical_name), canonical_name, generic_response)) %>%
  select(-canonical_name, -canonical_operator) %>%
  left_join(rail_crosswalk_df, by = c("generic_response" = "survey_name")) %>%
  mutate(generic_response = ifelse(!is.na(canonical_name), canonical_name, generic_response)) %>%
  select(-canonical_name)

# Combine the categorical and non-categorical survey data and prepare to flatten
survey_flat <- bind_rows(survey_cat, survey_non) %>%
  select(-survey_variable, -survey_response) %>%
  spread(generic_variable, generic_response) %>%
  arrange(operator, survey_year, ID) %>%
  mutate(route = ifelse(operator == "BART", paste0("BART", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  mutate(route = ifelse(operator == "Caltrain", paste0("CALTRAIN", OPERATOR_DELIMITER, onoff_enter_station, ROUTE_DELIMITER, onoff_exit_station), route)) %>%
  left_join(rail_crosswalk_df, by = c("route" = "survey_name")) %>%
  mutate(route = ifelse(!is.na(canonical_name), canonical_name, route)) %>%
  select(-canonical_name)

remove(survey_cat,
       survey_non)


## Update survey technology
print('Update technology for multiple-tech operators')

# _User Intervention_
# As noted above, when the operator data is read in, it assumes every route in the survey uses
# the same technology (e.g., all Muni routes are local bus). In face, some operators operate
# multiple technologies. These bespoke technologies are added here. These changes are recorded
# in the `canonical route name database` and must be updated manually.

survey_flat <- survey_flat %>%
  left_join(canonical_routes_crosswalk %>% select(-survey_name, -technology_detail) %>% unique(),
            by = c("operator" = "survey", "route" = "canonical_name", "survey_year"))

# for multi-tech operators, survey_tech = technology
survey_flat <- survey_flat %>%
  mutate(survey_tech = ifelse(((operator == 'AC Transit') & (survey_year == 2018)) | (
                               (operator == 'FAST') & (survey_year == 2017)) | (
                               (operator == 'Golden Gate Transit') & (survey_year == 2018)) | (
                               (operator == 'Napa Vine') & (survey_year == 2019)) | (
                               (operator == 'Napa Vine') & (survey_year == 2014)) | (
                               (operator == 'SF Muni') & (survey_year == 2017)) | (
                               (operator == 'VTA') & (survey_year == 2017)) | (
                               (operator == 'WestCAT') & (survey_year == 2017)), 
                              technology,
                              survey_tech))

print('Stats on technology by operator:')
table(survey_flat$operator, survey_flat$survey_tech, useNA = 'ifany')


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
    str_detect(year_born_four_digit,"Missing") | str_detect(year_born_four_digit,"Not Provided") | str_detect(year_born_four_digit,'REFUSED'),
    NA,
    year_born_four_digit)) %>%
  mutate(year_born = ifelse(is.na(year_born), NA, as.numeric(year_born))) %>%
  select(-year_born_four_digit)


# Manual fixes to year born
survey_standard <- survey_standard %>%
  mutate(survey_year = as.numeric(survey_year)) %>%
  mutate(year_born = ifelse(year_born == 1900, 2000, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1901, 2001, year_born)) %>%
  mutate(year_born = ifelse(year_born == 3884, 1984, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1899, NA, year_born))

print('Stats on year_born:')
table(survey_standard$year_born, useNA = 'ifany')

# Compute approximate respondent age
survey_standard <- survey_standard %>%
  mutate(approximate_age = ifelse(!is.na(year_born) & survey_year >= year_born, survey_year - year_born, NA)) %>%
  mutate(approximate_age = ifelse(approximate_age == 0, NA, approximate_age))

print('Stats on approximate_age:')
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
                            "grade_school", dest_purp)) %>%

  # for Capitol Corridor 2019 survey, use 'trip_purp'
  mutate(trip_purp = ifelse(trip_purp == "school", "high school", trip_purp)) %>%
  mutate(trip_purp = ifelse(trip_purp == "school" & approximate_age < 14,
                            "grade_school", trip_purp)) %>%
  mutate(trip_purp = ifelse(trip_purp == "school" & approximate_age > 18,
                            "college", trip_purp))

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

# for Capitol Corridor 2019 survey, use 'trip_purp' instead of 'orig/dest_purp'
  mutate(tour_purp = ifelse(operator == 'Capitol Corridor', trip_purp, tour_purp)) %>%
  mutate(tour_purp_case = ifelse(operator == 'Capitol Corridor', 'CC trip_purp', tour_purp_case)) %>%
  
# finally, if work-related or business apt, categorize as 'other maintenance'

  mutate(tour_purp = ifelse(tour_purp %in% c('work-related','business apt'), 'other maintenance', tour_purp))
      
# Output frequency file, test file to review missing cases, and test of duplicates

print('Stats on tour_purp:')
table(survey_standard$tour_purp, useNA = 'ifany')

print('Examine interim output "missing_tour_df" for records with missing tour_purp')
missing_tour_df <- survey_standard %>% filter(tour_purp=='missing') %>% select(operator, survey_year, ID, orig_purp,dest_purp,tour_purp,at_school_after_dest_purp,at_school_prior_to_orig_purp,at_work_after_dest_purp,at_work_prior_to_orig_purp,approximate_age)


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


# Step 4:  Automobile Sufficiency ------------------------------------------------------
print('Calculate automobile sufficiency')

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

# map vehicles and workers counts to numeric values in order to calculate auto-sufficiency
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
  mutate(auto_suff = ifelse(vehicle_numeric_cat == 0, 'zero autos', 'missing')) %>%
  mutate(auto_suff = ifelse(vehicle_numeric_cat > 0 &
                              worker_numeric_cat > 0  &
                              worker_numeric_cat >  vehicle_numeric_cat,
                            'auto negotiating',
                            auto_suff)) %>%
  mutate(auto_suff = ifelse(vehicle_numeric_cat > 0 &
                              worker_numeric_cat >= 0 &
                              worker_numeric_cat <= vehicle_numeric_cat,
                            'auto sufficient',
                            auto_suff)) %>%
  mutate(auto_suff = ifelse((vehicle_numeric_cat == 'missing') | (
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
                            auto_suff))

print('Stats on auto_suff:')
table(survey_standard$auto_suff, useNA = 'ifany')

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
  select(canonical_name, canonical_operator, operator_detail) %>%
  unique() %>%
  rename(transfer_route_name = canonical_name,
         transfer_operator_canonical = canonical_operator,
         transfer_operator_detail = operator_detail)

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
  select(-survey_name) %>%
  rename(temp_tech = technology) %>%
  unique()

tech_crosswalk_expansion_list <- tech_crosswalk_df %>%
  select(survey, survey_year) %>%
  unique() %>%
  filter(!survey %in% c("BART", "Caltrain", "GEOCODE"))

tech_crosswalk_expansion_df <- tech_crosswalk_df %>%
  filter(survey == "GEOCODE") %>%
  select(-survey, -survey_year)

tech_crosswalk_expansion_df <- tech_crosswalk_expansion_list %>%
  merge(tech_crosswalk_expansion_df, by = NULL)

tech_crosswalk_df <- tech_crosswalk_df %>%
  bind_rows(tech_crosswalk_expansion_df) %>%
  filter(survey != "GEOCODE") %>%
  select(-operator_detail, -technology_detail)

remove(tech_crosswalk_expansion_df, tech_crosswalk_expansion_list)

survey_standard <- survey_standard %>%
  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "first_before_operator" = "canonical_operator",
                                     "first_route_before_survey_board" = "canonical_name")) %>%
  rename(first_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "second_before_operator" = "canonical_operator",
                                     "second_route_before_survey_board" = "canonical_name")) %>%
  rename(second_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "third_before_operator" = "canonical_operator",
                                     "third_route_before_survey_board" = "canonical_name")) %>%
  rename(third_before_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "first_after_operator" = "canonical_operator",
                                     "first_route_after_survey_alight" = "canonical_name")) %>%
  rename(first_after_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "second_after_operator" = "canonical_operator",
                                     "second_route_after_survey_alight" = "canonical_name")) %>%
  rename(second_after_technology = temp_tech) %>%

  left_join(tech_crosswalk_df, by =c("operator" = "survey", "survey_year",
                                     "third_after_operator" = "canonical_operator",
                                     "third_route_after_survey_alight" = "canonical_name")) %>%
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

print('Stats on transfer_from:')
table(survey_standard$transfer_from, useNA = 'ifany')
print('Stats on transfer_to:')
table(survey_standard$transfer_to, useNA = 'ifany')
print('Stats on survey_tech:')
table(survey_standard$survey_tech, useNA = 'ifany')
print('Stats on first_board_tech:')
table(survey_standard$first_board_tech, useNA = 'ifany')
print('Stats on last_alight_tech:')
table(survey_standard$last_alight_tech, useNA = 'ifany')


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
survey_standard <- survey_standard %>%
  mutate(boardings = 1L) %>%
  mutate(boardings = ifelse(!(first_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_before_technology == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(first_after_technology   == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_after_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_after_technology   == "Missing"), boardings + 1, boardings))

print('Stats on number of boardings:')
table(survey_standard$boardings, useNA = 'ifany')

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

print('Stats on number_transfers_orig_board:')
table(survey_standard$number_transfers_orig_board, useNA = 'ifany')
print('Stats on number_transfers_alight_dest:')
table(survey_standard$number_transfers_alight_dest, useNA = 'ifany')

survey_standard <- survey_standard %>%
  mutate(survey_boardings = 1L +
           as.numeric(number_transfers_orig_board) +
           as.numeric(number_transfers_alight_dest) )

print('Stats on boardings/survey_boardings for debug:')
table(survey_standard$boardings, survey_standard$survey_boardings, useNA = 'ifany')

# Build debug data frame to find odds and ends
# "boardings" is calculated from transfer technologies. "survey_boarding" is calclated from
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
  select(ID, operator, route, direction,
         boardings, survey_boardings,
         first_route_before_survey_board,  first_before_operator,  first_before_technology,
         second_route_before_survey_board, second_before_operator, second_before_technology,
         third_route_before_survey_board,  third_before_operator,  third_before_technology,
         first_route_after_survey_alight,   first_after_operator,  first_after_technology,
         second_route_after_survey_alight, second_after_operator, second_after_technology,
         third_route_after_survey_alight,  third_after_operator,  third_after_technology)
print('Examine interim output "debug_transfers" for records with boardings/survey_boardings mismatch')

survey_standard <- survey_standard %>%
  select(-survey_boardings)



# Step 7:  Standardize Demographics ----------------------------------------------------
print('Configure demographic variables')

# fill n/a in race columns with 0 so that they won't affect race_dmy_sum calculation
survey_standard[c('race_dmy_ind', 'race_dmy_asn', 'race_dmy_blk', 'race_dmy_hwi', 'race_dmy_wht', 'race_dmy_mdl_estn',
                  'race_other_string', 'race_cat')][is.na(survey_standard[c('race_dmy_ind', 'race_dmy_asn', 'race_dmy_blk',
                                                                            'race_dmy_hwi', 'race_dmy_wht', 'race_dmy_mdl_estn',
                                                                            'race_other_string', 'race_cat')])] <- 0

survey_standard <- survey_standard %>%

  # Race
  mutate(race_dmy_ind = as.numeric(race_dmy_ind)) %>%
  mutate(race_dmy_asn = as.numeric(race_dmy_asn)) %>%
  mutate(race_dmy_blk = as.numeric(race_dmy_blk)) %>%
  mutate(race_dmy_hwi = as.numeric(race_dmy_hwi)) %>%
  mutate(race_dmy_wht = as.numeric(race_dmy_wht)) %>%
  mutate(race_dmy_mdl_estn = as.numeric(race_dmy_mdl_estn)) %>%
  mutate(race_dmy_oth = ifelse(str_length(as.character(race_other_string)) > 2, 1L, 0L)) %>%
  mutate(race_categories = ifelse(str_length(as.character(race_cat)) > 2, 1L, 0L)) %>%
  mutate(race_dmy_sum = race_dmy_ind + race_dmy_asn + race_dmy_blk + race_dmy_hwi + race_dmy_wht + race_dmy_mdl_estn + race_dmy_oth + race_categories) %>%

  mutate(race = 'OTHER') %>%
  mutate(race = ifelse(race_dmy_sum == 1 & (race_dmy_asn == 1 | race_cat == 'ASIAN'), 'ASIAN', race)) %>%
  mutate(race = ifelse(race_dmy_sum == 1 & (race_dmy_blk == 1 | race_cat == 'BLACK'), 'BLACK', race)) %>%
  mutate(race = ifelse(race_dmy_sum == 1 & (race_dmy_wht == 1 | race_cat == 'WHITE'), 'WHITE', race)) %>%
  mutate(race = ifelse(race_dmy_sum == 1 & race_dmy_mdl_estn == 1, 'WHITE', race)) %>%
  mutate(race = ifelse(race_dmy_sum == 2 & (
         race_dmy_wht == 1 | race_cat == 'WHITE') & (race_dmy_mdl_estn == 1), 'WHITE', race)) %>%

  # Language at home
  mutate(language_at_home = ifelse(as.character(language_at_home_binary) == 'OTHER',
                                   as.character(language_at_home_detail),
                                   as.character(language_at_home_binary))) %>%
  mutate(language_at_home = ifelse(as.character(language_at_home) == 'other',
                                   as.character(language_at_home_detail_other),
                                   as.character(language_at_home))) %>%
  mutate(language_at_home = toupper(language_at_home)) %>%
  select(-language_at_home_binary,
         -language_at_home_detail,
         -language_at_home_detail_other,
         -race_dmy_ind,
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
  mutate(weekpart = ifelse((is.na(date_string) & operator == "BART"), "WEEKDAY", weekpart))

print('Stats on date_string and time_string for debug:')
table(survey_standard$date_string, useNA = 'ifany')
table(survey_standard$time_string, useNA = 'ifany')

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
table(survey_standard$operator, survey_standard$day_of_the_week, useNA = 'ifany')

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
table(survey_standard$operator,survey_standard$weekpart, useNA = 'ifany')

# Get field dates from date
field_dates <- survey_standard %>%
  group_by(operator, survey_year) %>%
  filter(!is.na(date)) %>%
  summarise(field_start = min(date), field_end = max(date))

survey_standard <- survey_standard %>%
  left_join(field_dates, by = c("operator", "survey_year"))

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
table(survey_standard$operator, survey_standard$time_period, useNA = 'ifany')
table(survey_standard$operator, survey_standard$day_part_temp, useNA = 'ifany')

# the final 'day_part' variable defaults to time_period/strata, and use day_part_temp when time_period is na
survey_standard <- survey_standard %>%
  mutate(day_part = ifelse(is.na(time_period), day_part_temp, time_period))

print('Stats on final day_part variable:')
table(survey_standard$operator, survey_standard$day_part, useNA = 'ifany')

# recode 'depart_time/return_time' of ACE 2019 survey to 'depart_hour/return_hour'
# to be consistent with other surveys
survey_standard <- survey_standard %>%
  mutate(depart_time_stamp = as.POSIXct(strptime(depart_time, format = "%l:%M:%S %p"))) %>%
  mutate(depart_hour_ace = as.numeric(format(depart_time_stamp,"%H"))) %>%
  mutate(depart_hour = ifelse(operator == 'ACE',         # ACE 2019 generates 'depart_hour' from 'depart_time' 
                              depart_hour_ace,
                              depart_hour)) %>%
  mutate(return_time_stamp = as.POSIXct(strptime(return_time, format = "%l:%M:%S %p"))) %>%
  mutate(return_hour_ace = as.numeric(format(return_time_stamp,"%H"))) %>%
  mutate(return_hour = ifelse(operator == 'ACE',         # ACE 2019 generates 'return_hour' from 'return_time' 
                              return_hour_ace,
                              return_hour))

survey_standard <- survey_standard %>%
  select(-date_string, -time_string, -time1, -time2,
         -time3, -time4, -survey_time_posix,
         -depart_time, -return_time,
         -depart_time_stamp, -return_time_stamp,
         -depart_hour_ace, -return_hour_ace)


## Geocode XY to travel model geographies
print('Geocode XY')

# Prepare and write locations that need to be geo-coded to disk
survey_standard <- survey_standard %>%
  mutate(unique_ID = paste(ID, operator, survey_year, sep = "___"))

survey_lat <- survey_standard %>%
  select(unique_ID, dest = dest_lat, home = home_lat, orig = orig_lat,
         school = school_lat, workplace = workplace_lat)

survey_lon <- survey_standard %>%
  select(unique_ID, dest = dest_lon, home = home_lon, orig = orig_lon,
         school = school_lon, workplace = workplace_lon)

survey_lat <- survey_lat %>%
  gather(variable, y_coord, -unique_ID)

survey_lon <- survey_lon %>%
  gather(variable, x_coord, -unique_ID)

survey_coords <- left_join(survey_lat, survey_lon, by = c("unique_ID", "variable"))

# check duplicates
dup_survey_coords <- survey_coords[duplicated(survey_coords),]

survey_coords <- survey_coords %>%                  # remove records with no lat/lon
  mutate(x_coord = as.numeric(x_coord)) %>%
  mutate(y_coord = as.numeric(y_coord)) %>%
  filter(!is.na(x_coord)) %>%
  filter(!is.na(y_coord))

survey_standard <- survey_standard %>%
  mutate(first_board_lat  = ifelse(number_transfers_orig_board == 0,  survey_board_lat,  first_board_lat)) %>%
  mutate(first_board_lon  = ifelse(number_transfers_orig_board == 0,  survey_board_lon,  first_board_lon)) %>%
  mutate(last_alight_lat  = ifelse(number_transfers_alight_dest == 0, survey_alight_lat, last_alight_lat)) %>%
  mutate(last_alight_lon  = ifelse(number_transfers_alight_dest == 0, survey_alight_lon, last_alight_lon))

survey_board <- survey_standard %>%
  select(unique_ID, first_board_lat, first_board_lon, first_board_tech) %>%
  mutate(first_board_lat = as.numeric(first_board_lat)) %>%
  mutate(first_board_lon = as.numeric(first_board_lon)) %>%
  filter(!is.na(first_board_lat)) %>%
  filter(!is.na(first_board_lon))

survey_alight <- survey_standard %>%
  select(unique_ID, last_alight_lat, last_alight_lon, last_alight_tech) %>%
  mutate(last_alight_lat = as.numeric(last_alight_lat)) %>%
  mutate(last_alight_lon = as.numeric(last_alight_lon)) %>%
  filter(!is.na(last_alight_lat)) %>%
  filter(!is.na(last_alight_lon))

remove(survey_lat, survey_lon)

## Geocode Transit Locations

taps_coords <- read.csv(f_taps_coords_path, stringsAsFactors = FALSE) %>%
  rename_all(tolower) %>%
  rowwise %>% mutate(mode=as.list(strsplit(mode_recode,","))) %>%                 # Create a list of tap modes for each row
  select(n, mode, lat, lon = long)

# CRS = 4326 sets the lat/long coordinates in the WGS1984 geographic survey
# CRS = 2230 sets the projection for NAD 1983 California Zone 6 in US Feet
taps_spatial <- st_as_sf(taps_coords, coords = c("lon", "lat"), crs = 4326)
taps_spatial <- st_transform(taps_spatial, crs = 2230)
survey_board_spatial <- st_as_sf(survey_board, coords = c("first_board_lon", "first_board_lat"), crs = 4326)
survey_board_spatial <- st_transform(survey_board_spatial, crs = 2230)
survey_alight_spatial <- st_as_sf(survey_alight, coords = c("last_alight_lon", "last_alight_lat"), crs = 4326)
survey_alight_spatial <- st_transform(survey_alight_spatial, crs = 2230)

survey_board_spatial <- survey_board_spatial %>%
  mutate(board_tap = NA)#,
# distance = NA)
survey_alight_spatial <- survey_alight_spatial %>%
  mutate(alight_tap = NA)#,
# distance = NA)

for (item in unique(unlist(taps_spatial$mode))) {
  temp_tap_spatial <- taps_spatial %>%
    filter(item %in% mode)
  temp_tap_spatial <- temp_tap_spatial %>%
    bind_cols(match = 1:nrow(temp_tap_spatial))

  temp_board <- survey_board_spatial %>%
    filter(first_board_tech == item)
  temp_board <- temp_board %>%
    bind_cols(temp_board_tap = st_nearest_feature(temp_board, temp_tap_spatial))

  # temp_distance <- temp_board %>%
  #   select(temp_board_tap)
  # st_geometry(temp_distance) <- NULL
  # temp_distance <- temp_tap_spatial %>%
  #   right_join(temp_distance, by = c("match" = "temp_board_tap"))
  # temp_distance <- temp_distance %>%
  #   mutate(dist = st_distance(temp_board, temp_distance, by_element = TRUE))
  # st_geometry(temp_distance) <- NULL
  # temp_board <- temp_board %>%
  #   bind_cols(temp_distance %>% select(dist)) %>%
  #   mutate(distance = dist) %>%
  #   select(-dist)

  temp_alight <- survey_alight_spatial %>%
    filter(last_alight_tech == item)
  temp_alight <- temp_alight %>%
    bind_cols(temp_alight_tap = st_nearest_feature(temp_alight, temp_tap_spatial))

  # temp_distance <- temp_alight %>%
  #   select(temp_alight_tap)
  # st_geometry(temp_distance) <- NULL
  # temp_distance <- temp_tap_spatial %>%
  #   right_join(temp_distance, by = c("match" = "temp_alight_tap"))
  # temp_distance <- temp_distance %>%
  #   mutate(dist = st_distance(temp_alight, temp_distance, by_element = TRUE))
  # st_geometry(temp_distance) <- NULL
  # temp_alight <- temp_alight %>%
  #   bind_cols(temp_distance %>% select(dist)) %>%
  #   mutate(distance = dist) %>%
  #   select(-dist)

  st_geometry(temp_tap_spatial) <- NULL

  temp_board <- temp_board %>%
    left_join(as.data.frame(temp_tap_spatial) %>% select(match, n), by = c("temp_board_tap" = "match")) %>%
    select(-temp_board_tap) %>%
    rename(temp_board_tap = n)

  survey_board_spatial <- survey_board_spatial %>%
    left_join(as.data.frame(temp_board) %>% select(unique_ID, temp_board_tap
                                                   # temp_dist = distance
    ), by = "unique_ID") %>%
    mutate(board_tap = ifelse(!is.na(temp_board_tap), temp_board_tap, board_tap)
           # distance = ifelse(!is.na(temp_dist), temp_dist, distance)
    ) %>%
    select(-temp_board_tap)#, -temp_dist)

  temp_alight <- temp_alight %>%
    left_join(as.data.frame(temp_tap_spatial) %>% select(match, n), by = c("temp_alight_tap" = "match")) %>%
    select(-temp_alight_tap) %>%
    rename(temp_alight_tap = n)

  survey_alight_spatial <- survey_alight_spatial %>%
    left_join(as.data.frame(temp_alight) %>% select(unique_ID, temp_alight_tap
                                                    # temp_dist = distance
    ), by = "unique_ID") %>%
    mutate(alight_tap = ifelse(!is.na(temp_alight_tap), temp_alight_tap, alight_tap)
           # distance = ifelse(!is.na(temp_dist), temp_dist, distance)
    ) %>%
    select(-temp_alight_tap)#, -temp_dist)

  rm(temp_tap_spatial, temp_board, temp_alight)
}

board_coords <- as.data.frame(st_coordinates(survey_board_spatial)) %>%
  rename(first_board_lat = Y,
         first_board_lon = X)
survey_board_spatial <- survey_board_spatial %>%
  bind_cols(board_coords) %>%
  select(-first_board_tech)
st_geometry(survey_board_spatial) <- NULL

alight_coords <- as.data.frame(st_coordinates(survey_alight_spatial)) %>%
  rename(last_alight_lat = Y,
         last_alight_lon = X)
survey_alight_spatial <- survey_alight_spatial %>%
  bind_cols(alight_coords) %>%
  select(-last_alight_tech)
st_geometry(survey_alight_spatial) <- NULL

board_alight_tap <- survey_board_spatial %>%
  left_join(survey_alight_spatial, by = c("unique_ID"))

remove(alight_coords, board_coords,
       survey_board_spatial, survey_alight_spatial,
       taps_coords, taps_spatial)


## Geocode Other Locations

survey_coords_spatial <- st_as_sf(survey_coords, coords = c("x_coord", "y_coord"), crs = 4326)
survey_coords_spatial <- st_transform(survey_coords_spatial, crs = 2230)

tm1_taz_shp <- st_read(f_tm1_taz_shp_path, crs = 4326)%>%
  select(tm1_taz = TAZ1454)
tm1_taz_shp <- bind_cols(tm1_taz_shp, match = 1:nrow(tm1_taz_shp))
tm1_taz_shp <- st_transform(tm1_taz_shp, 2230)

tm2_taz_shp <- st_read(f_tm2_taz_shp_path) %>%
  select(tm2_taz = TAZ_ORIGIN)
tm2_taz_shp <- bind_cols(tm2_taz_shp, match = 1:nrow(tm2_taz_shp))
tm2_taz_shp <- st_transform(tm2_taz_shp, 2230)

tm2_maz_shp <- st_read(f_tm2_maz_shp_path) %>%
  select(tm2_maz = MAZ_ORIGIN)
tm2_maz_shp <- bind_cols(tm2_maz_shp, match = 1:nrow(tm2_maz_shp))
tm2_maz_shp <- st_transform(tm2_maz_shp, 2230)

#### Find nearest TM1 TAZ (within 1/4 mile, else NA)
survey_coords_spatial <- survey_coords_spatial %>%
  st_join(tm1_taz_shp, join = st_within)

bad_survey_coords_spatial <- survey_coords_spatial %>%
  filter(is.na(tm1_taz))

bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(taz_index = st_nearest_feature(bad_survey_coords_spatial, tm1_taz_shp))

bad_survey_coords_dist <- tm1_taz_shp %>%
  right_join(data.frame(match = bad_survey_coords_spatial$taz_index), by = "match")  %>%
  rename(bad_taz = tm1_taz)
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(dist = st_distance(bad_survey_coords_spatial, bad_survey_coords_dist, by_element = TRUE))

# If there is no TM1 TAZ within 1/4 mile, the TM1 TAZ is replaced with NA to indicate a failure
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(tm1_taz = bad_survey_coords_dist$bad_taz) %>%
  select(-taz_index) %>%
  mutate(tm1_taz = ifelse(as.numeric(dist) / 5280 <= 0.25, tm1_taz, NA))

# Plot distribution of distance between locations and TAZ where location not in TAZ
# Plot distribution of distance between boarding/alighting locations and TAP
# qplot(as.numeric(bad_survey_coords_spatial$dist),
#       geom = "histogram",
#       main = "Distribution of distance between \ncoordinates and TAZ",
#       xlab = "Distance (m)",
#       binwidth = 10)

st_geometry(bad_survey_coords_spatial) <- NULL
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  select(-match, -dist) %>%
  rename(dist_taz = tm1_taz)

survey_coords_spatial <- survey_coords_spatial %>%
  left_join(bad_survey_coords_spatial, by = c("unique_ID", "variable")) %>%
  mutate(tm1_taz = ifelse(is.na(tm1_taz), dist_taz, tm1_taz)) %>%
  select(-dist_taz, -match)

# check for duplicated unique_ID+variable
# "chk" should have 0 record
chk_tm1_taz = survey_coords_spatial[duplicated(survey_coords_spatial[,1:2]), ]


#### Find nearest TM2 TAZ (within 1/4 mile, else NA)
survey_coords_spatial <- survey_coords_spatial %>%
  st_join(tm2_taz_shp, join = st_within)

bad_survey_coords_spatial <- survey_coords_spatial %>%
  filter(is.na(tm2_taz)) %>%
  select(-tm1_taz)

bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(taz_index = st_nearest_feature(bad_survey_coords_spatial, tm2_taz_shp))

bad_survey_coords_dist <- tm2_taz_shp %>%
  right_join(data.frame(match = bad_survey_coords_spatial$taz_index), by = "match")  %>%
  rename(bad_taz = tm2_taz)
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(dist = st_distance(bad_survey_coords_spatial, bad_survey_coords_dist, by_element = TRUE))

# If there is no TM2 TAZ within 1/4 mile, the TM2 TAZ is replaced with NA to indicate a failure
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(tm2_taz = bad_survey_coords_dist$bad_taz) %>%
  select(-taz_index) %>%
  mutate(tm2_taz = ifelse(as.numeric(dist) / 5280 <= 0.25, tm2_taz, NA))

# Plot distribution of distance between locations and TAZ where location not in TAZ
# Plot distribution of distance between boarding/alighting locations and TAP
# qplot(as.numeric(bad_survey_coords_spatial$dist),
#       geom = "histogram",
#       main = "Distribution of distance between \ncoordinates and TAZ",
#       xlab = "Distance (m)",
#       binwidth = 10)

st_geometry(bad_survey_coords_spatial) <- NULL
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  select(-match, -dist) %>%
  rename(dist_taz = tm2_taz)

survey_coords_spatial <- survey_coords_spatial %>%
  left_join(bad_survey_coords_spatial, by = c("unique_ID", "variable")) %>%
  mutate(tm2_taz = ifelse(is.na(tm2_taz), dist_taz, tm2_taz)) %>%
  select(-dist_taz, -match)

# check for duplicated unique_ID+variable
# "chk" should have 0 record
chk_tm2_taz = survey_coords_spatial[duplicated(survey_coords_spatial[,1:2]), ]

# (April 6, 2021) The following lat/lon points are located at or too close to the boundary of two TM2 TAZs,
# therefore were joined to two TM2 TAZs
#     40113___BART___2015, dest_lat/lon	38.0815140, -122.2400470
#     40113___BART___2015, home_lat/lon	38.0815140, -122.2400470
#     1204___Caltrain___2014, home_lat/lon	37.761126, -122.399303
#     1204___Caltrain___2014, orig_lat/lon	37.761126, -122.399303
#     23645___SF Muni___2017, home_lat/lon  38.081514, -122.240047

# Temporarily manually drop the duplicates
survey_coords_spatial <- survey_coords_spatial[!duplicated(survey_coords_spatial[,1:2]), ]

#### Find nearest MAZ within 1/4 mile (else NA)
survey_coords_spatial <- survey_coords_spatial %>%
  st_join(tm2_maz_shp, join = st_within)

bad_survey_coords_spatial <- survey_coords_spatial %>%
  filter(is.na(tm2_maz)) %>%
  select(-tm1_taz, -tm2_taz)

bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(maz_index = st_nearest_feature(bad_survey_coords_spatial, tm2_maz_shp))

bad_survey_coords_dist <- tm2_maz_shp %>%
  right_join(data.frame(match = bad_survey_coords_spatial$maz_index), by = "match")  %>%
  rename(bad_maz = tm2_maz)
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(dist = st_distance(bad_survey_coords_spatial, bad_survey_coords_dist, by_element = TRUE))

# If there is no maz within 1/4 mile, the maz is replaced with NA to indicate a failure
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(tm2_maz = bad_survey_coords_dist$bad_maz) %>%
  select(-maz_index) %>%
  mutate(tm2_maz = ifelse(as.numeric(dist) / 5280 <= 0.25, tm2_maz, NA))

# Plot distribution of distance between locations and maz where location not in maz
# Plot distribution of distance between boarding/alighting locations and TAP
# qplot(as.numeric(bad_survey_coords_spatial$dist),
#       geom = "histogram",
#       main = "Distribution of distance between \ncoordinates and maz",
#       xlab = "Distance (m)",
#       binwidth = 10)

st_geometry(bad_survey_coords_spatial) <- NULL
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  select(-match, -dist) %>%
  rename(dist_maz = tm2_maz)

survey_coords_spatial <- survey_coords_spatial %>%
  left_join(bad_survey_coords_spatial, by = c("unique_ID", "variable")) %>%
  mutate(tm2_maz = ifelse(is.na(tm2_maz), dist_maz, tm2_maz)) %>%
  select(-dist_maz, -match)

# check for duplicated unique_ID+variable
# "chk" should have 0 record
chk_tm2_maz = survey_coords_spatial[duplicated(survey_coords_spatial[,1:2]), ]

# (April 6, 2021) The following lat/lon points are located at the boundary of two TM2 TAZs,
# therefore were joined to two TM2 MAZs
#     1204___Caltrain___2014, home_lat/lon 	 37.761126, -122.399303
#     1204___Caltrain___2014, orig_lat/lon   37.761126, -122.399303
#     31___Napa Vine___2019, home_lat/lon    38.161992,	-122.260128
#     31___Napa Vine___2019, dest_lat/lon    38.161992,	-122.260128
#     1226___Napa Vine___2014, orig_lat/lon  37.72195, -122.478136
# Temporarily manually drop the duplicates
survey_coords_spatial <- survey_coords_spatial[!duplicated(survey_coords_spatial[,1:2]), ]

# Bring in the geocoding results
st_geometry(survey_coords_spatial) <- NULL

survey_coords_spatial_tm1_taz <- survey_coords_spatial %>%
  select(-tm2_taz, -tm2_maz) %>%
  spread(variable, tm1_taz) %>%
  rename(dest_tm1_taz = dest, home_tm1_taz = home, orig_tm1_taz = orig,
         school_tm1_taz = school, workplace_tm1_taz = workplace)

survey_coords_spatial_tm2_taz <- survey_coords_spatial %>%
  select(-tm1_taz, -tm2_maz) %>%
  spread(variable, tm2_taz) %>%
  rename(dest_tm2_taz = dest, home_tm2_taz = home, orig_tm2_taz = orig,
         school_tm2_taz = school, workplace_tm2_taz = workplace)

survey_coords_spatial_tm2_maz <- survey_coords_spatial %>%
  select(-tm1_taz, -tm2_taz) %>%
  spread(variable, tm2_maz) %>%
  rename(dest_tm2_maz = dest, home_tm2_maz = home, orig_tm2_maz = orig,
         school_tm2_maz = school, workplace_tm2_maz = workplace)

board_alight_tap <- board_alight_tap %>%
  select(unique_ID, board_tap, alight_tap)

# Joins
survey_standard <- survey_standard %>%
  left_join(survey_coords_spatial_tm1_taz, by = c("unique_ID")) %>%
  left_join(survey_coords_spatial_tm2_taz, by = c("unique_ID")) %>%
  left_join(survey_coords_spatial_tm2_maz, by = c("unique_ID")) %>%
  left_join(board_alight_tap, by = c("unique_ID"))

remove(board_alight_tap,
       survey_coords,
       survey_coords_spatial,
       survey_coords_spatial_tm1_taz,
       survey_coords_spatial_tm2_taz,
       survey_coords_spatial_tm2_maz)


### Clean up data types
print('Final cleanup')

# Cast all factors to numeric or string
survey_standard <- survey_standard %>%
  mutate_at(vars(contains("operator")), as.character) %>%
  mutate_at(vars(contains("hour")), as.numeric) %>%
  mutate(survey_board_lat       = as.numeric(survey_board_lat)) %>%
  mutate(survey_board_lon       = as.numeric(survey_board_lon)) %>%
  mutate(survey_alight_lat      = as.numeric(survey_alight_lat)) %>%
  mutate(survey_alight_lon      = as.numeric(survey_alight_lon)) %>%
  mutate(first_board_lat        = as.numeric(first_board_lat)) %>%
  mutate(first_board_lon        = as.numeric(first_board_lon)) %>%
  mutate(last_alight_lat        = as.numeric(last_alight_lat)) %>%
  mutate(last_alight_lon        = as.numeric(last_alight_lon)) %>%
  mutate(weight                 = as.numeric(weight))

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
         operator,
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
  select(-at_school_after_dest_purp,
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
         -third_route_after_survey_alight)


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
