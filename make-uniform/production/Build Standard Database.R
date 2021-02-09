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
           "C:/Users/ywang/Documents/R/OnboardSurvey_2020Oct_yq/Data and Reports/",
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
f_taps_coords_path <- paste0(dir_path, "_geocoding Standardized/TAPs/taps_lat_long.csv")
f_taz_shp_path <- paste0(dir_path, "_geocoding Standardized/TM2_Zones/tazs.shp")
f_maz_shp_path <- paste0(dir_path, "_geocoding Standardized/TM2_Zones/mazs.shp")
f_geocode_column_names_path <- "bespoke_survey_station_column_names.csv"
f_canonical_routes_path <- "canonical_route_crosswalk.csv"

f_actransit_survey_path <- paste0(dir_path,
                                  "AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode) NO POUND OR SINGLE QUOTE.csv")
f_bart_survey_path <- paste0(dir_path,
                             "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv")
f_caltrain_survey_path <- paste0(dir_path,
                                 "Caltrain/As CSV/Caltrain_Final_Submitted_1_5_2015_TYPE_WEIGHT_DATE NO POUND OR SINGLE QUOTE.csv")
f_marin_survey_path <- paste0(dir_path,
                              "Marin Transit/Final Data/marin transit_data file_final01222021_NO POUND OR SINGLE QUOTE.csv")
f_muni_survey_path <- paste0(dir_path,
                             "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")
# f_napa_survey_path <- paste0(dir_path,
#                              "Napa Vine/As CSV/Napa Vine Transit OD Survey Data_Dec10_Submitted_toAOK_with_transforms NO POUND OR SINGLE QUOTE.csv")
# f_vta_survey_path <- paste0(dir_path,
#                             "VTA/As CSV/VTA_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")
f_fast_survey_path <- paste0(dir_path,
                            "Solano County/As CSV/FAST_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_rvdb_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/Rio Vista Delta Breeze_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_vcc_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/Vacaville City Coach_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_soltrans_survey_path <- paste0(dir_path,
                             "Solano County/As CSV/SolTrans_removeTypos_add_route_time_NO POUND OR SINGLE QUOTE.csv")
f_ace_survey_path <- paste0(dir_path,
                             "ACE/2019/ACE19_Final Data Add New Route Date Time Columns NO POUND OR SINGLE QUOTE.csv")
f_unioncity_survey_path <- paste0(dir_path,
                                  "Union City/2017/Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv")
f_sonomact_survey_path <- paste0(dir_path,
                                 "Sonoma County/2018/As CSV/sc transit_data file_final_spring 2018_addRoutesCols NO POUND NO SINGLE QUOTE.csv")
f_smart_survey_path <- paste0(dir_path,
                              "SMART/As CSV/SMART Standardized Final Data_addRouteCols_NO POUND NO SINGLE QUOTE.csv")
f_weta_survey_path <- paste0(dir_path,
                             "WETA/WETA 2018/WETA-Final Weighted Data-Standardized_addCols_NO POUND OR SINGLE QUOTE.csv")
f_westcat_survey_path <- paste0(dir_path,
                                "WestCAT/As CSV/WestCAT_addCols_NO POUND OR SINGLE QUOTE.csv")
f_lavta_survey_path <- paste0(dir_path,
                              "LAVTA/2018/OD_20181207_LAVTA_Submittal_FINAL_addCols_NO POUND OR SINGLE QUOTE.csv")
f_tridelta2019_survey_path <- paste0(dir_path,
                                     "Tri Delta/2019/TriDelta_ODSurvey_Dataset_Weights_03272019_FinalDeliv_addCols_NO POUND OR SINGLE QUOTE.csv")
f_cccta2019_survey_path <- paste0(dir_path,
                                  "County Connection/2019/OD_20191105_CCCTA_Submittal_FINAL Expanded_addCols_NO POUND OR SINGLE QUOTE.csv")
f_ggtransit_survey_path <- paste0(dir_path,
                                  "Golden Gate Transit/2018/As CSV/20180907_OD_GoldenGate_allDays_addCols_NO POUND OR SINGLE QUOTE.csv")
f_napavine2019_survey_path <- paste0(dir_path,
                                     "Napa Vine/2019/Napa Vine_FINAL Data_addCols_NO POUND OR SINGLE QUOTE.csv")
f_petaluma2018_survey_path <- paste0(dir_path,
                                     "Petaluma/2018/As CSV/20180530_OD_Petaluma_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv")
f_SantaRosaCityBus2018_survey_path <- paste0(dir_path,
                                             "Santa Rosa CityBus/2018/As CSV/20180522_OD_SantaRosa_Submittal_addCols_FINAL NO POUND NO SINGLE QUOTE.csv")

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


# _User Intervention_
# When adding a new operator, the user must update the dictionary files that translate
# the usually-bespoke survey coding to the standard coding. Edits to the dictionary should be made in
# the file `Dictionary for Standard Database.csv`. The existing entries in the
# dictionary *should* explicate the expected task.

## Prepare dictionaries
dictionary_all <- read.csv(f_dict_standard,
                           header = TRUE) %>%
  rename_all(tolower)

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

#napa_vine_df <- read_operator('Napa Vine',
#                              2014,
#                              'local bus',
#                              f_napa_survey_path,
#                              dictionary_all,
#                              canonical_station_shp)

#vta_df <- read_operator('VTA',
#                        2017,
#                        'local bus',
#                        f_vta_survey_path,
#                        dictionary_all,
#                        canonical_station_shp)

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

survey_combine <- bind_rows(
  ac_transit_df,
  bart_df,
  caltrain_df,
  muni_df,
  marin_df,
  # # napa_vine_df,
  # # vta_df,
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
  SantaRosaCityBus2018_df
)

dup1 <- survey_combine[duplicated(survey_combine),]

# remove(
#        ac_transit_df,
#        bart_df,
#        caltrain_df,
#        muni_df,
#        napa_vine_df,
#        vta_df
#       )


## Flatten

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
  left_join(canonical_routes_crosswalk %>% select(-technology), by = c("operator" = "survey", "survey_year", "survey_response" = "survey_name")) %>%
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

dup2 <- survey_flat[duplicated(survey_flat),]

#remove(survey_cat,
#       survey_non)


## Update survey technology

# _User Intervention_
# As noted above, when the operator data is read in, it assumes every route in the survey uses
# the same technology (e.g., all Muni routes are local bus). In face, some operators operate
# multiple technologies. These bespoke technologies are added here. These changes are recorded
# in the `canonical route name database` and must be updated manually.

survey_flat <- survey_flat %>%
  left_join(canonical_routes_crosswalk %>% select(-survey_name) %>% unique(),
            by = c("operator" = "survey", "route" = "canonical_name", "survey_year"))

dup3 <- survey_flat[duplicated(survey_flat),]


# _User Intervention_
# User should run each of the `Steps` below individually and make sure the results make sense.
# In addition, the `debug_transfers` dataframe should be empty. If it's not, the code
# has failed to identify an operator or technology for a route that is being
# transferred to or from.

## Build standard variables

# Step 1:  Age-related transformations ----

# Standardize year born
survey_standard <- survey_flat %>%
  mutate(year_born = ifelse(
    str_detect(year_born_four_digit,"Missing") | str_detect(year_born_four_digit,"Not Provided") | str_detect(year_born_four_digit,'REFUSED'),
    NA,
    year_born_four_digit)) %>%
  mutate(year_born = ifelse(is.na(year_born), NA, as.numeric(year_born))) %>%
  select(-year_born_four_digit)

dup4 <- survey_standard[duplicated(survey_standard),]

# Manual fixes to year born
survey_standard <- survey_standard %>%
  mutate(survey_year = as.numeric(survey_year)) %>%
  mutate(year_born = ifelse(year_born == 1900, 2000, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1901, 2001, year_born)) %>%
  mutate(year_born = ifelse(year_born == 3884, 1984, year_born)) %>%
  mutate(year_born = ifelse(year_born == 1899, NA, year_born))

table(survey_standard$year_born)

# Compute approximate respondent age
survey_standard <- survey_standard %>%
  mutate(approximate_age = ifelse(!is.na(year_born) & survey_year >= year_born, survey_year - year_born, NA)) %>%
  mutate(approximate_age = ifelse(approximate_age == 0, NA, approximate_age))

table(survey_standard$approximate_age)


# Step 2:  Trip- and tour-purpose-related transformations ------------------------------

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

dup5 <- survey_standard[duplicated(survey_standard),]

# Refine school purpose
survey_standard <- survey_standard %>%
  mutate(orig_purp = ifelse(orig_purp == "school", "high school", orig_purp)) %>%
  mutate(orig_purp = ifelse(orig_purp == "school" & approximate_age < 14,
                            "grade_school", orig_purp)) %>%
  mutate(dest_purp = ifelse(dest_purp == "school", "high school", dest_purp)) %>%
  mutate(dest_purp = ifelse(dest_purp == "school" & approximate_age < 14,
                            "grade_school", dest_purp))


# (Approximate) Tour purpose
survey_standard <- survey_standard %>%
  mutate(tour_purp = 'missing') %>%

  # workers -- simple
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              orig_purp == 'home' &
                              dest_purp == 'work',
                            'work',
                            tour_purp)) %>%

  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              orig_purp == 'work' &
                              dest_purp == 'home',
                            'work',
                            tour_purp)) %>%

  # students -- simple
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              (orig_purp == 'grade school' | dest_purp == 'grade school'),
                            'grade school',
                            tour_purp)) %>%
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              (orig_purp == 'high school' | dest_purp == 'high school'),
                            'high school', tour_purp)) %>%

  # non-working university students
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              (orig_purp == 'college'  | dest_purp == 'college'),
                            'university',
                            tour_purp) ) %>%

  # non-workers, non-students, home-based travel
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              student_status == 'non-student' &
                              orig_purp == 'home',
                            dest_purp, tour_purp)) %>%

  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              student_status == 'non-student' &
                              dest_purp == 'home',
                            orig_purp,
                            tour_purp)) %>%

  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              student_status == 'non-student' &
                              orig_purp == dest_purp,
                            orig_purp,
                            tour_purp)) %>%

  # non-workers, non-students, non-home-based (which we know from above implementation) escorting travel
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              student_status == 'non-student' &
                              (orig_purp == 'escorting' | dest_purp == 'escorting'),
                            'escorting',
                            tour_purp)) %>%

  # university is present, but work is not, then university
  mutate(tour_purp = ifelse(tour_purp == 'missing'
                            & at_work_prior_to_orig_purp == 'not at work before surveyed trip'
                            & at_work_after_dest_purp == 'not at work after surveyed trip' &
                              (orig_purp == 'college' | dest_purp == 'college'),
                            'university',
                            tour_purp)) %>%

  # if work before trip and home after, assume work tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              at_work_prior_to_orig_purp == 'at work before surveyed trip' &
                              dest_purp == 'home',
                            'work',
                            tour_purp)) %>%

  # if work after trip and home before, assume work tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              at_work_after_dest_purp == 'at work after surveyed trip' &
                              orig_purp == 'home',
                            'work',
                            tour_purp)) %>%

  # if non-worker, school before trip and home after, over 18, university tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              at_school_prior_to_orig_purp == 'at school before surveyed trip' &
                              approximate_age > 18 &
                              dest_purp == 'home',
                            'university',
                            tour_purp)) %>%

  # if non-worker, school after trip and home before, over 18, university tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              at_school_after_dest_purp == 'at school after surveyed trip' &
                              approximate_age > 18 &
                              orig_purp == 'home',
                            'university',
                            tour_purp)) %>%

  # if non-worker, school before trip and home after, 14 to 18, high school tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              at_school_prior_to_orig_purp == 'at school before surveyed trip' &
                              approximate_age <= 18 &
                              approximate_age >= 14 &
                              dest_purp == 'home',
                            'high school',
                            tour_purp)) %>%

  # if non-worker, school after trip and home before, 14 to 18, high school tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              work_status == 'non-worker' &
                              at_school_after_dest_purp == 'at school after surveyed trip' &
                              approximate_age <= 18 &
                              approximate_age >= 14 &
                              orig_purp == 'home',
                            'high school',
                            tour_purp)) %>%

  # if no work before or after, but work is a leg, assume a work tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              at_work_prior_to_orig_purp == 'not at work before surveyed trip' &
                              at_work_after_dest_purp == 'not at work after surveyed trip' &
                              (orig_purp == 'work' | dest_purp == 'work'),
                            'work',
                            tour_purp)) %>%

  # at work tours
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              at_work_prior_to_orig_purp == 'at work before surveyed trip' &
                              dest_purp == 'work',
                            'at work',
                            tour_purp) ) %>%

  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              at_work_after_dest_purp == 'at work after surveyed trip' &
                              orig_purp == 'work',
                            'at work',
                            tour_purp)) %>%

  # if still left and work before or after the trip, assume work tour
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              (at_work_after_dest_purp == 'at work after surveyed trip' |
                               at_work_prior_to_orig_purp == 'at work before surveyed trip'),
                            'work',
                            tour_purp)) %>%

  # if still left and home is one end, chose the other as the purpose
  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              orig_purp == 'home',
                            dest_purp,
                            tour_purp)) %>%

  mutate(tour_purp = ifelse(tour_purp == 'missing' &
                              dest_purp == 'home',
                            orig_purp,
                            tour_purp)) %>%

  # if still left, pick the orig_purp
  mutate(tour_purp = ifelse(tour_purp == 'missing', orig_purp, tour_purp)) %>%

  # finally, if work-related, categorize as 'other maintenance'
  mutate(tour_purp = ifelse(tour_purp == 'work-related', 'other maintenance', tour_purp))


table(survey_standard$tour_purp)

dup6 <- survey_standard[duplicated(survey_standard),]


# Step 3:  Update Key locations and Status Flags --------------------------------------

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

table(survey_standard$work_status)
table(survey_standard$student_status)


# Step 4:  Automobile Sufficiency ------------------------------------------------------

# Transform vehicles and workers to standard scale
survey_standard <- survey_standard %>%
  mutate(vehicles = ifelse((vehicles == 'other' & 'vehicles_additional_info' %in% colnames(survey_standard)),
                              vehicles_additional_info, vehicles)) %>%
  mutate(vehicles = ifelse((vehicles == 'other' & 'vehicles_other' %in% colnames(survey_standard)),
                              vehicles_other, vehicles)) %>%
  mutate(workers = ifelse((workers == 'other' & 'workers_additional_info' %in% colnames(survey_standard)),
                              workers_additional_info,  workers)) %>%
  mutate(workers = ifelse((workers == 'other' & 'workers_other' %in% colnames(survey_standard)),
                          workers_other,  workers))

table(survey_standard$vehicles)
table(survey_standard$workers)

vehicles_dictionary <- data.frame(
  vehicles = c('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
               'eight', 'nine', 'ten', 'eleven', 'twelve', 'four or more',
               '5', '6', '7', '8', '9', '10'),
  vehicle_numeric_cat = c(0, 1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                          4, 4, 4, 4, 4, 4))

workers_dictionary <- data.frame(
  workers = c('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
              'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen',
              'fifteen', 'six or more',
              '7', '8', '9', '10', '11'),
  worker_numeric_cat = c(0, 1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                         4, 4, 4, 4, 4))

survey_standard <- left_join(survey_standard, vehicles_dictionary, by = c("vehicles"))
survey_standard <- left_join(survey_standard, workers_dictionary, by = c("workers"))
# some surveys have numeric values in 'vehicles_other' and 'workers_other' fields that cannot join with the
# count dictionary, therefore fill in the na using the raw numeric value
survey_standard <- survey_standard %>%
  mutate(vehicle_numeric_cat = ifelse(is.na(vehicle_numeric_cat), vehicles, vehicle_numeric_cat)) %>%
  mutate(worker_numeric_cat = ifelse(is.na(worker_numeric_cat), workers, worker_numeric_cat))

table(survey_standard$vehicle_numeric_cat)
table(survey_standard$worker_numeric_cat)

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
                               vehicle_numeric_cat == 'Ref') | (
                               worker_numeric_cat == 'Ref') | (
                               vehicle_numeric_cat == "DON'T KNOW") | (
                               worker_numeric_cat == "DON'T KNOW"),
                            'missing',
                            auto_suff))

table(survey_standard$auto_suff)

remove(vehicles_dictionary,
       workers_dictionary)

dup7 <- survey_standard[duplicated(survey_standard),]


# Step 5:  Operator and Technology sequence --------------------------------------------

# Set operator for each of six legs (three before, three after)
# - remove Dummy Records
survey_standard <- survey_standard %>%
  mutate(first_route_before_survey_board = ifelse(str_detect(first_route_before_survey_board, "Missing___"),
                                                  "",
                                                  first_route_before_survey_board)) %>%
  mutate(second_route_before_survey_board = ifelse(str_detect(second_route_before_survey_board, "Missing___"),
                                                   "",
                                                   second_route_before_survey_board)) %>%
  mutate(third_route_before_survey_board = ifelse(str_detect(third_route_before_survey_board, "Missing___"),
                                                  "",
                                                  third_route_before_survey_board))

survey_standard <- survey_standard %>%
  mutate(first_route_after_survey_alight = ifelse(str_detect(first_route_after_survey_alight, "Missing___"),
                                                  "",
                                                  first_route_after_survey_alight)) %>%
  mutate(second_route_after_survey_alight = ifelse(str_detect(second_route_after_survey_alight, "Missing___"),
                                                   "",
                                                   second_route_after_survey_alight)) %>%
  mutate(third_route_after_survey_alight = ifelse(str_detect(third_route_after_survey_alight, "Missing___"),
                                                  "",
                                                  third_route_after_survey_alight))

survey_standard <- survey_standard %>%
  mutate(first_before_operator  = str_extract(first_route_before_survey_board,  "^[A-z -]+?(?=_)"),
         second_before_operator = str_extract(second_route_before_survey_board, "^[A-z -]+?(?=_)"),
         third_before_operator  = str_extract(third_route_before_survey_board,  "^[A-z -]+?(?=_)"),
         first_after_operator   = str_extract(first_route_after_survey_alight,  "^[A-z -]+?(?=_)"),
         second_after_operator  = str_extract(second_route_after_survey_alight, "^[A-z -]+?(?=_)"),
         third_after_operator   = str_extract(third_route_after_survey_alight,  "^[A-z -]+?(?=_)"))

dup8 <- survey_standard[duplicated(survey_standard),]

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
  filter(survey != "GEOCODE")

#remove(tech_crosswalk_expansion_df, tech_crosswalk_expansion_list)

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

dup9 <- survey_standard[duplicated(survey_standard),]

table(survey_standard$first_before_technology)  #### check if there is "Missing"


# Step 6:  Travel Model One path details -----------------------------------------------

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

table(survey_standard$transfer_from)
table(survey_standard$transfer_to)
table(survey_standard$survey_tech)
table(survey_standard$first_board_tech)
table(survey_standard$last_alight_tech)


# Travel Model One path (re-write to acknowledge NA explicitly)
# -- Access
survey_standard <- survey_standard %>%
  mutate(path_access = "X") %>%
  mutate(path_access = ifelse(access_mode == "walk", "W", path_access)) %>%
  mutate(path_access = ifelse(access_mode == "pnr" , "D", path_access)) %>%
  mutate(path_access = ifelse(access_mode == "knr" , "D", path_access)) %>%
  mutate(path_access = ifelse(access_mode == "bike", "B", path_access)) %>%
  mutate(path_access = ifelse(access_mode == "tnc",  "T", path_access)) %>%
  mutate(path_access = ifelse(is.na(access_mode), "X", path_access)) %>%
  # consider "bike" as "D"
  mutate(path_access_recode = ifelse(path_access == "B", "D", path_access)) %>%
  # consider "TNC" as "D"
  mutate(path_access_recode = ifelse(path_access_recode == "T", "D", path_access_recode))

table(survey_standard$path_access)
table(survey_standard$path_access_recode)

# -- Egress
survey_standard <- survey_standard %>%
  mutate(path_egress = "X") %>%
  mutate(path_egress = ifelse(egress_mode == "walk", "W", path_egress)) %>%
  mutate(path_egress = ifelse(egress_mode == "pnr" , "D", path_egress)) %>%
  mutate(path_egress = ifelse(egress_mode == "knr" , "D", path_egress)) %>%
  mutate(path_egress = ifelse(egress_mode == "bike", "B", path_egress)) %>%
  mutate(path_egress = ifelse(egress_mode == "tnc", "T", path_egress)) %>%
  mutate(path_egress = ifelse(is.na(egress_mode), "X", path_egress)) %>%
  # consider "bike" as "D"
  mutate(path_egress_recode = ifelse(path_egress == "B", "D", path_egress)) %>%
  # consider "TNC" as "D"
  mutate(path_egress_recode = ifelse(path_egress_recode == "T", "D", path_egress_recode))

table(survey_standard$path_egress)
table(survey_standard$path_egress_recode)

# -- Line haul
# --- Technology present calculations
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

helper_relevant_tech <- 'commuter rail'
survey_standard <- survey_standard %>%
  mutate(commuter_rail_present = FALSE) %>%
  mutate(commuter_rail_present = first_before_technology == helper_relevant_tech |
           second_before_technology == helper_relevant_tech |
           third_before_technology == helper_relevant_tech |
           first_after_technology == helper_relevant_tech |
           second_after_technology == helper_relevant_tech |
           third_after_technology == helper_relevant_tech)

dup10<- survey_standard[duplicated(survey_standard),]

table(survey_standard$commuter_rail_present)

helper_relevant_tech <- 'heavy rail'
survey_standard <- survey_standard %>%
  mutate(heavy_rail_present = FALSE) %>%
  mutate(heavy_rail_present = first_before_technology == helper_relevant_tech |
           second_before_technology == helper_relevant_tech |
           third_before_technology == helper_relevant_tech |
           first_after_technology == helper_relevant_tech |
           second_after_technology == helper_relevant_tech |
           third_after_technology == helper_relevant_tech)

table(survey_standard$heavy_rail_present)

helper_relevant_tech <- 'express bus'
survey_standard <- survey_standard %>%
  mutate(express_bus_present = FALSE) %>%
  mutate(express_bus_present = first_before_technology == helper_relevant_tech |
           second_before_technology == helper_relevant_tech |
           third_before_technology == helper_relevant_tech |
           first_after_technology == helper_relevant_tech |
           second_after_technology == helper_relevant_tech |
           third_after_technology == helper_relevant_tech)

table(survey_standard$express_bus_present)

helper_relevant_tech <- 'ferry'
survey_standard <- survey_standard %>%
  mutate(ferry_present = FALSE) %>%
  mutate(ferry_present = first_before_technology == helper_relevant_tech |
           second_before_technology == helper_relevant_tech |
           third_before_technology == helper_relevant_tech |
           first_after_technology == helper_relevant_tech |
           second_after_technology == helper_relevant_tech |
           third_after_technology == helper_relevant_tech)

table(survey_standard$ferry_present)

helper_relevant_tech <- 'light rail'
survey_standard <- survey_standard %>%
  mutate(light_rail_present = FALSE) %>%
  mutate(light_rail_present = first_before_technology == helper_relevant_tech |
           second_before_technology == helper_relevant_tech |
           third_before_technology == helper_relevant_tech |
           first_after_technology == helper_relevant_tech |
           second_after_technology == helper_relevant_tech |
           third_after_technology == helper_relevant_tech)

table(survey_standard$light_rail_present)

survey_standard <- survey_standard %>%
  mutate(path_line_haul = "LOC") %>%
  mutate(path_line_haul = ifelse(light_rail_present,
                                 "LRF",
                                 path_line_haul)) %>%
  mutate(path_line_haul = ifelse(ferry_present,
                                 "LRF",
                                 path_line_haul)) %>%
  mutate(path_line_haul = ifelse(express_bus_present,
                                 "EXP",
                                 path_line_haul)) %>%
  mutate(path_line_haul = ifelse(heavy_rail_present,
                                 "HVY",
                                 path_line_haul)) %>%
  mutate(path_line_haul = ifelse(commuter_rail_present, "COM", path_line_haul)) %>%
  mutate(path_label = paste(path_access, path_line_haul, path_egress, sep = "-")) %>%
  mutate(path_label_recode = paste(path_access_recode, path_line_haul, path_egress_recode, sep = "-"))

table(survey_standard$path_label)
table(survey_standard$path_label_recode)

dup11 <- survey_standard[duplicated(survey_standard),]

# Boardings
survey_standard <- survey_standard %>%
  mutate(boardings = 1L) %>%
  mutate(boardings = ifelse(!(first_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_before_technology == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_before_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(first_after_technology   == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(second_after_technology  == "Missing"), boardings + 1, boardings)) %>%
  mutate(boardings = ifelse(!(third_after_technology   == "Missing"), boardings + 1, boardings))

table(survey_standard$boardings)

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

table(survey_standard$number_transfers_orig_board)
table(survey_standard$number_transfers_alight_dest)

survey_standard <- survey_standard %>%
  mutate(survey_boardings = 1L +
           as.numeric(number_transfers_orig_board) +
           as.numeric(number_transfers_alight_dest) )

table(survey_standard$boardings, survey_standard$survey_boardings)

# Build debug data frame to find odds and ends
# Ideally, debug_transfers has 0 record. When it's not empty, examine if the transfer routes, transfer operator,
# and transfer technology are coded correctly.
# One caveat (Nov 10, 2020): the calculation of "survey_boarding" is based on "number_transfers_orig_board" and "number_transfers_alight_dest";
# Muni survey tracks 4 transfers before and after the surveyed route, therefore "number_transfers_orig_board"/"number_transfers_alight_dest"
# maxes at 4, but this script only tracks 3 transfers before and after, so the sum of transfers before or after maxes at 3, causing inconsistency
# between boardings and survey_boardings. Currently there are only two such records and they are captured in debug_transfer (ID 25955, 31474).

# Another situation where debug_transfers contains records: the survey data comes with "number_transfers_alight_dest" and	"number_transfers_orig_board"
# columns, but one or more of the transfers are routes that are "Missing" operator, e.g. unspecified private shuttle. In this case, "survey_boarding"
# is larger than "boardings". This occurs in WestCAT 2017 survey (ID 181, 229, 304, 391, 709), Solano County 2017 Survey (FAST ID 1071, 1576;
# Soltrans ID 1107, 1453).

# Napa Vine 2019 has cases where the "number_transfers_orig_board" or "number_transfers_alight_dest" values in the raw data were wrong, resulting in
# inconsistency between 'boardings' and 'survey_boardings': ID 14, 77, 306, 9142, 9216.


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

survey_standard <- survey_standard %>%
  select(-survey_boardings, -commuter_rail_present, -heavy_rail_present,
         -ferry_present, -light_rail_present, -express_bus_present)



# Step 7:  Standardize Demographics ----------------------------------------------------

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

  # Language at home
  mutate(language_at_home = ifelse(as.character(language_at_home_binary) == 'OTHER',
                                   as.character(language_at_home_detail),
                                   as.character(language_at_home_binary))) %>%
  mutate(language_at_home = toupper(language_at_home)) %>%
  select(-language_at_home_binary,
         -language_at_home_detail,
         -race_dmy_ind,
         -race_dmy_asn,
         -race_dmy_blk,
         -race_dmy_hwi,
         -race_dmy_wht,
         -race_dmy_oth,
         -race_dmy_sum,
         -race_other_string)

# check if all records have the "race" variable filled, race_chk should be empty
race_chk <- survey_standard[which(is.na(survey_standard$race)),]

# Update fare medium for surveys with clipper detail
survey_standard <- survey_standard %>%
  mutate(fare_medium = ifelse(is.na(clipper_detail), fare_medium, clipper_detail)) %>%
  select(-clipper_detail)

# consolidate missing household income into 'missing' 
survey_standard <- survey_standard %>%
  mutate(household_income = ifelse(household_income == "DON'T KNOW", "Missing", household_income))

table(survey_standard$work_status)
table(survey_standard$student_status)
table(survey_standard$fare_medium)
table(survey_standard$fare_category)
table(survey_standard$hispanic)
table(survey_standard$race)
table(survey_standard$language_at_home)
table(survey_standard$household_income)
table(survey_standard$eng_proficient)


# Step 8:  Set dates and times ---------------------------------------------------------

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


table(survey_standard$date_string)
table(survey_standard$time_string)

# Get day of the week from date
survey_standard <- survey_standard %>%
  mutate(date1 = as.Date(date_string, format = "%m/%d/%Y")) %>%
  mutate(date2 = as.Date(date_string, format = "%Y-%m-%d")) %>%
  mutate(date = as.Date(ifelse(!is.na(date1), date1,
                                              ifelse(!is.na(date2), date2, NA)),
                        origin="1970-01-01")) %>%
  mutate(day_of_the_week = toupper(weekdays(date))) %>%
  mutate(day_of_the_week = ifelse(is.na(date), "Missing", day_of_the_week))

table(survey_standard$operator, survey_standard$day_of_the_week)

# Fill in missing weekpart
survey_standard <- survey_standard %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "SUNDAY",   "WEEKEND", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "MONDAY",   "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "TUESDAY",  "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "WEDNESDAY","WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "THURSDAY", "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "FRIDAY",   "WEEKDAY", weekpart)) %>%
  mutate(weekpart = ifelse(is.na(weekpart) & day_of_the_week == "SATURDAY", "WEEKEND", weekpart))

table(survey_standard$operator,survey_standard$weekpart)

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
  # create a time_start (in hours) for day_part
  mutate(time_start = as.numeric(format(survey_time_posix,"%H"))) %>%
  mutate(day_part = 'EVENING') %>%
  mutate(day_part = ifelse(time_start >= 3  & time_start < 6,  'EARLY AM', day_part)) %>%
  mutate(day_part = ifelse(time_start >= 6  & time_start < 10, 'AM PEAK' , day_part)) %>%
  mutate(day_part = ifelse(time_start >= 10 & time_start < 15, 'MIDDAY'  , day_part)) %>%
  mutate(day_part = ifelse(time_start >= 15 & time_start < 19, 'PM PEAK' , day_part)) %>%
  # keep survey_time to output
  mutate(survey_time=format(survey_time_posix, format="%H:%M:%S"))

table(survey_standard$field_start)
table(survey_standard$field_end)
table(survey_standard$time_start)
table(survey_standard$day_part)
table(survey_standard$day_of_the_week)

survey_standard <- survey_standard %>%
  select(-date_string, -time_string, -time1, -time2,
         -time3, -time4, -survey_time_posix)


## Geocode XY to travel model geographies

# Prepare and write locations that need to be geo-coded to disk
survey_standard <- survey_standard %>%
  mutate(unique_ID = paste(ID, operator, survey_year, sep = "___"))

dup12 <- survey_standard[duplicated(survey_standard),]

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

# remove(survey_lat, survey_lon)

## Geocode Transit Locations

taps_coords <- read.csv(f_taps_coords_path) %>%
  rename_all(tolower) %>%
  select(n, mode, lat, lon = long) %>%
  mutate(mode = recode(mode,
                       `1` = "local bus", `2` = "express bus", `3` = "ferry",
                       `4` = "light rail", `5` = "heavy rail", `6` = "commuter rail"))

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

for (item in unique(taps_spatial$mode)) {
  temp_tap_spatial <- taps_spatial %>%
    filter(mode == item)
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

# remove(alight_coords, board_coords,
#       survey_board_spatial, survey_alight_spatial,
#       taps_coords, taps_spatial)


## Geocode Other Locations

survey_coords_spatial <- st_as_sf(survey_coords, coords = c("x_coord", "y_coord"), crs = 4326)
survey_coords_spatial <- st_transform(survey_coords_spatial, crs = 2230)

taz_shp <- st_read(f_taz_shp_path) %>%
  select(taz = TAZ_ORIGIN)
taz_shp <- bind_cols(taz_shp, match = 1:nrow(taz_shp))
taz_shp <- st_set_crs(taz_shp, 2230)

maz_shp <- st_read(f_maz_shp_path) %>%
  select(maz = MAZ_ORIGIN)
maz_shp <- bind_cols(maz_shp, match = 1:nrow(maz_shp))
maz_shp <- st_set_crs(maz_shp, 2230)

#### Find nearest TAZ (within 1/4 mile, else NA)
survey_coords_spatial <- survey_coords_spatial %>%
  st_join(taz_shp, join = st_within)

bad_survey_coords_spatial <- survey_coords_spatial %>%
  filter(is.na(taz))

bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(taz_index = st_nearest_feature(bad_survey_coords_spatial, taz_shp))

bad_survey_coords_dist <- taz_shp %>%
  right_join(data.frame(match = bad_survey_coords_spatial$taz_index), by = "match")  %>%
  rename(bad_taz = taz)
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(dist = st_distance(bad_survey_coords_spatial, bad_survey_coords_dist, by_element = TRUE))

# If there is no TAZ within 1/4 mile, the TAZ is replaced with NA to indicate a failure
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(taz = bad_survey_coords_dist$bad_taz) %>%
  select(-taz_index) %>%
  mutate(taz = ifelse(as.numeric(dist) / 5280 <= 0.25, taz, NA))

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
  rename(dist_taz = taz)

survey_coords_spatial <- survey_coords_spatial %>%
  left_join(bad_survey_coords_spatial, by = c("unique_ID", "variable")) %>%
  mutate(taz = ifelse(is.na(taz), dist_taz, taz)) %>%
  select(-dist_taz, -match)

# check there is no duplicated unique_ID+variable
# "chk" should have 0 record
chk = survey_coords_spatial[duplicated(survey_coords_spatial[,1:2]), ]

# Find nearest MAZ within 1/4 mile (else NA)
survey_coords_spatial <- survey_coords_spatial %>%
  st_join(maz_shp, join = st_within)

bad_survey_coords_spatial <- survey_coords_spatial %>%
  filter(is.na(maz)) %>%
  select(-taz)


bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(maz_index = st_nearest_feature(bad_survey_coords_spatial, maz_shp))

bad_survey_coords_dist <- maz_shp %>%
  right_join(data.frame(match = bad_survey_coords_spatial$maz_index), by = "match")  %>%
  rename(bad_maz = maz)
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(dist = st_distance(bad_survey_coords_spatial, bad_survey_coords_dist, by_element = TRUE))

# If there is no maz within 1/4 mile, the maz is replaced with NA to indicate a failure
bad_survey_coords_spatial <- bad_survey_coords_spatial %>%
  mutate(maz = bad_survey_coords_dist$bad_maz) %>%
  select(-maz_index) %>%
  mutate(maz = ifelse(as.numeric(dist) / 5280 <= 0.25, maz, NA))

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
  rename(dist_maz = maz)

survey_coords_spatial <- survey_coords_spatial %>%
  left_join(bad_survey_coords_spatial, by = c("unique_ID", "variable")) %>%
  mutate(maz = ifelse(is.na(maz), dist_maz, maz)) %>%
  select(-dist_maz, -match)

# check there is no duplicated unique_ID+variable
# "chk" should have 0 record
chk = survey_coords_spatial[duplicated(survey_coords_spatial[,1:2]), ]
# (Nov 11, 2020) One Muni 2017 survey record (ID 21120) is joined to two maz zones, creating duplicates.
# Temporarily manually drop the duplicates
survey_coords_spatial <- survey_coords_spatial[!duplicated(survey_coords_spatial[,1:2]), ]

# Bring in the geocoding results
st_geometry(survey_coords_spatial) <- NULL

survey_coords_spatial_taz <- survey_coords_spatial %>%
  select(-maz) %>%
  spread(variable, taz) %>%
  rename(dest_taz = dest, home_taz = home, orig_taz = orig,
         school_taz = school, workplace_taz = workplace)

survey_coords_spatial_maz <- survey_coords_spatial %>%
  select(-taz) %>%
  spread(variable, maz) %>%
  rename(dest_maz = dest, home_maz = home, orig_maz = orig,
         school_maz = school, workplace_maz = workplace)

board_alight_tap <- board_alight_tap %>%
  select(unique_ID, board_tap, alight_tap)

# Joins
survey_standard <- survey_standard %>%
  left_join(survey_coords_spatial_taz, by = c("unique_ID")) %>%
  left_join(survey_coords_spatial_maz, by = c("unique_ID")) %>%
  left_join(board_alight_tap, by = c("unique_ID"))

remove(board_alight_tap,
       survey_coords,
       survey_coords_spatial,
       survey_coords_spatial_taz,
       survey_coords_spatial_maz)


### Clean up data types

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
         path_access,
         path_access_recode,
         path_egress,
         path_egress_recode,
         path_line_haul,
         path_label,
         boardings,
         day_of_the_week,
         field_start,
         field_end,
         day_part,
         trip_weight)

saveRDS(survey_decomposition, file = f_output_decom_rdata_path)
write.csv(survey_decomposition, file = f_output_decom_csv_path,  row.names = FALSE)

# Drop variables we don't want to carry forward to standard dataset
survey_standard <- survey_standard %>%
  select(-at_school_after_dest_purp,
         -at_school_prior_to_orig_purp,
         -at_work_after_dest_purp,
         -at_work_prior_to_orig_purp,
         -date,
         -time_start,
         -vehicle_numeric_cat,
         -worker_numeric_cat,
         -year_born,
         -number_transfers_alight_dest,
         -number_transfers_orig_board,
         -first_route_before_survey_board,
         -first_route_after_survey_alight,
         -second_route_before_survey_board,
         -second_route_after_survey_alight,
         -third_route_before_survey_board,
         -third_route_after_survey_alight,
         -first_before_operator,
         -second_before_operator,
         -third_before_operator,
         -first_after_operator,
         -second_after_operator,
         -third_after_operator,
         -first_before_technology,
         -second_before_technology,
         -third_before_technology,
         -first_after_technology,
         -second_after_technology,
         -third_after_technology)

# Drop lat/lon for locations
survey_standard <- survey_standard %>%
  select(-dest_lat,
         -dest_lon,
         -home_lat,
         -home_lon,
         -orig_lat,
         -orig_lon,
         -school_lat,
         -school_lon,
         -workplace_lat,
         -workplace_lon)


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

saveRDS(survey_standard, file = f_output_rds_path)
saveRDS(ancillary_df, file = f_ancillary_output_rdata_path)

write.csv(survey_standard, file = f_output_csv_path, row.names = FALSE)
write.csv(ancillary_df, file = f_ancillary_output_csv_path, row.names = FALSE)
