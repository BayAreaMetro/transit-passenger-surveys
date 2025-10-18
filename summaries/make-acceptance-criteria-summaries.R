#-------------------------------------------------------------------------------
#
# Create acceptance criteria summaries for TM2
# See https://github.com/BayAreaMetro/tm2py-utils/tree/main/tm2py_utils/summary/acceptance
#
# Right now, this is 2015-specific but it should be generalized to work for different
# model years.
#
# This creates three outputfiles:
# 1. acceptance-criteria-summaries-year-2015.csv with columns:
#    survey_operator:
#    survey_tech
#    survey_route
#    time_period
#    survey_boardings: based on final_boardWeight_2015
# 2. acceptance-criteria-access-summaries-year-2015.csv with columns:
#    operator (ACE, BART or Caltrain)
#    boarding_station
#    time_period
#    access_mode
#    survey_trips: based on trip_weight
# 3. acceptance-criteria-spatial-flows-year-2015.csv with columns:
#    orig_taz
#    dest_taz
#    time_period
#    is_[loc,exp,ltr,fry,hvy,com]_in_path: based on first_board_tech, last_alight_tech or SURVEY_MODE,
#       this is the share of the trips for this orig/dest/time_period that use this tech.
#       These sum to 100% for orig/dest/time_period.
#    observed_trips: based on final_tripWeight_2015

#-------------------------------------------------------------------------------
  
# Overhead ---------------------------------------------------------------------
packages_vector <- c("tidyverse")

need_to_install <- packages_vector[!(packages_vector %in% installed.packages()[,"Package"])]

if (length(need_to_install)) install.packages(need_to_install)

for (package in packages_vector) {
  library(package, character.only = TRUE)
}
options(width = 10000)
options(dplyr.width = 10000)
options(datatable.print.nrows = 1000)
options(str = strOptions(list.len = 1000))
options(warn=2) # error on warning
# don't warn: "summarise()` has grouped output by ... You can override using the `.groups` argument."
options(dplyr.summarise.inform=F) 

# Remote I-O -------------------------------------------------------------------
BOX_DIR <- "E:/Box/Modeling and Surveys"
# Documentation on this: https://app.asana.com/1/11860278793487/project/1199982433633229/task/1211653469121610?focus=true
SURVEY_DATA_FILE <- file.path(BOX_DIR, "Share Data/Protected Data/David Ory/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata")
OUTPUT_DIR       <- file.path(BOX_DIR, "Development/Travel Model Two Conversion/Observed/2015 Observed Data/Survey_Database_090221")

output_filename        <- file.path(OUTPUT_DIR, "acceptance-criteria-summaries-year-2015.csv")
output_access_filename <- file.path(OUTPUT_DIR, "acceptance-criteria-access-summaries-year-2015.csv")
output_flows_filename  <- file.path(OUTPUT_DIR, "acceptance-criteria-spatial-flows-year-2015.csv")

run_log <- file.path(OUTPUT_DIR, "make-acceptance-criteria-summaries.log")
print(paste("Writing log file to",run_log))
sink(run_log, append=FALSE, type = c('output', 'message'))

# Parameters -------------------------------------------------------------------
time_period_dict_df <- tibble(
  day_part = c("EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "NIGHT"),
  time_period = c("ea", "am", "md", "pm", "ev", "ev")
)

mode_dict_df <- tibble(
  SURVEY_MODE = c("LB", "EB", "LR", "FR", "HR", "CR"),
  survey_tech = c("local bus", "express bus", "light rail", "ferry", "heavy rail", "commuter rail")
)

rail_operators_vector <- c("BART","Caltrain","ACE","Capitol Corridor","Sonoma-Marin Area Rail Transit")
ALL_DAY_WORD <- "daily"
survey_years_to_summarise <- seq(from = 2012, to = 2019)

# Methods ----------------------------------------------------------------------
make_direction_from_route <- function(input_df, input_reg_ex_word, brackets_bool) {
  print(paste("make_direction_from_route() with",input_reg_ex_word,", ",brackets_bool))
  if (brackets_bool) {
    replace_word <- trimws(gsub("\\[|\\]", "", input_reg_ex_word))
  }
  else {
    replace_word <- input_reg_ex_word
  }
  
  return_df <- input_df %>%
    mutate(flag = str_detect(route, input_reg_ex_word)) %>%
    mutate(route = if_else(flag, trimws(str_replace(route, input_reg_ex_word, "")), route)) %>%
    mutate(direction = if_else(flag, replace_word, direction)) 
  
  # what did we do?
  # print("route/direction for flag:")
  # print(dplyr::count(return_df, flag))
  # print(dplyr::count(filter(return_df, flag==TRUE), route, direction))

  return(return_df)
  
}

# Data Reads -------------------------------------------------------------------
print(paste("Loading",SURVEY_DATA_FILE))
load(SURVEY_DATA_FILE, verbose = TRUE)
print("str(TPS):")
print(str(TPS))

print("survey_year counts:")
print(dplyr::count(TPS, survey_year, sort=TRUE))

# Reductions 00: Common --------------------------------------------------------
common_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  select(-time_period) %>%
  filter(survey_year %in% survey_years_to_summarise) %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  left_join(., mode_dict_df, by = c("SURVEY_MODE"))

common_df <- common_df %>% mutate(
  operator=replace_na(operator, "Missing"),
  survey_tech=replace_na(survey_tech, "Missing")
)
print("operator count:")
print(dplyr::count(common_df, operator))

print("survey_tech count:")
print(dplyr::count(common_df, survey_tech))

# Reductions 01: Boardings by route --------------------------------------------
# For rail operators, routes aren't really routes but a combination of board/alight stops
#   So discard route, and just use operator.
# For other operators, ...
by_time_period_df <- common_df %>%
  mutate(is_rail = operator %in% rail_operators_vector) %>%
  mutate(route = if_else(is_rail, operator, route))

print("Before make_direction_from_route()")
print(dplyr::count(by_time_period_df, is_rail, operator, route, sort=TRUE))

by_time_period_df <- by_time_period_df %>%
  make_direction_from_route(., "\\[ INBOUND \\]", TRUE) %>%
  make_direction_from_route(., "\\[ OUTBOUND \\]", TRUE) %>%
  make_direction_from_route(., "\\[Eastbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Westbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Northbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Southbound\\]", TRUE) %>%
  make_direction_from_route(., "NORTHBOUND", FALSE) %>%
  make_direction_from_route(., "SOUTHBOUND", FALSE)

# Noting that this currently does nothing because these strings aren't in the routes...
# Route includes direction for some operators but not for others
print("After make_direction_from_route()")
print(dplyr::count(by_time_period_df, is_rail, operator, route, sort=TRUE))

by_time_period_df <- by_time_period_df %>%
  group_by(operator, survey_tech, route, time_period) %>%
  summarise(survey_boardings = sum(final_boardWeight_2015), num_records = n(), .groups = "drop")

output_df <- by_time_period_df %>%
  rename(survey_operator = operator,
         survey_route = route)

# Reductions 02: Access shares for rail stations -------------------------------
access_df <- common_df %>%
  filter(operator %in% rail_operators_vector) %>%
  group_by(operator, onoff_enter_station, time_period, access_mode) %>%
  summarise(survey_trips = sum(final_tripWeight_2015), num_records = n(), .groups = "drop") %>%
  filter(!is.na(time_period)) %>%
  filter(!is.na(access_mode)) %>%
  select(operator,
         boarding_station = onoff_enter_station,
         time_period,
         access_mode,
         survey_trips,
         num_records)

# Reductions 03: Flows by technology -------------------------------------------
working_df <- common_df %>%
  filter(orig_tm2_taz > 0) %>%
  filter(dest_tm2_taz > 0) %>%
  mutate(temp = (first_board_tech == "LB") | (last_alight_tech == "LB") | (SURVEY_MODE == "LB")) %>%
  mutate(is_loc_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "EB") | (last_alight_tech == "EB") | (SURVEY_MODE == "EB")) %>%
  mutate(is_exp_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "LR") | (last_alight_tech == "LR") | (SURVEY_MODE == "LR")) %>%
  mutate(is_ltr_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "FR") | (last_alight_tech == "FR") | (SURVEY_MODE == "FR")) %>%
  mutate(is_fry_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "HR") | (last_alight_tech == "HR") | (SURVEY_MODE == "HR")) %>%
  mutate(is_hvy_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "CR") | (last_alight_tech == "CR") | (SURVEY_MODE == "CR")) %>%
  mutate(is_com_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(techs_in_path = is_loc_in_path + is_exp_in_path + is_ltr_in_path + is_fry_in_path + is_hvy_in_path + is_com_in_path) %>%
  select(ID, 
         orig_taz = orig_tm2_taz, 
         dest_taz = dest_tm2_taz, 
         time_period, 
         is_loc_in_path, 
         is_exp_in_path, 
         is_ltr_in_path, 
         is_fry_in_path, 
         is_hvy_in_path, 
         is_com_in_path, 
         techs_in_path, 
         trip_weight = final_tripWeight_2015)
# All alternative version using trip weights: these only differ when there are multiple trips between an Origin and Destination
# but just to see
working_df <- working_df %>% mutate(
  is_loc_in_path_wt = if_else(is_loc_in_path == 1, trip_weight, 0),
  is_exp_in_path_wt = if_else(is_exp_in_path == 1, trip_weight, 0),
  is_ltr_in_path_wt = if_else(is_ltr_in_path == 1, trip_weight, 0),
  is_fry_in_path_wt = if_else(is_fry_in_path == 1, trip_weight, 0),
  is_hvy_in_path_wt = if_else(is_hvy_in_path == 1, trip_weight, 0),
  is_com_in_path_wt = if_else(is_com_in_path == 1, trip_weight, 0),
  techs_in_path_wt = is_loc_in_path_wt + is_exp_in_path_wt + is_ltr_in_path_wt + is_fry_in_path_wt + is_hvy_in_path_wt + is_com_in_path_wt
)

flows_df <- working_df %>%
  group_by(orig_taz, dest_taz, time_period) %>%
  summarise(is_loc_in_path = sum(is_loc_in_path)/sum(techs_in_path),
            is_exp_in_path = sum(is_exp_in_path)/sum(techs_in_path),
            is_ltr_in_path = sum(is_ltr_in_path)/sum(techs_in_path),
            is_fry_in_path = sum(is_fry_in_path)/sum(techs_in_path),
            is_hvy_in_path = sum(is_hvy_in_path)/sum(techs_in_path),
            is_com_in_path = sum(is_com_in_path)/sum(techs_in_path),
            is_loc_in_path_wt = sum(is_loc_in_path_wt)/sum(techs_in_path_wt),
            is_exp_in_path_wt = sum(is_exp_in_path_wt)/sum(techs_in_path_wt),
            is_ltr_in_path_wt = sum(is_ltr_in_path_wt)/sum(techs_in_path_wt),
            is_fry_in_path_wt = sum(is_fry_in_path_wt)/sum(techs_in_path_wt),
            is_hvy_in_path_wt = sum(is_hvy_in_path_wt)/sum(techs_in_path_wt),
            is_com_in_path_wt = sum(is_com_in_path_wt)/sum(techs_in_path_wt),            
            observed_trips = sum(trip_weight),
            num_records = n(),
            .groups = "drop")

check_df <- flows_df %>%
  mutate(sum_check = is_loc_in_path + is_exp_in_path + is_ltr_in_path + is_hvy_in_path + is_fry_in_path + is_com_in_path) %>%
  filter(sum_check > 1.0)


# Write ------------------------------------------------------------------------
write_csv(output_df, output_filename)
write_csv(access_df, output_access_filename)
write_csv(flows_df, output_flows_filename)

sum_df <- output_df %>%
  filter(time_period == "am") %>%
  group_by(survey_operator) %>%
  summarise(boardings = sum(survey_boardings), .groups = "drop")

sam_df <- TPS %>%
  filter(str_detect(operator, "SamTrans"))