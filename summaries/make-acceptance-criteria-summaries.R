#-------------------------------------------------------------------------------
#title: "Make Acceptance Criteria Summaries"
#-------------------------------------------------------------------------------
  
# Overhead ---------------------------------------------------------------------
packages_vector <- c("tidyverse")

need_to_install <- packages_vector[!(packages_vector %in% installed.packages()[,"Package"])]

if (length(need_to_install)) install.packages(need_to_install)

for (package in packages_vector) {
  library(package, character.only = TRUE)
}

# Remote I-O -------------------------------------------------------------------
box_dir <- "~/Box Sync/"
survey_filename <- paste0(box_dir, "Survey_Database_122717/survey.Rdata")
output_filename <- paste0(
  box_dir, 
  "Survey_Database_122717/acceptance-criteria-summaries-year-2015.csv"
)

output_access_filename <- paste0(box_dir, "Survey_Database_122717/acceptance-criteria-access-summaries-year-2015.csv")

output_flows_filename <- paste0(box_dir, "Survey_Database_122717/acceptance-criteria-spatial-flows-year-2015.csv")

# Parameters -------------------------------------------------------------------
time_period_dict_df <- tibble(
  day_part = c("EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "NIGHT"),
  time_period = c("ea", "am", "md", "pm", "ev", "ev")
)
rail_operators_vector <- c("BART","Caltrain","ACE","Sonoma-Marin Area Rail Transit")
ALL_DAY_WORD <- "daily"
survey_years_to_summarise <- seq(from = 2012, to = 2017)

# Methods ----------------------------------------------------------------------
make_direction_from_route <- function(input_df, input_reg_ex_word, brackets_bool) {
  
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
  
  return(return_df)
  
}

# Data Reads -------------------------------------------------------------------
load(survey_filename)

# Reductions 00: Common --------------------------------------------------------
common_df <- survey %>%
  filter(weekpart != "WEEKEND") %>%
  filter(survey_year %in% survey_years_to_summarise) %>%
  left_join(., time_period_dict_df, by = c("day_part")) 
  

# Reductions 01: Boardings by route --------------------------------------------
by_time_period_df <- common_df %>%
  mutate(is_rail = operator %in% rail_operators_vector) %>%
  mutate(route = if_else(is_rail, operator, route)) %>%
  make_direction_from_route(., "\\[ INBOUND \\]", TRUE) %>%
  make_direction_from_route(., "\\[ OUTBOUND \\]", TRUE) %>%
  make_direction_from_route(., "\\[Eastbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Westbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Northbound\\]", TRUE) %>%
  make_direction_from_route(., "\\[Southbound\\]", TRUE) %>%
  make_direction_from_route(., "NORTHBOUND", FALSE) %>%
  make_direction_from_route(., "SOUTHBOUND", FALSE) %>%
  group_by(operator, route, direction, time_period) %>%
  summarise(survey_boardings = sum(weight), .groups = "drop")

daily_df <- by_time_period_df %>%
  group_by(operator, route) %>%
  summarise(survey_boardings = sum(survey_boardings), .groups = "drop") %>%
  mutate(direction = NA) %>%
  mutate(time_period = ALL_DAY_WORD)

output_df <- bind_rows(by_time_period_df, daily_df) %>%
  rename(survey_operator = operator,
         survey_route = route,
         survey_direction = direction)

# Reductions 02: Access shares for rail stations -------------------------------
access_df <- common_df %>%
  filter(operator %in% rail_operators_vector) %>%
  group_by(operator, onoff_enter_station, time_period, access_mode) %>%
  summarise(survey_trips = sum(trip_weight), .groups = "drop") %>%
  filter(!is.na(time_period)) %>%
  filter(!is.na(access_mode)) %>%
  select(operator,
         boarding_station = onoff_enter_station,
         time_period,
         access_mode,
         survey_trips)

# Reductions 03: Flows by technology -------------------------------------------
working_df <- common_df %>%
  filter(orig_taz > 0) %>%
  filter(dest_taz > 0) %>%
  mutate(temp = (first_board_tech == "local bus") | (last_alight_tech == "local bus") | (survey_tech == "local bus")) %>%
  mutate(is_loc_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "express bus") | (last_alight_tech == "express bus") | (survey_tech == "express bus")) %>%
  mutate(is_exp_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "light rail") | (last_alight_tech == "light rail") | (survey_tech == "light rail")) %>%
  mutate(is_lrt_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "ferry") | (last_alight_tech == "ferry") | (survey_tech == "ferry")) %>%
  mutate(is_fry_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "heavy rail") | (last_alight_tech == "heavy rail") | (survey_tech == "heavy rail")) %>%
  mutate(is_hvy_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(temp = (first_board_tech == "commuter rail") | (last_alight_tech == "commuter rail") | (survey_tech == "commuter rail")) %>%
  mutate(is_com_in_path = if_else(temp, 1.0, 0.0)) %>%
  mutate(techs_in_path = is_loc_in_path + is_exp_in_path + is_lrt_in_path + is_fry_in_path + is_hvy_in_path + is_com_in_path) %>%
  select(Unique_ID, orig_taz, dest_taz, time_period, is_loc_in_path, is_exp_in_path, is_lrt_in_path, is_fry_in_path, is_hvy_in_path, is_com_in_path, techs_in_path, trip_weight)

flows_df <- working_df %>%
  group_by(orig_taz, dest_taz, time_period) %>%
  summarise(is_loc_in_path = sum(is_loc_in_path)/sum(techs_in_path),
            is_exp_in_path = sum(is_exp_in_path)/sum(techs_in_path),
            is_lrt_in_path = sum(is_lrt_in_path)/sum(techs_in_path),
            is_fry_in_path = sum(is_fry_in_path)/sum(techs_in_path),
            is_hvy_in_path = sum(is_hvy_in_path)/sum(techs_in_path),
            is_com_in_path = sum(is_com_in_path)/sum(techs_in_path),
            observed_trips = sum(trip_weight),
            .groups = "drop")

check_df <- flows_df %>%
  mutate(sum_check = is_loc_in_path + is_exp_in_path + is_lrt_in_path + is_hvy_in_path + is_fry_in_path + is_com_in_path) %>%
  filter(sum_check > 1.0)


# Write ------------------------------------------------------------------------
write_csv(output_df, output_filename)
write_csv(access_df, output_access_filename)
write_csv(flows_df, output_flows_filename)

