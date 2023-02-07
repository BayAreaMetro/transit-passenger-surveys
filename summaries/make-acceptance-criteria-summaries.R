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

# Reductions -------------------------------------------------------------------
by_time_period_df <- survey %>%
  filter(weekpart != "WEEKEND") %>%
  filter(survey_year %in% survey_years_to_summarise) %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
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

# Write ------------------------------------------------------------------------
write_csv(output_df, output_filename)


