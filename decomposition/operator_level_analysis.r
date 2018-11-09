# operator-level decomposition analysis

# Libraries and Options --------------------------------------------------------
list_of_packages <- c("tidyverse")

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

options(stringsAsFactors = FALSE)

# User-specific Dir ------------------------------------------------------------
user_list <- data.frame(
  user = c("helseljw", 
           "USDO225024"), 
  path = c("../../Data and Reports/", 
           "~/GitHub/onboard-surveys/Data and Reports/")
)

dir_path <- user_list %>%
  filter(user == Sys.getenv("USERNAME")) %>%
  .$path

# File Paths -------------------------------------------------------------------
obs_data_path <- paste0(dir_path, "_data Standardized/decomposition/survey_decomposition.RDS")
legacy_data_path <- paste0(dir_path, "_data Standardized/survey_legacy.Rdata")
output_path <- paste0(dir_path, "_data Standardized/decomposition/operator_level_analysis.csv")

# Read Files -------------------------------------------------------------------
load(legacy_data_path)
survey_decomposition <- readRDS(obs_data_path)

# Align Operator Names ---------------------------------------------------------
obs_df <- survey_decomposition %>% 
  mutate(operator = toupper(operator),
         operator = ifelse(operator == "SF MUNI", "MUNI", operator)) %>%
  filter(weekpart == "WEEKDAY")


# Decomposition Analysis -------------------------------------------------------
working_df <- obs_df %>%
  group_by(operator) %>%
  summarise(surveyed_resp = sum(trip_weight))


add_sequence <- function(observed_df, running_df, sequence_name_string, weight_name_string) {
  
  vars <- c(operator = sequence_name_string)
  
  join_df <- observed_df %>%
    select(vars, trip_weight) %>%
    group_by(operator) %>%
    summarise(trip_weight = sum(trip_weight))
  
  vars <- c(weight_name_string)
  
  return_df <- running_df %>%
    left_join(join_df, by = c("operator")) %>%
    mutate(trip_weight = ifelse(is.na(trip_weight), 0, trip_weight)) %>%
    mutate(!!weight_name_string := trip_weight) %>%
    select(-trip_weight)

  return(return_df)

  
}

working_df <- add_sequence(obs_df, working_df, 'first_before_operator',  'before_01')
working_df <- add_sequence(obs_df, working_df, 'second_before_operator', 'before_02')
working_df <- add_sequence(obs_df, working_df, 'third_before_operator',  'before_03')

working_df <- add_sequence(obs_df, working_df, 'first_after_operator',   'after_01')
working_df <- add_sequence(obs_df, working_df, 'second_after_operator',  'after_02')
working_df <- add_sequence(obs_df, working_df, 'third_after_operator',  'after_03')

working_df <- working_df %>%
  mutate(trnsf_from_resp = before_01 + before_02 + before_03) %>%
  mutate(trnsf_to_resp = after_01 + after_02 + after_03) %>%
  select(operator, surveyed_resp, trnsf_from_resp, trnsf_to_resp)


# Process unlinked boardings --------------------------------------------------
boardings_df <- obs_df %>%
  group_by(operator) %>%
  summarise(weight = sum(weight))

working_df <- working_df %>%
  left_join(boardings_df, by = "operator") %>%
  rename(observed = weight) %>%
  arrange(operator)

write.csv(working_df, output_path, row.names = FALSE)


