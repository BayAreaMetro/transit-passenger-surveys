# Libraries and Options --------------------------------------------------------
list_of_packages <- c("tidyverse")

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

options(stringsAsFactors = FALSE)

# User Inputs ------------------------------------------------------------------
# New users should add their own path to the relevant data in user_list
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
  filter(weekpart == "WEEKDAY") # Weekday records only 


# Decomposition Analysis -------------------------------------------------------
# TODO revisit -- why 0 and not NA?
decom_df <- data.frame(operator = unique(obs_df$operator)) %>%
  mutate(surveyed_resp = 0,
         trnsf_from_resp = 0,
         trnsf_to_resp = 0,
         t1 = 0,
         t2 = 0,
         t3 = 0,
         t4 = 0,
         f1 = 0,
         f2 = 0,
         f3 = 0,
         f4 = 0)
  
join_df <- obs_df %>%
  filter(first_before_operator %in% decom_df$operator) %>%
  group_by(operator) %>%
  summarise(trip_weight = sum(trip_weight))

decom_df <- decom_df %>%
  left_join(join_df, by = "operator") %>%
  mutate(surveyed_resp = trip_weight) %>% 
  select(-trip_weight)

add_sequence <- function(observed_df, running_df, sequence_name_string, weight_name_string) {
  
  # observed_df <- obs_df
  # running_df <- decom_df
  # sequence_name_string <- 'first_before_operator'
  # weight_name_string <- 'f1'
  
  vars <- c(operator = sequence_name_string)
  
  join_df <- observed_df %>%
    select(vars, trip_weight) %>%
    group_by(operator) %>%
    summarise(trip_weight = sum(trip_weight))
  
  vars <- c(weight_name_string)
  
  return_df <- running_df %>%
    left_join(join_df, by = c("operator")) %>%
    mutate(!!weight_name_string := trip_weight) %>%
    select(-trip_weight)

  return(return_df)

  
}

decom_df <- add_sequence(obs_df, decom_df, 'first_before_operator',  'f1')
decom_df <- add_sequence(obs_df, decom_df, 'second_before_operator', 'f2')
decom_df <- add_sequence(obs_df, decom_df, 'third_before_operator',  'f3')

decom_df <- add_sequence(obs_df, decom_df, 'first_after_operator',   't1')
decom_df <- add_sequence(obs_df, decom_df, 'second_after_operator',  't2')
decom_df <- add_sequence(obs_df, decom_df, 'second_after_operator',  't3')

# START HERE 

decom_df[is.na(da_table)] <- 0

da_table <- da_table %>%
  mutate(trnsf_from_resp = f1 + f2 + f3 + f4,
         trnsf_to_resp = t1 + t2 + t3 + t4
         )

da_table <- da_table %>%
  select(operator, surveyed_resp, trnsf_from_resp, trnsf_to_resp)

# Process unlinked boardings

boardings <- obs %>%
  group_by(operator) %>%
  summarise(weight = sum(weight))

da_table <- da_table %>%
  left_join(boardings, by = "operator") %>%
  rename(observed = weight)

da_table <- da_table %>%
  arrange(operator)

write.csv(da_table, decom_output_path, row.names = FALSE)
