##################################################################################################
### Script to implement decomposition analysis on MTC survey data
### Author: Shimon Israel, February 2018, based on Binny M Paul, binny.paul@rsginc.com, April 2016
### Updated: John Helsel, john.helsel@wsp.com, October 2018
##################################################################################################

# Libraries and Options
list_of_packages <- c("tidyverse")

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

options(stringsAsFactors = FALSE)

# User Inputs
# New users should add their own path to the relevant data in user_list
user_list <- data.frame(
  user = c("helseljw"), 
  path = c("../Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

# File Paths
obs_data_path <- paste0(dir_path, "_data Standardized/decomposition/survey_decomposition.RData")
legacy_data_path <- paste0(dir_path, "_data Standardized/survey_legacy.RData")
decom_output_path <- paste0(dir_path, "_data Standardized/decomposition/DecompositionAnalysis.csv")

# Read Files
load(legacy_data_path)
load(obs_data_path)

# Rename operators so everything matches and filter weekday records only

obs <- survey_decomposition %>% 
  mutate(operator = toupper(operator),
         operator = ifelse(operator == "SF MUNI", "MUNI", operator)
         ) %>%
  filter(weekpart == "WEEKDAY") # Weekday records only 


# Decomposition Analysis
da_table <- data.frame("operator" = unique(obs$operator))
da_table <- da_table %>%
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

temp <- obs %>%
  filter(first_before_operator %in% da_table$operator) %>%
  group_by(operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = "operator") %>%
  mutate(surveyed_resp = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(first_before_operator %in% da_table$operator) %>%
  group_by(first_before_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "first_before_operator")) %>%
  mutate(f1 = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(second_before_operator %in% da_table$operator) %>%
  group_by(second_before_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "second_before_operator")) %>%
  mutate(f2 = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(third_before_operator %in% da_table$operator) %>%
  group_by(third_before_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "third_before_operator")) %>%
  mutate(f3 = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(first_after_operator %in% da_table$operator) %>%
  group_by(first_after_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "first_after_operator")) %>%
  mutate(t1 = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(second_after_operator %in% da_table$operator) %>%
  group_by(second_after_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "second_after_operator")) %>%
  mutate(t2 = trip_weight) %>% 
  select(-trip_weight)

temp <- obs %>%
  filter(third_after_operator %in% da_table$operator) %>%
  group_by(third_after_operator) %>%
  summarise(trip_weight = sum(trip_weight))
da_table <- da_table %>%
  left_join(temp, by = c("operator" = "third_after_operator")) %>%
  mutate(t3 = trip_weight) %>% 
  select(-trip_weight)

da_table[is.na(da_table)] <- 0

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
