# ACTC Income Summaries.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Jacki Taylor/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and directories and read in files
dir_path                <- "M:/Data/OnBoard/Data and Reports/"
f_actransit_survey_path <- paste0(dir_path,
                                  "AC Transit/2018/As CSV/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode)_fixTransfers_NO POUND OR SINGLE QUOTE.csv")
f_unioncity_survey_path <- paste0(dir_path,
                                  "Union City/2017/As CSV/Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv")
f_lavta_survey_path     <- paste0(dir_path,
                              "LAVTA/2018/As CSV/OD_20181207_LAVTA_Submittal_FINAL_addCols_NO POUND OR SINGLE QUOTE.csv")

ac     <- read.csv(f_actransit_survey_path, stringsAsFactors = FALSE) %>% 
  mutate(unlinked_weight_factor=as.numeric(unlinked_weight_factor))
uc     <- read.csv(f_unioncity_survey_path, stringsAsFactors = FALSE) %>% 
  mutate(weight=as.numeric(weight))
lavta  <- read.csv(f_lavta_survey_path, stringsAsFactors = FALSE) %>% 
  mutate(Unlinked_Weight_Factor=as.numeric(Unlinked_Weight_Factor))

# Summarize income for AC
ac_summary <- ac %>% 
  filter(!is.na(unlinked_weight_factor)) %>% 
  group_by(ac_income=household_income) %>% 
  summarize(ac_total=sum(unlinked_weight_factor))

# Summarize income for Lavta

lavta_summary <- lavta %>% 
  filter(!is.na(Unlinked_Weight_Factor)) %>% 
  group_by(lavta_income=household_income) %>% 
  summarize(lavta_total=sum(Unlinked_Weight_Factor))

# Summarize income for Union City

uc_summary <- uc %>% 
  filter(!is.na(weight)) %>% 
  group_by(uc_income=income) %>% 
  summarize(uc_total=sum(weight))

final <- bind_cols(ac_summary,lavta_summary,uc_summary)


# Summarize income for LAVTA

write.csv(final,"Alameda County Operator Income.csv",row.names = FALSE)







