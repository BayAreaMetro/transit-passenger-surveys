# Income and Race_Ethnicity by Operator for Zero Car Households.r
# Summarize household income and race/ethnicity by operator 
# Note that SMART data is not part of the standardized dataset (as it started service after 2015)

# Import Library

suppressMessages(library(tidyverse))

# Eliminate scientific notation

options(scipen = 999)

# Input TPS

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
load (TPS_SURVEY_IN)

# Output location

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
BOX_TM               <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
Output               <- file.path(BOX_TM,"Share Data","bespoke","Transit_Passenger_Survey","Transform Summaries")

# Now analyze database for all operators (other than SMART)
# Income

survey_income <- TPS %>% 
  filter(!(household_income %in% c("under $35,000","Missing","refused","$35,000 or higher"))) %>% 
  filter(!(is.na(household_income))) %>% 
  filter(vehicles=="zero") %>% 
  mutate(
    income_rc=case_when(
      household_income=="under $10,000"        ~"1_less than 25k",
      household_income=="$10,000 to $25,000"   ~"1_less than 25k",
      household_income=="under $25,000"        ~"1_less than 25k",
      household_income=="$25,000 to $35,000"   ~"2_25-50k",
      household_income=="$35,000 to $50,000"   ~"2_25-50k",
      household_income=="$25,000 to $50,000"   ~"2_25-50k",
      household_income=="$50,000 to $75,000"   ~"3_50-75k",
      household_income=="$75,000 to $100,000"  ~"4_75-100k",
      household_income=="$100,000 to $150,000" ~"5_100-150k",
      household_income=="$150,000 or higher"   ~"6_150k+",
      TRUE                           ~"Not coded"
    )
  ) %>% 
  group_by(operator,income_rc) %>% 
  summarize(total=sum(final_boardWeight_2015)) %>% 
  spread(income_rc,total)

# Race

survey_race <- TPS %>% 
  filter(!(race=="Missing")) %>% 
  filter(vehicles=="zero") %>% 
  mutate(
    race_rc=case_when(
      hispanic=="HISPANIC/LATINO OR OF SPANISH ORIGIN"  ~ "5_Hispanic",
      race=="WHITE"                                     ~ "1_White",
      race=="BLACK"                                     ~ "2_Black",
      race=="ASIAN"                                     ~ "3_Asian",
      race=="OTHER"                                     ~ "4_Other",
      TRUE                                              ~ "Not coded"
    )
  ) %>% 
  group_by(operator,race_rc) %>% 
  summarize(total=sum(final_boardWeight_2015)) %>% 
  spread(race_rc,total)

# Write out files

write.csv(survey_income,file.path(Output,"Transit passengers with zero vehicles by income.csv"),row.names = FALSE)
write.csv(survey_race,file.path(Output,"Transit passengers with zero vehicles by race_ethnicity.csv"),row.names = FALSE)
 
