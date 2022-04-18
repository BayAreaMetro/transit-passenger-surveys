# TIP 2023 Income and Race_Ethnicity by Operator.R
# Summarize household income and race/ethnicity by operator for the TIP 2023 equity analysis
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
Output               <- file.path(BOX_TM,"Share Data","bespoke","2023 TIP Investment Analysis")

# Bring in SMART data and summarize
Onboard       <- "M:/Data/OnBoard/Data and Reports/"
SMART_in      <- paste0(Onboard,"SMART/As CSV/SMART Standardized Final Data NO POUND NO SINGLE QUOTE.csv")

SMART <- read.csv(SMART_in, stringsAsFactors = FALSE) 

# Summarize income
SMART_income <- SMART %>% 
  filter(income!="UNKNOWN") %>% mutate(
    income_rc=case_when(
      income=="UNDER $10,000"        ~"1_less than 25k",
      income=="$10,000 to $25,000"   ~"1_less than 25k",
      income=="$25,000 to $35,000"   ~"2_25-50k",
      income=="$35,000 to $50,000"   ~"2_25-50k",
      income=="$50,000 to $75,000"   ~"3_50-75k",
      income=="$75,000 to $100,000"  ~"4_75-100k",
      income=="$100,000 to $150,000" ~"5_100-150k",
      income=="$150,000 OR HIGHER"   ~"6_150k+"
    )
  ) %>% 
  group_by(income_rc) %>% 
  summarize(total=sum(WEIGHT)) %>% 
  spread(income_rc,total)

# Summarize race/ethnicity
SMART_race <- SMART %>% 
  select(hisp,race_dmy_ind,race_dmy_hwi,race_dmy_blk,race_dmy_wht,race_dmy_asn,race_other,race_6_other,WEIGHT) %>% 
  filter(race_6_other != "REFUSED") %>% mutate(
    race_sum=race_dmy_ind+race_dmy_hwi+race_dmy_blk+race_dmy_wht+race_dmy_asn+race_other,
    race_general=case_when(
      hisp=="HISPANIC/LATINO OR OF SPANISH ORIGIN" | race_6_other=="HISPANIC"  ~ "5_Hispanic",
      race_sum>=2                                                              ~ "4_Other, Not Hispanic",
      race_dmy_wht==1                                                          ~ "1_White, Not Hispanic",
      race_dmy_blk==1                                                          ~ "2_Black, Not Hispanic",
      race_dmy_asn==1                                                          ~ "3_Asian, Not Hispanic",
      race_dmy_ind==1                                                          ~ "4_Other, Not Hispanic",
      race_dmy_hwi==1                                                          ~ "4_Other, Not Hispanic",
      race_other==1                                                            ~ "4_Other, Not Hispanic",
      TRUE                                                                     ~ "Uncoded")
  ) %>% 
  group_by(race_general) %>% 
  summarize(total=sum(WEIGHT)) %>% 
  spread(race_general,total)

# Write out final CSV files for SMART

write.csv(SMART_income,file.path(Output,"SMART_income.csv"),row.names = FALSE)
write.csv(SMART_race,file.path(Output,"SMART_race.csv"),row.names = FALSE)

# Now analyze database for all other operators
# Income

survey_income <- TPS %>% 
  filter(!(household_income %in% c("under $35,000","Missing","refused","$35,000 or higher"))) %>% 
  filter(!(is.na(household_income))) %>% 
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

write.csv(survey_income,file.path(Output,"survey_income.csv"),row.names = FALSE)
write.csv(survey_race,file.path(Output,"survey_race.csv"),row.names = FALSE)
 
