# Combine_Snapshot_ACE_Golden_Gate_Recode_Dumbarton.r
# Create dataset that can be used for a regionl Tableau dashboard
# Align race/ethnicity, household income, daypart/weekpart, trip purpose
# 

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)
library(zipcodeR)

# Set file directories for input and output

userprofile     <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir1        <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
box_dir2        <- file.path(box_dir1, "Snapshot Survey", "Data","mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")
ggt_in          <- "M:/Data/OnBoard/Data and Reports/Golden Gate Transit/2023/GoldenGate_Transit_Ferry_preprocessed.csv"
ace_in          <- "M:/Data/OnBoard/Data and Reports/ACE/2023/ACE_Onboard_preprocessed.csv"
ace_original_in <- "M:/Data/OnBoard/Data and Reports/ACE/2023/ACE Onboard Data (sent 7.7.23).xlsx"


# Bring in Snapshot Survey data

snapshot <- read_excel(box_dir2, sheet = "data file")

# Bring in GGT and ACE data

ggt          <- read.csv(ggt_in)
ace_original <- read_excel(
  ace_original_in,
  sheet = "ACE Onboard Data Weighted 7.7.2"
) %>%
  mutate(home_zip = as.character(home_zip)) %>%
  select(id, home_zip)
ace          <- read.csv(ace_in) %>% 
  left_join(.,ace_original,by="id")

# Geocode home county to ace zip codes using a function
# Use the built-in ZIP code database

zip_db <- zipcodeR::zip_code_db

# Join to get county name by ZIP, rename to home_county
ace <- ace %>%
  left_join(zip_db %>% select(zipcode, county), 
            by = c("home_zip" = "zipcode"))  

ace <- ace %>% 
  rename(home_county=county)


  

# Adjust categories for overlapping values
# Start with Snapshot

snapshot1 <- snapshot %>%
  mutate(race_regional=case_when(
    Q19_1=="B" & if_all(Q19_2:Q19_4, is.na)             ~ "Missing",
    if_any(Q19_1:Q19_4, ~.== 4)                         ~ "Hispanic",
    Q19_1=="1" & if_all(Q19_2:Q19_4, is.na)             ~ "Black, not Hispanic",
    Q19_1=="2" & if_all(Q19_2:Q19_4, is.na)             ~ "Other, not Hispanic",
    Q19_1=="3" & if_all(Q19_2:Q19_4, is.na)             ~ "Asian/Pacific Islander, not Hispanic",
    Q19_1=="5" & if_all(Q19_2:Q19_4, is.na)             ~ "Asian/Pacific Islander, not Hispanic",
    Q19_1=="6" & if_all(Q19_2:Q19_4, is.na)             ~ "White, not Hispanic",
    Q19_1 %in% c("7","9") & if_all(Q19_2:Q19_4, is.na)  ~ "Other, not Hispanic",
    if_any(Q19_2:Q19_4, ~ !is.na(.))                    ~ "Other, not Hispanic",
    TRUE                                                ~ "Miscoded"
  ),
  income_regional=recode(Q22,
                "1"="Under $50,000",
                "2"="Under $50,000",
                "3"="Under $50,000",
                "4"="Under $50,000",
                "5"="$50,000 to $99,999",
                "6"="$50,000 to $99,999",
                "7"="$50,000 to $99,999",
                "8"="$50,000 to $99,999",
                "9"="$100,000 to $149,999",
                "10"="$150,000 to $199,999",
                "11"="$200,000 and above",
                "B"="Missing",
                "M"="Multiple responses"
  ),
  daytype_regional=Daytype,
  strata_regional=Strata,
  purpose_regional=recode(Q1,
                         "1"="Work",
                         "2"="School",
                         "3"="Social/recreation",
                         "4"="Personal errand/medical",
                         "5"="Shopping",
                         "6"="Other",
                         "B"="Missing",
                         "M"="Multiple responses"))

# Adjust Golden Gate

goldengate1 <- goldengate %>% 
  mutate(income_regional=recode(household_income,
                       "1"="Under $50,000",
                       "2"="Under $50,000",
                       "3"="Under $50,000",
                       "4"="Under $50,000",
                       "5"="$50,000 to $99,999",
                       "6"="$50,000 to $99,999",
                       "7"="$100,000 to $149,999",
                       "8"="$150,000 to $199,999",
                       "9"="$200,000 and above",
                       "0"="Missing",
                       "10"="Multiple responses"
  ),
  race_regional = case_when(
    if_all(hispanic:race_dmy_wht, ~ is.na(.)) ~ "Missing",
    hispanic == 1 ~ "Hispanic",
    if_all(hispanic:race_dmy_wht, ~ . == 0)                                                   ~ "Other, not Hispanic", 
    hispanic == 0 & race_dmy_wht == 1 & if_all(race_dmy_asn:race_dmy_ind, ~ . == 0)           ~ "White, not Hispanic",
    hispanic == 0 & race_dmy_asn == 1 & if_all(race_dmy_blk:race_dmy_wht, ~ . == 0)           ~ "Asian/Pacific Islander, not Hispanic",
    hispanic == 0 & race_dmy_blk == 1 & race_dmy_asn==0 & race_dmy_ind==0 & race_dmy_wht==0   ~ "Black, not Hispanic",
    TRUE                                                                                      ~ "Other, not Hispanic" 
  ),
  daytype_regional=case_when(
    Strata %in% c("AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "AM OFF")   ~ "DAY",
    Strata %in% c("SAT","SUN")                                           ~ "END",
    TRUE                                                                 ~ "Missing"),
  strata_regional=case_when(
    Strata %in% c("AM PEAK", "AM OFF")                                   ~ "AM",
    Strata =="MIDDAY"                                                    ~ "MID",
    Strata %in% c("PM PEAK", "EVENING")                                  ~ "EVE",
    TRUE                                                                 ~ "Missing"
  ))

# Adjust ACE

ace1 <- ace %>% 
  mutate(income_regional = recode(income,
                                "1" = "Under $50,000",
                                "2" = "Under $50,000",
                                "3" = "Under $50,000",
                                "4" = "Under $50,000",
                                "5" = "$50,000 to $99,999",
                                "6" = "$50,000 to $99,999",
                                "7" = "$100,000 to $149,999",
                                "8" = "$150,000 to $199,999",
                                "9" = "$200,000 and above",
                                .missing = "Missing"
  ),
  race_simple = case_when(
    hispanic == 1                                                                             ~ "Hispanic",
    hispanic == 2 & if_all(race_1:race_5, ~ . == 0)                                           ~ "Other, not Hispanic", 
    hispanic == 2 & race_4==1 & race_1==0 & race_2==0 & race_3==0 & race_5==0                 ~ "White, not Hispanic",
    hispanic == 2 & race_1==1 & if_all(race_2:race_5, ~ . == 0)                               ~ "Black, not Hispanic",
    hispanic == 2 & race_2==1 & race_1==0 & race_3==0 & race_4==0 & race_5==0                 ~ "Other, not Hispanic",
    hispanic == 2 & race_3==1 & race_1==0 & race_2==0 & race_4==0 & race_5==0                 ~ "Asian/Pacific Islander, not Hispanic",
    hispanic == 2 & race_5==1 & if_all(race_1:race_4, ~ . == 0)                               ~ "Asian/Pacific Islander, not Hispanic",
    TRUE                                                                                      ~ "Other, not Hispanic" 
  ),
  daytype_regional="DAY",           # Only weekday service for ACE
  strata_regional="EVE",             # Trains surveyed were only evening trains (because most people ride round trip and only are surveyed once)
  purpose_regional = recode(income,
                           "1" = "Under $50,000",
                           "2" = "Under $50,000",
                           "3" = "Under $50,000",
                           "4" = "Under $50,000",
                           "5" = "$50,000 to $99,999",
                           "6" = "$50,000 to $99,999",
                           "7" = "$100,000 to $149,999",
                           "8" = "$150,000 to $199,999",
                           "9" = "$200,000 and above",
                           .missing = "Missing"
  )

  
  
                                


  
  
