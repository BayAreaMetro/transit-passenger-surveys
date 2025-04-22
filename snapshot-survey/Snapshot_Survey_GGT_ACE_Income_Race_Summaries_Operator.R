# Snapshot_Survey_Income_Race_Summaries_Operator.R
# Calculate income and race summaries by operator

"Use the following datasets to determine income and race/ethnicity for transit by operator:
1. Snapshot Survey 2023/2024
2. Golden Gate Transit Survey 2023 (completed by the operator, from 'preprocessed' version that includes MTC expansion weights)
3. ACE 2023 (completed by the operator, from 'preprocessed' version that includes MTC expansion weights)
"

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set file directories for input and output, bring in Snapshot Survey data

userprofile    <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir1       <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
box_dir2       <- file.path(box_dir1, "Snapshot Survey", "Data","mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")
output_dir     <- file.path(userprofile,"Box","Plan Bay Area 2050+","Performance and Equity","Equity Analysis","Investment Analysis")


snapshot <- read_excel(box_dir2, sheet = "data file")

# Set locations for Golden Gate Transit and ACE "preprocessed" data files, bring in data

goldengate_in <- "M:/Data/OnBoard/Data and Reports/Golden Gate Transit/2023/GoldenGate_Transit_Ferry_preprocessed.csv"
ace_in        <- "M:/Data/OnBoard/Data and Reports/ACE/2023/ACE_Onboard_preprocessed.csv"

goldengate    <- read.csv(goldengate_in)
ace           <- read.csv(ace_in)

# Summarize Snapshot Survey data

race_income_snapshot <- snapshot %>%
  mutate(race=case_when(
    Q19_1=="B" & if_all(Q19_2:Q19_4, is.na)             ~ "Missing",
    if_any(Q19_1:Q19_4, ~.== 4)                         ~ "Hispanic",
    Q19_1=="1" & if_all(Q19_2:Q19_4, is.na)             ~ "Black, not Hispanic",
    Q19_1=="2" & if_all(Q19_2:Q19_4, is.na)             ~ "Other, not Hispanic",
    Q19_1=="3" & if_all(Q19_2:Q19_4, is.na)             ~ "Asian, not Hispanic",
    Q19_1=="5" & if_all(Q19_2:Q19_4, is.na)             ~ "Other, not Hispanic",
    Q19_1=="6" & if_all(Q19_2:Q19_4, is.na)             ~ "White, not Hispanic",
    Q19_1 %in% c("7","9") & if_all(Q19_2:Q19_4, is.na)  ~ "Other, not Hispanic",
    if_any(Q19_2:Q19_4, ~ !is.na(.))                    ~ "Other, not Hispanic",
    TRUE                                                ~ "Miscoded"
  ),
  income=recode(Q22,
                "1"="Under $15,000",
                "2"="$15,000 to $29,999",
                "3"="$30,000 to $39,999",
                "4"="$40,000 to $49,999",
                "5"="$50,000 to $59,999",
                "6"="$60,000 to $69,999",
                "7"="$70,000 to $79,999",
                "8"="$80,000 to $99,999",
                "9"="$100,000 to $149,999",
                "10"="$150,000 to $199,999",
                "11"="$200,000 and above",
                "B"="Missing",
                "M"="Multiple responses"
                )) %>% 
  relocate(race,.after = Q19_4) %>% 
  relocate(income,.after = Q22)

# Summarize income and race by operator for weekdays

income_summary_snapshot <- race_income_snapshot %>% 
  filter(Daytype=="DAY") %>% 
  group_by(System,income) %>% 
  summarize(total=sum(Weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = income,values_from = total) %>% 
  select("System", "Under $15,000","$15,000 to $29,999", "$30,000 to $39,999", "$40,000 to $49,999", 
         "$50,000 to $59,999", "$60,000 to $69,999", "$70,000 to $79,999", 
         "$80,000 to $99,999", "$100,000 to $149,999", "$150,000 to $199,999", 
         "$200,000 and above",  "Missing", "Multiple responses")

race_summary_snapshot <- race_income_snapshot %>% 
  filter(Daytype=="DAY") %>% 
  group_by(System,race) %>% 
  summarize(total=sum(Weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = race,values_from = total) %>% 
  select("System","White, not Hispanic","Black, not Hispanic","Asian, not Hispanic","Other, not Hispanic","Hispanic", 
         "Missing" )


# Golden Gate Transit summaries for income and race

income_summary_goldengate <- goldengate %>% 
  filter(Strata %in% c("AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "AM OFF")) %>% 
  mutate(income=recode(household_income,
                       "1"="Under $10,000",
                       "2"="$10,000 to $24,999",
                       "3"="$25,000 to $34,999",
                       "4"="$35,000 to $49,999",
                       "5"="$50,000 to $74,999",
                       "6"="$75,000 to $99,999",
                       "7"="$100,000 to $149,999",
                       "8"="$150,000 to $199,999",
                       "9"="$200,000 and above",
                       "0"="Missing",
                       "10"="Multiple responses"
  )) %>% 
  group_by(income) %>% 
  summarize(total=sum(weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = income,values_from = total) %>% 
  mutate("System"="Golden Gate Transit") %>% 
  relocate(System,.before = everything()) %>% 
  select(System,"Under $10,000","$10,000 to $24,999", "$25,000 to $34,999", "$35,000 to $49,999", 
         "$50,000 to $74,999", "$75,000 to $99,999", "$100,000 to $149,999", "$150,000 to $199,999", 
         "$200,000 and above",  "Missing", "Multiple responses")

race_summary_goldengate <- goldengate %>% 
  filter(Strata %in% c("AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "AM OFF")) %>% 
  mutate(race_simple = case_when(
    if_all(hispanic:race_dmy_wht, ~ is.na(.)) ~ "Missing",
    hispanic == 1 ~ "Hispanic",
    if_all(hispanic:race_dmy_wht, ~ . == 0)                                                   ~ "Other, not Hispanic", 
    hispanic == 0 & race_dmy_wht == 1 & if_all(race_dmy_asn:race_dmy_ind, ~ . == 0)           ~ "White, not Hispanic",
    hispanic == 0 & race_dmy_asn == 1 & if_all(race_dmy_blk:race_dmy_wht, ~ . == 0)           ~ "Asian/Pacific Islander, not Hispanic",
    hispanic == 0 & race_dmy_blk == 1 & race_dmy_asn==0 & race_dmy_ind==0 & race_dmy_wht==0   ~ "Black, not Hispanic",
    TRUE                                                                                      ~ "Other, not Hispanic" 
  )) %>% 
  relocate(race_simple,.before = hispanic) %>% 
  group_by(race_simple) %>% 
  summarize(total=sum(weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = race_simple,values_from = total) %>% 
  mutate("System"="Golden Gate Transit") %>% 
  relocate(System,.before = everything()) %>% 
  select(System, "White, not Hispanic","Black, not Hispanic", "Asian/Pacific Islander, not Hispanic", "Other, not Hispanic", "Hispanic")

# ACE summaries for income and race

income_summary_ace <- ace %>% 
  mutate(simple_income = recode(income,
                         "1" = "Under $15,000",
                         "2" = "$15,000 to $24,999",
                         "3" = "$25,000 to $34,999",
                         "4" = "$35,000 to $49,999",
                         "5" = "$50,000 to $74,999",
                         "6" = "$75,000 to $99,999",
                         "7" = "$100,000 to $149,999",
                         "8" = "$150,000 to $199,999",
                         "9" = "$200,000 and above",
                         .missing = "Missing"
  )) %>% 
  relocate(simple_income,.before = income) %>% 
  group_by(simple_income) %>% 
  summarize(total=sum(weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = simple_income,values_from = total) %>% 
  mutate("System"="ACE") %>% 
  relocate(System,.before = everything()) %>% 
  select(System,"Under $15,000","$15,000 to $24,999", "$25,000 to $34,999", "$35,000 to $49,999", 
         "$50,000 to $74,999", "$75,000 to $99,999", "$100,000 to $149,999", "$150,000 to $199,999", 
         "$200,000 and above",  "Missing")

race_summary_ace <- ace %>% 
  mutate(race_simple = case_when(
    hispanic == 1                                                                             ~ "Hispanic",
    hispanic == 2 & if_all(race_1:race_5, ~ . == 0)                                           ~ "Other, not Hispanic", 
    hispanic == 2 & race_4==1 & race_1==0 & race_2==0 & race_3==0 & race_5==0                 ~ "White, not Hispanic",
    hispanic == 2 & race_1==1 & if_all(race_2:race_5, ~ . == 0)                               ~ "Black, not Hispanic",
    hispanic == 2 & race_2==1 & race_1==0 & race_3==0 & race_4==0 & race_5==0                 ~ "Other, not Hispanic",
    hispanic == 2 & race_3==1 & race_1==0 & race_2==0 & race_4==0 & race_5==0                 ~ "Asian, not Hispanic",
    hispanic == 2 & race_5==1 & if_all(race_1:race_4, ~ . == 0)                               ~ "Pacific Islander, not Hispanic",
    TRUE                                                                                      ~ "Other, not Hispanic" 
  )) %>% 
  relocate(race_simple,.before = hispanic) %>% 
  group_by(race_simple) %>% 
  summarize(total=sum(weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = race_simple,values_from = total) %>% 
  mutate("System"="ACE") %>% 
  relocate(System,.before = everything()) %>% 
  select(System, "White, not Hispanic","Black, not Hispanic", "Asian, not Hispanic", "Pacific Islander, not Hispanic", "Other, not Hispanic", "Hispanic")



# Output files

write.csv(income_summary_snapshot,file.path(output_dir,"Snapshot_Income_Summary.csv"),row.names=F)
write.csv(race_summary_snapshot,file.path(output_dir,"Snapshot_Race_Summary.csv"),row.names=F)

write.csv(income_summary_goldengate,file.path(output_dir,"GoldenGate_Income_Summary.csv"),row.names=F)
write.csv(race_summary_goldengate,file.path(output_dir,"GoldenGate_Race_Summary.csv"),row.names=F)

write.csv(income_summary_ace,file.path(output_dir,"ACE_Income_Summary.csv"),row.names=F)
write.csv(race_summary_ace,file.path(output_dir,"ACE_Race_Summary.csv"),row.names=F)



