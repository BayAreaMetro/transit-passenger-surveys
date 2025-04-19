# Snapshot_Survey_Income_Race_Summaries_Operator.R
# Calculate income and race summaries by operator

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set file directories for input and output

userprofile    <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir1       <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
box_dir2       <- file.path(box_dir1, "Snapshot Survey", "Data","mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")
output_dir     <- file.path(userprofile,"Box","Plan Bay Area 2050+","Performance and Equity","Equity Analysis","Investment Analysis")


snapshot <- read_excel(box_dir2, sheet = "data file")

race_income <- snapshot %>%
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

income_summary <- race_income %>% 
  filter(Daytype=="DAY") %>% 
  group_by(System,income) %>% 
  summarize(total=sum(Weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = income,values_from = total) %>% 
  select("System", "Under $15,000","$15,000 to $29,999", "$30,000 to $39,999", "$40,000 to $49,999", 
         "$50,000 to $59,999", "$60,000 to $69,999", "$70,000 to $79,999", 
         "$80,000 to $99,999", "$100,000 to $149,999", "$150,000 to $199,999", 
         "$200,000 and above",  "Missing", "Multiple responses")

race_summary <- race_income %>% 
  filter(Daytype=="DAY") %>% 
  group_by(System,race) %>% 
  summarize(total=sum(Weight),.groups = "drop") %>% 
  pivot_wider(.,names_from = race,values_from = total) %>% 
  select("System","White, not Hispanic","Black, not Hispanic","Asian, not Hispanic","Other, not Hispanic","Hispanic", 
         "Missing" )


# Output files

write.csv(total_income,file.path(output_dir,"BATS_2023_Total_Income.csv"),row.names=F)
write.csv(roadway_income,file.path(output_dir,"BATS_2023_Roadway_Income.csv"),row.names=F)


