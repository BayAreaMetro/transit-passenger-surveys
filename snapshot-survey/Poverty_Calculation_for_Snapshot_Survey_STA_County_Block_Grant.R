# Poverty_calculation_for_Snapshot_Survey.r
# Calculate poverty thresholds for Snapshot Survey records

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set file directories for input and output

USERPROFILE    <- gsub("////","/", Sys.getenv("USERPROFILE"))
BOX_dir1       <- file.path(USERPROFILE, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
BOX_dir2       <- file.path(BOX_dir1, "Snapshot Survey", "Data","mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")

output <- "M:/Data/Requests/Jacki Taylor"

# Bring in snapshot data and perform poverty calculations for 200 poverty level
# Only include operators - AC Transit, BART, LAVTA, and Union City Transit

snapshot <- read_excel(BOX_dir2, sheet = "data file")

poverty <- snapshot %>%
  filter(System %in% c("AC TRANSIT","BART","LAVTA","UNION CITY TRANSIT")) %>% 
  select(CCGID,Q13,Q22,Daytype,Weight,System) %>% 
  mutate(Poverty_Status=case_when(
    Q13 %in% c("B","M")                                 ~ "Missing household size",
    Q22 %in% c("B","M")                                 ~ "Missing household income",
    Q13=="1" & Q22 %in% c("1","2")                      ~ "Below poverty",
    Q13=="2" & Q22 %in% c("1","2","3")                  ~ "Below poverty",
    Q13=="3" & Q22 %in% c("1","2","3","4")              ~ "Below poverty",
    Q13=="4" & Q22 %in% c("1","2","3","4","5")          ~ "Below poverty",
    Q13=="5" & Q22 %in% c("1","2","3","4","5","6")      ~ "Below poverty",
    Q13=="6" & Q22 %in% c("1","2","3","4","5","6","7")  ~ "Below poverty",
    TRUE                                                ~ "Above poverty"
  )) 

final <- poverty %>% 
  filter(Daytype=="DAY") %>% 
  group_by(System,Poverty_Status) %>% 
  summarize(Count=n(),Weighted_Total=sum(Weight),.groups = "drop")

# Output final file

write.csv(final,file.path(output,"Snapshot Survey 2023-2024 Poverty Calculations for ACTC.csv"),row.names = F)

