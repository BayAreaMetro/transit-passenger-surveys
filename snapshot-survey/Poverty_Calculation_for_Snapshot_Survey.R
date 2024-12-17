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

snapshot <- read_excel(BOX_dir2, sheet = "data file")

poverty <- snapshot %>% 
  select(CCGID,Q13,Q22,Daytype,Weight) %>% 
  mutate(poverty=case_when(
    Q13 %in% c("B","M") | Q22 %in% c("B","M")           ~ "Missing",
    Q13=="1" & Q22 %in% c("1","2")                      ~ "Poverty",
    Q13=="2" & Q22 %in% c("1","2","3")                  ~ "Poverty",
    Q13=="3" & Q22 %in% c("1","2","3","4")              ~ "Poverty",
    Q13=="4" & Q22 %in% c("1","2","3","4","5")          ~ "Poverty",
    Q13=="5" & Q22 %in% c("1","2","3","4","5","6")      ~ "Poverty",
    Q13=="6" & Q22 %in% c("1","2","3","4","5","6","7")  ~ "Poverty",
    TRUE                                                ~ "Not Poverty"
  )) 

final <- poverty %>% 
  filter(Daytype=="DAY") %>% 
  group_by(poverty) %>% 
  summarize(total=sum(Weight))

print(final)

