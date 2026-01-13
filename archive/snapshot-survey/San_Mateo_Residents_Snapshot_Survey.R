# San_Mateo_Residents_Snapshot_Survey.R
# Calculate ridership for San Mateo residents on different operators

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set file directories for input and output

userprofile    <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir1       <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
box_dir2       <- file.path(box_dir1, "Snapshot Survey", "Data","mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")
output_dir     <- "M:/Data/Requests/Raleigh McCoy"

snapshot <- read_excel(box_dir2, sheet = "data file")

# Summarize weekday use by operator for San Mateo Residents

san_mateo <- snapshot %>% 
  filter(BAYSUM=="5",Daytype=="DAY") %>% 
  group_by(System) %>% 
  summarize(total=sum(Weight),.groups = "drop")

# Export data

write.csv(san_mateo,file.path(output_dir,"Snapshot_San_Mateo_Residents_Operators.csv"),row.names=F)
