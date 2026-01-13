# Snapshot_Survey_Ridership_SF_OD.R
# Get non-GGT ridership for large operators
# Get SF share of regional O/D

library(tidyverse)
library(readxl)

# Bring in Snapshot Survey, set up output folder

USERPROFILE    <- gsub("////","/", Sys.getenv("USERPROFILE"))
box_dir1       <- file.path(USERPROFILE, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys","Snapshot Survey", "Data")
snapshot_in    <- file.path(box_dir1,"mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")

snapshot <- read_excel(snapshot_in, sheet = "data file")

output   <-"M:/Data/Requests/Liz Brisson"

# Bring in integrated transit file, keep weekday-only records

regional_in <- "M:/Data/OnBoard/Data and Reports/_data_Standardized/standardized_2024-11-08/survey_standard.RDS"

regional <- readRDS(regional_in) %>% 
  filter(weekpart=="WEEKDAY", survey_name=="Regional Snapshot" |
           (survey_name=="Golden Gate Transit" & survey_year==2023) |
           (survey_name=="ACE" & survey_year==2023),
         !is.na(orig_county_GEOID) & !is.na(dest_county_GEOID))

# Filter to weekday ridership for large operators

snapshot_weekday <- snapshot %>% 
  filter(Daytype== "DAY")

ridership_summary <- snapshot_weekday %>% 
  filter(System != "RIO VISTA/DELTA BREEZE") %>% 
  group_by(System) %>% 
  summarize(Weekday_Ridership=sum(Weight))

write.csv(ridership_summary,file.path(output,"Weekday_Ridership_2023.csv"),row.names = F)

# Summarize 

origin_destination <- regional %>% 
  select(ID,survey_name,survey_year,orig_county_GEOID,dest_county_GEOID,weekpart,weight) %>% 
  mutate(o_d_county = if_else(orig_county_GEOID=="06075" | dest_county_GEOID=="06075","San Francisco","Not San Francisco")) %>% 
  group_by(o_d_county) %>% 
  summarize(Total=sum(weight))

write.csv(origin_destination,file.path(output,"San Francisco_Origin or Destination_2023.csv"),row.names = F)
