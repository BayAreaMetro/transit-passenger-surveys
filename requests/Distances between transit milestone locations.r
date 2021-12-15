# Distance between transit milestone locations.r
# Calculate crow-fly ("Haversine") distance between transit milestone locations in miles

# Import Libraries

suppressMessages(library(tidyverse))
library(geosphere)

# Set radius of the earth for Haversine distance calculation
# https://www.space.com/17638-how-big-is-earth.html
# Distance is calculated in miles (3963.20 mi.)
# Alternate distance in meters would be 6378137 m. 

radius_miles <- 3963.2

# Input standardized survey file

survey_in <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights_lat_lon_2021-12-14.Rdata"
load (survey_in)

# Set working directory for file output

wd <- "M:/Data/OnBoard/Bespoke/Milestone Distance Calculations"
setwd(wd)

# Convert coordinate data to numeric format from character
# Assign NA if out of range lat value (appears in only one field for a few records)

temp <- TPS %>% 
  mutate_at(
c("orig_lon",
  "orig_lat",
  "first_board_lon",
  "first_board_lat",
  "survey_board_lon",
  "survey_board_lat",
  "survey_alight_lon",
  "survey_alight_lat",
  "last_alight_lon",
  "last_alight_lat",
  "dest_lon",
  "dest_lat"),as.numeric) %>% 
  mutate(
    first_board_lat=if_else(first_board_lat < -90,NA_real_,first_board_lat)
  )


# Calculate distances
# Origin to destination
# Origin to first boarding
# Origin to survey vehicle boarding
# Survey vehicle alighting to destination
# Last alighting to destination

TPS_distance = temp %>% 
  rowwise() %>% 
  mutate(orig_dest_dist          = distHaversine(c(orig_lon,orig_lat),c(dest_lon,dest_lat),r=radius_miles),
         orig_firstboard_dist    = distHaversine(c(orig_lon,orig_lat),c(first_board_lon,first_board_lat),r=radius_miles),
         orig_surveyboard_dist   = distHaversine(c(orig_lon,orig_lat),c(survey_board_lon,survey_board_lat),r=radius_miles),
         survey_alight_dest_dist = distHaversine(c(survey_alight_lon,survey_alight_lat),c(dest_lon,dest_lat),r=radius_miles),
         last_alight_dest_dist   = distHaversine(c(last_alight_lon,last_alight_lat),c(dest_lon,dest_lat),r=radius_miles),
         )

# Write out file

write.csv(TPS_distance,file = "Transit Passenger Survey with lat lon and distances 121421.csv",row.names = F)
