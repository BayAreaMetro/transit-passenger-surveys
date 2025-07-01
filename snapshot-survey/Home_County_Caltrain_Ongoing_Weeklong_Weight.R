# Home_County_Caltrain_Ongoing_Weeklong_Weight.R
# Calculate county of residence shares for ongoing survey for Caltrain

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)
library(sf)
library(tigris)

# Set tigris options
options(tigris_use_cache = TRUE, tigris_class = "sf")

# Set file directories for input and output

userprofile    <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir1       <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
box_dir2       <- file.path(box_dir1,"Ongoing TPS","Individual Operator Efforts","Caltrain 2024","Caltrain MTC RSG Project Folder")
caltrain_in    <- file.path(box_dir2,"OD Data and Deliverables (Includes Data and Report)","2024 Caltrain OD Data (sent 11.7.2024).xlsx")
output_dir     <- "M:/Data/Requests/Rebecca Long"

# Read in Caltrain data, match home lat/long to county

caltrain <- read_excel(caltrain_in, sheet = "Data with Labels")

points_df <- caltrain %>% 
  select(id,home_address,home_address_lat,home_address_lon)

points_clean <- points_df %>%
  filter(!is.na(home_address_lat) & !is.na(home_address_lon))

# Convert to sf points using WGS84 (EPSG:4326)
points_sf <- st_as_sf(points_clean, coords = c("home_address_lon", "home_address_lat"), crs = 4326)

# Load U.S. counties (includes all states)

counties_sf <- counties(cb = TRUE) %>%
  st_transform(crs = 4326)                  # Ensure CRS matches point data

# Step 4: Spatial join points to counties
points_joined <- st_join(points_sf, counties_sf, left = TRUE) %>% 
  mutate(COUNTRY=if_else(is.na(NAMELSAD),"Outside United States","United States"))

# We'll keep county name and state FIPS
matched_df <- points_joined %>%
  st_drop_geometry() %>%
  select(id, GEOID,NAMELSAD, home_state_fips = STATEFP,COUNTRY)

# Final result with original input + county info
caltrain <- caltrain %>%
  left_join(matched_df, by = "id") %>% 
  mutate(residence_county=case_when(
    GEOID=="06001"                                            ~ "Alameda",
    GEOID=="06013"                                            ~ "Contra Costa",
    GEOID=="06041"                                            ~ "Marin",
    GEOID=="06055"                                            ~ "Napa",
    GEOID=="06075"                                            ~ "San Francisco",
    GEOID=="06081"                                            ~ "San Mateo",
    GEOID=="06085"                                            ~ "Santa Clara",
    GEOID=="06095"                                            ~ "Solano",
    GEOID=="06097"                                            ~ "Sonoma",
    COUNTRY %in% c("United States", "Outside United States")  ~ "Not in Bay Area",
    is.na(COUNTRY)                                            ~ "Missing"
  ))

# Summarize by average weeklong day

caltrain_final <- caltrain %>% 
  group_by(residence_county) %>% 
  summarize(
    unweighted = n(),
    weighted = sum(week_weight) / 7,
    .groups = "drop"
  ) %>%
  mutate(
    share_unweighted = (unweighted / sum(unweighted))*100,
    share_weighted = (weighted / sum(weighted))*100
  )

# Export data

write.csv(caltrain_final,file.path(output_dir,"Caltrain_Residence_County_Ongoing.csv"),row.names=F)
