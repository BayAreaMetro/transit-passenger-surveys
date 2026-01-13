# VTA_Request_Caltrain_Spatial_Aggregation.r
# Script to join VTA TAZs to Caltrain survey data
# SI

# Import libraries

library(sf)
suppressMessages(library(tidyverse))
library(readxl)

# Set up input and output directories

Caltrain_in <- "M:/Data/OnBoard/Data and Reports/Caltrain/2024/2024 Caltrain OD Data (sent 11.7.2024).xlsx"
VTA_TAZ_in     <- "M:/Data/Requests/Louisa Leung/Caltrain Survey Data/VTATAZ_CCAG/VTATAZ.shp"

output_location <-"M:/Data/Requests/Louisa Leung/Caltrain Survey Data/"

# Bring in shapefiles and select TAZs and geometry columns

TAZ <- st_read(VTA_TAZ_in) %>%
  select(TAZ,geometry)

# Bring in operator file, ensuring lat/long format is numeric

Caltrain <- read_xlsx(Caltrain_in,sheet = "Data") %>% mutate(
  origin_lat = as.numeric(origin_lat),
  origin_lon = as.numeric(origin_lon),
  destination_lat = as.numeric(destination_lat),
  destination_lon = as.numeric(destination_lon),
  home_address_lat = as.numeric(home_address_lat),
  home__address_lon = as.numeric(home_address_lon),
  work_address_lat = as.numeric(work_address_lat),
  work_address_lon = as.numeric(work_address_lon),
  school_address_lat = as.numeric(school_address_lat),
  school_address_lon = as.numeric(school_address_lon)
  )

# Separate files for aggregation

Caltrain_origin <- Caltrain %>% 
  select(id,origin_lat,origin_lon) %>% 
  filter(!is.na(origin_lat))

Caltrain_destination <- Caltrain %>% 
  select(id,destination_lat,destination_lon) %>% 
  filter(!is.na(destination_lat))

Caltrain_home <- Caltrain %>% 
  select(id,home_address_lat,home_address_lon) %>% 
  filter(!is.na(home_address_lat))

Caltrain_work <- Caltrain %>% 
  select(id,work_address_lat,work_address_lon) %>% 
  filter(!is.na(work_address_lat))

Caltrain_school <- Caltrain %>% 
  select(id,school_address_lat,school_address_lon) %>% 
  filter(!is.na(school_address_lat))


# Convert to projection used for VTA TAZs (EPSG 2227)

Caltrain_origin_space <- st_as_sf(Caltrain_origin, coords = c("origin_lon", "origin_lat"), crs = 4326)
Caltrain_origin_space <- st_transform(Caltrain_origin_space,crs = st_crs(TAZ))

Caltrain_destination_space <- st_as_sf(Caltrain_destination, coords = c("destination_lon", "destination_lat"), crs = 4326)
Caltrain_destination_space <- st_transform(Caltrain_destination_space,crs = st_crs(TAZ))

Caltrain_home_space <- st_as_sf(Caltrain_home, coords = c("home_address_lon", "home_address_lat"), crs = 4326)
Caltrain_home_space <- st_transform(Caltrain_home_space,crs = st_crs(TAZ))

Caltrain_work_space <- st_as_sf(Caltrain_work, coords = c("work_address_lon", "work_address_lat"), crs = 4326)
Caltrain_work_space <- st_transform(Caltrain_work_space,crs = st_crs(TAZ))

Caltrain_school_space <- st_as_sf(Caltrain_school, coords = c("school_address_lon", "school_address_lat"), crs = 4326)
Caltrain_school_space <- st_transform(Caltrain_school_space,crs = st_crs(TAZ))


# Spatially join origin, destination, home, work, and school to shapefile

Caltrain_origin2 <- st_join(Caltrain_origin_space,TAZ, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) 

Caltrain_destination2 <- st_join(Caltrain_destination_space,TAZ, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) 

Caltrain_home2 <- st_join(Caltrain_home_space,TAZ, join=st_within,left=TRUE)%>%
  rename(Home_TAZ=TAZ) 

Caltrain_work2 <- st_join(Caltrain_work_space,TAZ, join=st_within,left=TRUE)%>%
  rename(Work_TAZ=TAZ) 

Caltrain_school2 <- st_join(Caltrain_school_space,TAZ, join=st_within,left=TRUE)%>%
  rename(School_TAZ=TAZ) 

# Remove geometry columns from origin/destination for join

Caltrain_origin2            <- as.data.frame(Caltrain_origin2) %>% select(-geometry)
Caltrain_destination2       <- as.data.frame(Caltrain_destination2) %>% select(-geometry)
Caltrain_home2              <- as.data.frame(Caltrain_home2) %>% select(-geometry)
Caltrain_work2              <- as.data.frame(Caltrain_work2) %>% select(-geometry)
Caltrain_school2            <- as.data.frame(Caltrain_school2) %>% select(-geometry)

# Join TAZs and MAZs to files by operator, remove PII geography

Caltrain2 <- left_join(Caltrain,Caltrain_origin2,by="id") %>% 
  left_join(.,Caltrain_destination2,by="id") %>% 
  left_join(.,Caltrain_home2,by="id") %>% 
  left_join(.,Caltrain_work2,by="id") %>% 
  left_join(.,Caltrain_school2,by="id") %>%
  select(-origin_lat, -origin_lon, -destination_lat, -destination_lon, 
         -home_address_lat, -home_address_lon, -work_address_lat, -work_address_lon, 
         -school_address_lat,-school_address_lon, -home_address_lon,-grep("address",names(.)))

# Write out final CSV files

write.csv(Caltrain2,paste0(output_location,"Caltrain_2024_Aggregated.csv"),row.names = FALSE)



