# Spatial_Join_SoCo_Rail_Study_Fehr_Peers.r
# Script to join MAZ and TAZ for a few operators not in the standard dataset
# SI

# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(dplyr))

# Set up input and output directories

Onboard <- "M:/Data/OnBoard/Data and Reports/"
ACE_in  <- paste0(Onboard,"ACE/2019/ACE19_Final Data Add New Route Date Time Columns NO POUND OR SINGLE QUOTE.csv")
UC_in   <- paste0(Onboard,"Union City/2017/Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv")
ACT_in  <- paste0(Onboard,"AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode) NO POUND OR SINGLE QUOTE.csv")


TAZ_in     <- "M:/Data/GIS layers/Travel_Analysis_Zones_(TAZ1454)/Travel Analysis Zones.shp"
MAZ_in     <- paste0(Onboard,"_geocoding Standardized/TM2_Zones/mazs.shp")

username        <- Sys.getenv("USERNAME")
output_location <-paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Protected Data/Fehr_Peers/")

# Bring in shapefiles and select TAZs and geometry columns

TAZ <- st_read(TAZ_in) %>%
  select(TAZ=TAZ1454,geometry)

MAZ <- st_read(MAZ_in) %>%
  select(MAZ=MAZ_ORIGIN,geometry)
  
# Bring in operator files, ensuring origin and destination lat/long format is numeric
# ACE

ACE <- read.csv(ACE_in, stringsAsFactors = FALSE) %>% mutate(
  Origin_lat = as.numeric(Origin_lat),
  Origin_lon = as.numeric(Origin_lon),
  Destination_lat = as.numeric(Destination_lat),
  Destination_lon = as.numeric(Destination_lon),
  Home_lat = as.numeric(Home_lat),
  Home_lon = as.numeric(Home_lon),
  Work_lat = as.numeric(Work_lat),
  Work_lon = as.numeric(Work_lon),
  School_lat = as.numeric(School_lat),
  School_lon = as.numeric(School_lon)
  )

# Union City

UC <- read.csv(UC_in, stringsAsFactors = FALSE) %>% mutate(
  startlat = as.numeric(startlat),
  startlon = as.numeric(startlon),
  endlat = as.numeric(endlat),
  endlon = as.numeric(endlon),
  homelat = as.numeric(homelat),
  homelon = as.numeric(homelon),
  worklat = as.numeric(worklat),
  worklon = as.numeric(worklon),
  school_lat = as.numeric(school_lat),
  school_lon = as.numeric(school_lon)
)

# AC Transit

AC <- read.csv(ACT_in, stringsAsFactors = FALSE) %>% mutate(
  orig_lat = as.numeric(orig_lat),
  orig_lon = as.numeric(orig_lon),
  dest_lat = as.numeric(dest_lat),
  dest_lon = as.numeric(dest_lon),
  home_lat = as.numeric(home_lat),
  home_lon = as.numeric(home_lon),
  workplace_lat = as.numeric(workplace_lat),
  workplace_lon = as.numeric(workplace_lon),
  school_lat = as.numeric(school_lat),
  school_lon = as.numeric(school_lon)
)

# Separate locations into separate files, remove missing data

#ACE

ACE_origin <- ACE %>% 
  select(ID,Origin_lat,Origin_lon) %>% 
  filter(!is.na(Origin_lat))

ACE_destination <- ACE %>% 
  select(ID,Destination_lat,Destination_lon) %>% 
  filter(!is.na(Destination_lat))

ACE_home <- ACE %>% 
  select(ID,Home_lat,Home_lon) %>% 
  filter(!is.na(Home_lat))

ACE_work <- ACE %>% 
  select(ID,Work_lat,Work_lon) %>% 
  filter(!is.na(Work_lat))

ACE_school <- ACE %>% 
  select(ID,School_lat,School_lon) %>% 
  filter(!is.na(School_lat))

#Union City

UC_origin <- UC %>% 
  select(id,startlat, startlon) %>% 
  filter(!is.na(startlat))

UC_destination <- UC %>% 
  select(id,endlat,endlon) %>% 
  filter(!is.na(endlat))

UC_home <- UC %>% 
  select(id,homelat,homelon) %>% 
  filter(!is.na(homelat))

UC_work <- UC %>% 
  select(id,worklat,worklon) %>% 
  filter(!is.na(Work_lat))

UC_school <- UC %>% 
  select(id,school_lat,school_lon) %>% 
  filter(!is.na(school_lat))

#AC Transit

AC_origin <- AC %>% 
  select(id,orig_lat,orig_lon) %>% 
  filter(!is.na(orig_lat))

AC_destination <- AC %>% 
  select(id,dest_lat,dest_lon) %>% 
  filter(!is.na(dest_lat))

AC_home <- AC %>% 
  select(id,home_lat,home_lon) %>% 
  filter(!is.na(home_lat))

AC_work <- AC %>% 
  select(id,workplace_lat,workplace_lon) %>% 
  filter(!is.na(workplace_lat))

AC_school <- AC %>% 
  select(id,school_lat,school_lon) %>% 
  filter(!is.na(school_lat))

# CRS = 4326 sets the lat/long coordinates in the WGS1984 geographic survey
# CRS = 2230 sets the projection for NAD 1983 California Zone 6 in US Feet

#ACE

ACE_origin_space <- st_as_sf(ACE_origin, coords = c("Origin_lon", "Origin_lat"), crs = 4326)
ACE_origin_space <- st_transform(ACE_origin_space,crs = 2230)

ACE_destination_space <- st_as_sf(ACE_destination, coords = c("Destination_lon", "Destination_lat"), crs = 4326)
ACE_destination_space <- st_transform(ACE_destination_space,crs = 2230)

ACE_home_space <- st_as_sf(ACE_home, coords = c("Home_lon", "Home_lat"), crs = 4326)
ACE_home_space <- st_transform(ACE_home_space,crs = 2230)

ACE_work_space <- st_as_sf(ACE_work, coords = c("Work_lon", "Work_lat"), crs = 4326)
ACE_work_space <- st_transform(ACE_work_space,crs = 2230)

ACE_school_space <- st_as_sf(ACE_school, coords = c("School_lon", "School_lat"), crs = 4326)
ACE_school_space <- st_transform(ACE_school_space,crs = 2230)

# Union City

UC_origin_space <- st_as_sf(UC_origin, coords = c("startlon", "startlat"), crs = 4326)
UC_origin_space <- st_transform(UC_origin_space,crs = 2230)

UC_destination_space <- st_as_sf(UC_destination, coords = c("endlon", "endlat"), crs = 4326)
UC_destination_space <- st_transform(UC_destination_space,crs = 2230)

UC_home_space <- st_as_sf(UC_home, coords = c("homelon", "homelat"), crs = 4326)
UC_home_space <- st_transform(UC_home_space,crs = 2230)

UC_work_space <- st_as_sf(UC_work, coords = c("worklon", "worklat"), crs = 4326)
UC_work_space <- st_transform(UC_work_space,crs = 2230)

UC_school_space <- st_as_sf(UC_school, coords = c("school_lon", "school_lat"), crs = 4326)
UC_school_space <- st_transform(UC_school_space,crs = 2230)

# AC Transit

AC_origin_space <- st_as_sf(AC_origin, coords = c("orig_lon", "orig_lat"), crs = 4326)
AC_origin_space <- st_transform(AC_origin_space,crs = 2230)

AC_destination_space <- st_as_sf(AC_destination, coords = c("dest_lon", "dest_lat"), crs = 4326)
AC_destination_space <- st_transform(AC_destination_space,crs = 2230)

AC_home_space <- st_as_sf(AC_home, coords = c("home_lon", "home_lat"), crs = 4326)
AC_home_space <- st_transform(AC_home_space,crs = 2230)

AC_work_space <- st_as_sf(AC_work, coords = c("workplace_lon", "workplace_lat"), crs = 4326)
AC_work_space <- st_transform(AC_work_space,crs = 2230)

AC_school_space <- st_as_sf(AC_school, coords = c("school_lon", "school_lat"), crs = 4326)
AC_school_space <- st_transform(AC_school_space,crs = 2230)

# Convert TAZ shape to same project as GGT (and all successive transit files)

TAZ_shape <- st_transform(TAZ,crs = st_crs(ACE_origin_space))
MAZ_shape <- st_transform(MAZ,crs = st_crs(ACE_origin_space))

# Spatially join origin, destination, home, work, and school to shapefile

# ACE

ACE_origin2 <- st_join(ACE_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_MAZ=MAZ)

ACE_destination2 <- st_join(ACE_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ)

#Petaluma

Petaluma_origin2 <- st_join(Petaluma_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ)

Petaluma_destination2 <- st_join(Petaluma_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) 

# SMART

SMART_origin2 <- st_join(SMART_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ)

SMART_destination2 <- st_join(SMART_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) 

# Sonoma

Sonoma_origin2 <- st_join(Sonoma_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ)

Sonoma_destination2 <- st_join(Sonoma_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) 

# SRCB

SRCB_origin2 <- st_join(SRCB_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ)

SRCB_destination2 <- st_join(SRCB_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) 


# Remove geometry columns from origin/destination for join

GGT_origin2              <- as.data.frame(GGT_origin2) %>% select(-geometry)
GGT_destination2         <- as.data.frame(GGT_destination2) %>% select(-geometry)
Petaluma_origin2         <- as.data.frame(Petaluma_origin2) %>% select(-geometry) 
Petaluma_destination2    <- as.data.frame(Petaluma_destination2) %>% select(-geometry)
SMART_origin2            <- as.data.frame(SMART_origin2) %>% select(-geometry)
SMART_destination2       <- as.data.frame(SMART_destination2) %>% select(-geometry)
Sonoma_origin2           <- as.data.frame(Sonoma_origin2) %>% select(-geometry)
Sonoma_destination2      <- as.data.frame(Sonoma_destination2) %>% select(-geometry)
SRCB_origin2             <- as.data.frame(SRCB_origin2) %>% select(-geometry)
SRCB_destination2        <- as.data.frame(SRCB_destination2) %>% select(-geometry)

# Join TAZs to files by operator, remove PII geography

# GGT

GGT2 <- left_join(GGT,GGT_origin2,by="id")
GGT2 <- left_join(GGT2,GGT_destination2, by="id") %>%
  select(-vis_zip,-home_address,-home_city,-home_state,-home_zip,-home_lat,-home_lon,-hotel_address,-hotel_city,
         -hotel_state,-hotel_zip,-hotel_lat,-hotel_lon,-origin_address,-origin_city,-origin_state,-origin_zip,-orig_lat,
         -orig_lon,-change_to_origin_address,-reason_for_change_to_origin_address,-final_origin_address,-final_origin_city,
         -final_origin_state,-final_origin_zip,-final_orig_lat,-final_orig_lon,-origin_dropoff_address,-origin_dropoff_city,
         -origin_dropoff_state,-origin_dropoff_zip,-orign_dropoff_lat,-origin_dropoff_lon,-destin_address,-destin_city,
         -destin_state,-destin_zip,-dest_lat,-dest_lon,-change_to_destin_address,-reason_for_change_to_destin_address,
         -final_destin_address,-final_destin_city,-final_destin_state,-final_destin_zip,-final_dest_lat,-final_dest_lon,
         -destin_dropoff_address,-destin_dropoff_city,-destin_dropoff_state,-destin_dropoff_zip,-orign_dropoff_lat,	
         -destin_dropoff_lon,-your_work_address,-your_work_city,-your_work_state,-your_work_zip,-workplace_lat,
         -workplace_lon,-your_school_address,-your_school_city,-your_school_state,-your_school_zip,-school_lat,-school_lon)

# Petaluma

Petaluma2 <- left_join(Petaluma,Petaluma_origin2,by="id")
Petaluma2 <- left_join(Petaluma2,Petaluma_destination2, by="id") %>%
  select(-vis_zip,-home_address,-home_city,-home_state,-home_zip,-home_lat,-home_lon,-hotel_address,-hotel_city,
         -hotel_state,-hotel_zip,-hotel_lat,-hotel_lon,-origin_address,-origin_city,-origin_state,-origin_zip,
         -orig_lat,-orig_lon,-change_to_origin_address,-reason_for_change_to_origin_address,-final_origin_address,
         -final_origin_city,-final_origin_state,-final_origin_zip,-final_origin_lat,-final_origin_lon,-destin_address,
         -destin_city,-destin_state,-destin_zip,-dest_lat,-dest_lon,-change_to_destin_address,
         -reason_for_change_to_destin_address,-final_destin_address,-final_destin_city,-final_destin_state,
         -final_destin_zip,-final_destin_lat,-final_destin_lon,-your_work_address,-your_work_city,-your_work_state,
         -your_work_zip,-workplace_lat,-workplace_lon,-your_school_address,-your_school_city,-your_school_state,
         -your_school_zip,-school_lat,-school_lon)

# SMART

SMART2 <- left_join(SMART,SMART_origin2,by="CCGID")
SMART2 <- left_join(SMART2,SMART_destination2, by="CCGID") %>% 
  select(-school_name,-college_name,-origlat,-origlon,-endlat,-endlon,-home_lat,-home_lon,-work_lat,-work_lon,
         -school_lat,-school_lon)

# Sonoma

Sonoma2 <- left_join(Sonoma,Sonoma_origin2,by="CCGID")
Sonoma2 <- left_join(Sonoma2,Sonoma_destination2, by="CCGID") %>%
  select(-X,-X.1,-X.2,-SCHOOL_NAME,-COLLEGE_NAME,-orig_lat,-orig_lon,-ENDLAT,-ENDLON,-HOMELAT,-HOMELON,-WORKLAT,
         -WORKLON,-SCHOOL_LAT,-SCHOOL_LON)

# SRCB

SRCB2 <- left_join(SRCB,SRCB_origin2,by="id")
SRCB2 <- left_join(SRCB2,SRCB_destination2, by="id") %>% 
  select(-home_address, -home_city, -home_state, -home_zip, -home_lat, -home_lon, -hotel_address, -hotel_city,-hotel_state,	
         -hotel_zip, -hotel_lat, -hotel_lon, -origin_address, -origin_city, -origin_state, -origin_zip, -orig_lat, -orig_lon,
         -change_to_origin_address, -reason_for_change_to_origin_address, -final_origin_address, -final_origin_city,	
         -final_origin_state, -final_origin_zip, -final_origin_lat, -final_origin_lon, -destin_address, -destin_city,	
         -destin_state, -destin_zip, -dest_lat, -dest_lon, -change_to_destin_address, -reason_for_change_to_destin_address,	
         -final_destin_address, -final_destin_city, -final_destin_state, -final_destin_zip, -final_destin_lat, 
         -final_destin_lon, -your_work_address, -your_work_city, -your_work_state, -your_work_zip, -workplace_lat,
         -workplace_lon, -your_school_address, -your_school_city, -your_school_state, -your_school_zip, -school_lat,
         -school_lon)


# Write out final CSV files

write.csv(GGT2,"Golden Gate Transit.csv",row.names = FALSE)
write.csv(Petaluma2,"Petaluma Transit.csv",row.names = FALSE)
write.csv(SMART2,"SMART.csv",row.names = FALSE)
write.csv(Sonoma2,"Sonoma County Transit.csv",row.names = FALSE)
write.csv(SRCB2,"Santa Rosa CityBus.csv",row.names = FALSE)

