# Spatial_Join_Sonoma_County.r
# Script to join Sonoma County origins and destinations to SCTA geography
# SI
# October 14, 2019

# Set working directory

wd <- "M:/Data/Requests/Chris Barney/Output Files/"
setwd(wd)

# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(dplyr))

# Set up input and output directories
Onboard       <- "M:/Data/OnBoard/Data and Reports/"
GGT_in        <- paste0(Onboard,"Golden Gate Transit/2018/As CSV/20180907_OD_GoldenGate_WEEKDAY_Submitted NO POUND NO SINGLE QUOTE.csv")
Petaluma_in   <- paste0(Onboard,"Petaluma/2018/As CSV/20180530_OD_Petaluma_Submittal_FINAL NO POUND NO SINGLE QUOTE.csv")
SMART_in      <- paste0(Onboard,"SMART/As CSV/SMART Standardized Final Data NO POUND NO SINGLE QUOTE.csv")
Sonoma_in     <- paste0(Onboard,"Sonoma County/2018/As CSV/sc transit_data file_final_spring 2018 NO POUND NO SINGLE QUOTE.csv")  
SRCB_in       <- paste0(Onboard,"Santa Rosa CityBus/2018/As CSV/20180522_OD_SantaRosa_Submittal_FINAL NO POUND NO SINGLE QUOTE.csv")  

Shapefile    <- "M:/Data/Requests/Chris Barney/TAZ_2019rev/TAZ_2019rev.shp"

# Bring in SCTA shape file and select SCTA TAZs and geometry columns

TAZ <- st_read(Shapefile) %>%
  select(TAZ,geometry)
  
# Bring in operator files, ensuring origin and destination lat/long format is numeric
# GGT

GGT <- read.csv(GGT_in, stringsAsFactors = FALSE) %>% mutate(
  final_orig_lat = as.numeric(final_orig_lat),
  final_orig_lon = as.numeric(final_orig_lon),
  final_dest_lat = as.numeric(final_dest_lat),
  final_dest_lon = as.numeric(final_dest_lon)
  )

# Petaluma

Petaluma <- read.csv(Petaluma_in, stringsAsFactors = FALSE) %>% mutate(
  final_origin_lat = as.numeric(final_origin_lat),
  final_origin_lon = as.numeric(final_origin_lon),
  final_destin_lat = as.numeric(final_destin_lat),
  final_destin_lon = as.numeric(final_destin_lon)
)

# SMART

SMART <- read.csv(SMART_in, stringsAsFactors = FALSE) %>% mutate(
  origlat = as.numeric(origlat),
  origlon = as.numeric(origlon),
  endlat = as.numeric(endlat),
  endlon = as.numeric(endlon)
)

# Sonoma

Sonoma <- read.csv(Sonoma_in, stringsAsFactors = FALSE) %>% mutate(
  orig_lat = as.numeric(orig_lat),
  orig_lon = as.numeric(orig_lon),
  ENDLAT = as.numeric(ENDLAT),
  ENDLON = as.numeric(ENDLON)
)

# SRCB

SRCB <- read.csv(SRCB_in, stringsAsFactors = FALSE) %>% mutate(
  final_origin_lat = as.numeric(final_origin_lat),
  final_origin_lon = as.numeric(final_origin_lon),
  final_destin_lat = as.numeric(final_destin_lat),
  final_destin_lon = as.numeric(final_destin_lon)
)

# Separate origin and destination into two files, remove missing data

#GGT

GGT_origin <- GGT %>%
  select(id,final_orig_lat,final_orig_lon) %>%
  filter(!is.na(final_orig_lat))

GGT_destination <- GGT %>%
  select(id,final_dest_lat,final_dest_lon)%>%
  filter(!is.na(final_dest_lat))

#Petaluma

Petaluma_origin <- Petaluma %>%
  select(id,final_origin_lat,final_origin_lon) %>%
  filter(!is.na(final_origin_lat))

Petaluma_destination <- Petaluma %>%
  select(id,final_destin_lat,final_destin_lon)%>%
  filter(!is.na(final_destin_lat))

#SMART

SMART_origin <- SMART %>%
  select(CCGID,origlat,origlon) %>%
  filter(!is.na(origlat))

SMART_destination <- SMART %>%
  select(CCGID,endlat,endlon)%>%
  filter(!is.na(endlat))

#Sonoma

Sonoma_origin <- Sonoma %>%
  select(CCGID,orig_lat,orig_lon) %>%
  filter(!is.na(orig_lat))

Sonoma_destination <- Sonoma %>%
  select(CCGID,ENDLAT,ENDLON)%>%
  filter(!is.na(ENDLAT))

#SRCB

SRCB_origin <- SRCB %>%
  select(id,final_origin_lat,final_origin_lon) %>%
  filter(!is.na(final_origin_lat))

SRCB_destination <- SRCB %>%
  select(id,final_destin_lat,final_destin_lon)%>%
  filter(!is.na(final_destin_lat))

# Assign projection for origin/destination and then convert projection into what's used in Bay Area - NAD83 / UTM zone 10N

#GGT

GGT_origin_space <- st_as_sf(GGT_origin, coords = c("final_orig_lon", "final_orig_lat"), crs = 4326)
GGT_origin_space <- st_transform(GGT_origin_space,crs = 26910)

GGT_destination_space <- st_as_sf(GGT_destination, coords = c("final_dest_lon", "final_dest_lat"), crs = 4326)
GGT_destination_space <- st_transform(GGT_destination_space,crs = 26910)

# Petaluma

Petaluma_origin_space <- st_as_sf(Petaluma_origin, coords = c("final_origin_lon", "final_origin_lat"), crs = 4326)
Petaluma_origin_space <- st_transform(Petaluma_origin_space,crs = 26910)

Petaluma_destination_space <- st_as_sf(Petaluma_destination, coords = c("final_destin_lon", "final_destin_lat"), crs = 4326)
Petaluma_destination_space <- st_transform(Petaluma_destination_space,crs = 26910)

# SMART

SMART_origin_space <- st_as_sf(SMART_origin, coords = c("origlon", "origlat"), crs = 4326)
SMART_origin_space <- st_transform(SMART_origin_space,crs = 26910)

SMART_destination_space <- st_as_sf(SMART_destination, coords = c("endlon", "endlat"), crs = 4326)
SMART_destination_space <- st_transform(SMART_destination_space,crs = 26910)

# Sonoma

Sonoma_origin_space <- st_as_sf(Sonoma_origin, coords = c("orig_lon", "orig_lat"), crs = 4326)
Sonoma_origin_space <- st_transform(Sonoma_origin_space,crs = 26910)

Sonoma_destination_space <- st_as_sf(Sonoma_destination, coords = c("ENDLON", "ENDLAT"), crs = 4326)
Sonoma_destination_space <- st_transform(Sonoma_destination_space,crs = 26910)

# SRCB

SRCB_origin_space <- st_as_sf(SRCB_origin, coords = c("final_origin_lon", "final_origin_lat"), crs = 4326)
SRCB_origin_space <- st_transform(SRCB_origin_space,crs = 26910)

SRCB_destination_space <- st_as_sf(SRCB_destination, coords = c("final_destin_lon", "final_destin_lat"), crs = 4326)
SRCB_destination_space <- st_transform(SRCB_destination_space,crs = 26910)

# Convert TAZ shape to same project as GGT (and all successive transit files)

TAZ_shape <- st_transform(TAZ,crs = st_crs(GGT_origin_space))

# Spatially join origin and destination to shapefile

# GGT

GGT_origin2 <- st_join(GGT_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) 

GGT_destination2 <- st_join(GGT_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
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

