# Spatial_Join_BiCounty_TPS.r
# Script to join TPS with Bi-County TAZ system
# SI
# June 29, 2022

# Set working directory

wd <- "C:/Users/sisrael/Box/Modeling and Surveys/Share Data/Protected Data/George Naylor at WSP"
setwd(wd)

# Import libraries

library(pacman)
p_load(sf,tidyverse,sp,rgdal,crsuggest)

# Set up input and output directories
TPS_in          <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights_lat_lon_2021-12-14.Rdata"
Shapefile       <- "M:/Data/GIS layers/BiCountyTAZ/2022_06_16/BiCountyModel_TAZs_20220616.shp"
  
# Bring in bicounty shapefile and select TAZs and geometry columns
   
TAZ_shape <- st_read(Shapefile) %>%
  select(BCM_TAZ,geometry) 

# Bring in TPS file, ensuring data in lat/long format is numeric
load(TPS_in)

TPS_working <- TPS %>% 
  mutate_at(.,vars("orig_lon", "orig_lat", "first_board_lon", 
                "first_board_lat", "survey_board_lon", "survey_board_lat", "survey_alight_lon", 
                "survey_alight_lat", "last_alight_lon", "last_alight_lat", "dest_lon", 
                "dest_lat"),~as.numeric(.)) 

## Work through data files by type and append TAZs

# Origin

TPS_o_temp <- TPS_working %>% 
  select(ID,operator,survey_year,orig_lon,orig_lat) %>% 
  filter(!(is.na(orig_lon)),!(is.na(orig_lat))) %>% 
  st_as_sf(., coords = c("orig_lon", "orig_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_o_final <- st_join(TPS_o_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_o_temp)

# First Board

TPS_fb_temp <- TPS_working %>% 
  select(ID,operator,survey_year,first_board_lon,first_board_lat) %>% 
  filter(!(is.na(first_board_lon)),!(is.na(first_board_lat))) %>% 
  st_as_sf(., coords = c("first_board_lon", "first_board_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_fb_final <- st_join(TPS_fb_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(First_Board_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_fb_temp)

# Survey Board

TPS_sb_temp <- TPS_working %>% 
  select(ID,operator,survey_year,survey_board_lon,survey_board_lat) %>% 
  filter(!(is.na(survey_board_lon)),!(is.na(survey_board_lat))) %>% 
  st_as_sf(., coords = c("survey_board_lon", "survey_board_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_sb_final <- st_join(TPS_sb_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Survey_Board_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_sb_temp)

# Survey Alight

TPS_sa_temp <- TPS_working %>% 
  select(ID,operator,survey_year,survey_alight_lon,survey_alight_lat) %>% 
  filter(!(is.na(survey_alight_lon)),!(is.na(survey_alight_lat))) %>% 
  st_as_sf(., coords = c("survey_alight_lon", "survey_alight_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_sa_final <- st_join(TPS_sa_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Survey_Alight_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_sa_temp)

# Last Alight

TPS_la_temp <- TPS_working %>% 
  select(ID,operator,survey_year,last_alight_lon,last_alight_lat) %>% 
  filter(!(is.na(last_alight_lon)),!(is.na(last_alight_lat))) %>% 
  st_as_sf(., coords = c("last_alight_lon", "last_alight_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_la_final <- st_join(TPS_la_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Last_Alight_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_la_temp)

# Destination

TPS_d_temp <- TPS_working %>% 
  select(ID,operator,survey_year,dest_lon,dest_lat) %>% 
  filter(!(is.na(dest_lon)),!(is.na(dest_lat))) %>% 
  st_as_sf(., coords = c("dest_lon", "dest_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_d_final <- st_join(TPS_d_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

rm(TPS_d_temp)

# Join TAZs to TPS dataset

final <- left_join(TPS,TPS_o_final,by=c("ID","operator","survey_year")) %>% 
  left_join(.,TPS_fb_final,by=c("ID","operator","survey_year")) %>% 
  left_join(.,TPS_sb_final,by=c("ID","operator","survey_year")) %>% 
  left_join(.,TPS_sa_final,by=c("ID","operator","survey_year")) %>% 
  left_join(.,TPS_la_final,by=c("ID","operator","survey_year")) %>% 
  left_join(.,TPS_d_final,by=c("ID","operator","survey_year")) %>% 
  select(-orig_lon, -orig_lat, -first_board_lon, -first_board_lat, -survey_board_lon, 
         -survey_board_lat, -survey_alight_lon, -survey_alight_lat, -last_alight_lon, 
         -last_alight_lat, -dest_lon, -dest_lat)
  
# Write out final CSV files

write.csv(final,file.path(wd,"Transit_Passenger_Survey_Bicounty_TAZ_063022.csv"),row.names = FALSE)


