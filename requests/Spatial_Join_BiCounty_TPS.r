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

# First Board

TPS_fb_temp <- TPS_working %>% 
  select(ID,operator,survey_year,first_board_lon,first_board_lat) %>% 
  filter(!(is.na(first_board_lon)),!(is.na(first_board_lat))) %>% 
  st_as_sf(., coords = c("first_board_lon", "first_board_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_fb_final <- st_join(TPS_fb_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(First_Board_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

# Survey Board

TPS_s_temp <- TPS_working %>% 
  select(ID,operator,survey_year,survey_board_lon,survey_board_lat) %>% 
  filter(!(is.na(first_board_lon)),!(is.na(first_board_lat))) %>% 
  st_as_sf(., coords = c("first_board_lon", "first_board_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_s_final <- st_join(TPS_fb_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(First_Board_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

# Destination

TPS_d_temp <- TPS_working %>% 
  select(ID,operator,survey_year,dest_lon,dest_lat) %>% 
  filter(!(is.na(dest_lon)),!(is.na(dest_lat))) %>% 
  st_as_sf(., coords = c("dest_lon", "dest_lat"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

TPS_o_final <- st_join(TPS_o_temp,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

# Spatially join origin and destination to shapefile
  
CHTS_places_o <- st_join(CHTS_places_o,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(O_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)



# Join origin and destination datasets and only include those with TAZ values greater than zero

CHTS_places_bicounty <- full_join(CHTS_places_o,CHTS_places_d,by=c("SAMPN", "PERNO", "PLANO")) %>% 
  filter(O_BCM_TAZ>0 | D_BCM_TAZ>0)

## Now HH file

CHTS_hhs <- read.csv(CHTS_hhs_in, stringsAsFactors = FALSE) %>% 
  mutate_at(.,c("HXCORD","HYCORD"),~as.numeric(.)) %>% 
  select("SAMPN", "HXCORD", "HYCORD", "HCTFIP") %>% 
  filter(!(is.na(HXCORD)),!(is.na(HYCORD)))

CHTS_hhs_trans <- CHTS_hhs %>% 
  st_as_sf(., coords = c("HXCORD", "HYCORD"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

CHTS_hhs_final <- st_join(CHTS_hhs_trans,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(H_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry) %>% 
  filter(H_BCM_TAZ>0)

## Now the person file for work and school

CHTS_person_temp <- read.csv(CHTS_persons_in, stringsAsFactors = FALSE) %>% 
  mutate_at(.,c("WXCORD", "WYCORD","SXCORD","SYCORD"),~as.numeric(.)) %>% 
  select("SAMPN", "PERNO", "WXCORD", "WYCORD","SXCORD","SYCORD")

CHTS_person_work <- CHTS_person_temp %>% 
  select(-SXCORD,-SYCORD) %>% 
  filter(!(is.na(WXCORD)),!(is.na(WYCORD))) %>% 
  st_as_sf(., coords = c("WXCORD", "WYCORD"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

CHTS_person_school <- CHTS_person_temp %>% 
  select(-WXCORD,-WYCORD) %>% 
  filter(!(is.na(SXCORD)),!(is.na(SYCORD))) %>% 
  st_as_sf(., coords = c("SXCORD", "SYCORD"), crs = 4326) %>% 
  st_transform(., crs=st_crs(TAZ_shape))

CHTS_work <- st_join(CHTS_person_work,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

CHTS_school <- st_join(CHTS_person_school,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_BCM_TAZ=BCM_TAZ) %>% 
  as.data.frame(.) %>% select(-geometry)

CHTS_person_bicounty <- full_join(CHTS_work,CHTS_school,by=c("SAMPN", "PERNO"))%>% 
  filter(Work_BCM_TAZ>0 | School_BCM_TAZ>0)

# Write out final CSV files

write.csv(CHTS_places_bicounty,file.path(wd,"CHTS_places_bicounty_TAZ.csv"),row.names = FALSE)
write.csv(CHTS_hhs_final,file.path(wd,"CHTS_hhs_bicounty_TAZ.csv"),row.names = FALSE)
write.csv(CHTS_person_bicounty,file.path(wd,"CHTS_person_locations_bicounty_TAZ.csv"),row.names = FALSE)


