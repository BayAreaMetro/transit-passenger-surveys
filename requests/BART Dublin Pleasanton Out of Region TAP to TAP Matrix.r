# BART Dublin Pleasanton Out of Region TAP to TAP Matrix.r
# Create a Dublin Pleasanton out of region TAP to TAP Matrix
# SI

# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(tidyverse))

# Set up input and output directories

onboard    <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version"
survey_in  <- file.path(onboard,"TPS_Model_Version_PopulationSim_Weights_lat_lon_2021-12-14.Rdata")
shapefile_in <- "M:/Data/Census/Geography/tl_2017_us_county/tl_2017_us_county_CA.shp"
load(survey_in)

username        <- Sys.getenv("USERNAME")
output_location <-paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/bespoke/Transit_Passenger_Survey/BART XI Matrix")
tap_in          <-file.path(output_location,"station_tap_attributes.csv")

# Bay counties list

bay_counties <- c("Alameda","Contra Costa", "Marin","Napa","San Francisco","San Mateo","Santa Clara","Solano", "Sonoma")

# Bring in shapefile and select county and geometry columns

county <- st_read(shapefile_in) %>%
  select(county_name=NAME,geometry)

# Bring in TAP equivalency, filter BART TAPS

tap <-read.csv(tap_in,header = T) %>% 
  filter(grepl("H",.$stType))

# MTC and RSG BART names differ slightly. Bring in equivalency

station_name_eq_in  <-  file.path(output_location,"MTC_RSG BART name equivalency.csv")
  
# Subset BART records
# Ensure origin and destination lat/long format is numeric

BART <- TPS %>% 
  filter(operator=="BART") %>% mutate(
  orig_lat = as.numeric(orig_lat),
  orig_lon = as.numeric(orig_lon),
  dest_lat = as.numeric(dest_lat),
  dest_lon = as.numeric(dest_lon)
  )

# CRS = 4326 sets the lat/long coordinates in the WGS1984 geographic survey
# CRS = 2230 sets the projection for NAD 1983 California Zone 6 in US Feet

BART_origin_space <- BART %>% 
  filter(!(is.na(orig_lon)))
BART_origin_space <- st_as_sf(BART_origin_space, coords = c("orig_lon", "orig_lat"), crs = 4326)
BART_origin_space <- st_transform(BART_origin_space,crs = 2230)

# Convert TAZ shape to same project as BART origins, now NAD83 / UTM zone 10N
# Join to county shapefile
# Remove shape column

county_shape <- st_transform(county,crs = st_crs(BART_origin_space))

BART_origin_joiner <- st_join(BART_origin_space,county_shape, join=st_within,left=TRUE)%>%
  rename(origin_county=county_name) %>% 
  select(ID,origin_county) 

BART_origin_joiner$geometry <- NULL

# Now do the same for destination county

BART_dest_space <- BART %>% 
  filter(!(is.na(dest_lon)))
BART_dest_space <- st_as_sf(BART_dest_space, coords = c("dest_lon", "dest_lat"), crs = 4326)
BART_dest_space <- st_transform(BART_dest_space,crs = 2230)

BART_dest_joiner <- st_join(BART_dest_space,county_shape, join=st_within,left=TRUE)%>%
  rename(destination_county=county_name) %>% 
  select(ID,destination_county) 

BART_dest_joiner$geometry <- NULL

# Join back to BART file

BART <- BART %>% 
  left_join(.,BART_origin_joiner,by="ID") %>% 
  left_join(.,BART_dest_joiner,by="ID")

# Now join TAP file to boarding and alighting stations
# Bring in MTC RSG BART station name equivalency
# Create xi and ix matrices

station_name_eq <- read.csv(station_name_eq_in,header = TRUE) %>% 
  left_join(.,tap,by=c("RSG_BART_list"="stName")) %>% 
  select(BART_station=MTC_BART_list,tap)

xi_Dublin <- BART %>% 
  filter(onoff_enter_station=="Dublin/Pleasanton") %>% 
  left_join(.,station_name_eq,by=c("onoff_enter_station"="BART_station")) %>% 
  rename(RSG_board_tap=tap) %>% 
  left_join(.,station_name_eq,by=c("onoff_exit_station"="BART_station")) %>%
  rename(RSG_alight_tap=tap) %>% 
  filter(!(origin_county %in% bay_counties))

ix_Dublin <- BART %>% 
  filter(onoff_exit_station=="Dublin/Pleasanton") %>% 
  left_join(.,station_name_eq,by=c("onoff_exit_station"="BART_station")) %>% 
  rename(RSG_alight_tap=tap) %>% 
  left_join(.,station_name_eq,by=c("onoff_enter_station"="BART_station")) %>%
  rename(RSG_board_tap=tap) %>% 
  filter(!(destination_county %in% bay_counties))

xi_Dublin_sum <- xi_Dublin %>% 
  group_by(day_part,RSG_board_tap,RSG_alight_tap,origin_county,
           destination_county) %>% 
  summarize(boardings=sum(final_boardWeight_2015)) %>% 
  ungroup()

ix_Dublin_sum <- ix_Dublin %>% 
  group_by(day_part,RSG_board_tap,RSG_alight_tap,origin_county,
           destination_county) %>% 
  summarize(boardings=sum(final_boardWeight_2015)) %>% 
  ungroup()


# Write out final CSV files

write.csv(xi_Dublin_sum,paste0(output_location,"BART Dublin Pleasanton External to Internal Tap to Tap.csv"),row.names = FALSE)
write.csv(ix_Dublin_sum,paste0(output_location,"BART Dublin Pleasanton Internal to External Tap to Tap.csv"),row.names = FALSE)
