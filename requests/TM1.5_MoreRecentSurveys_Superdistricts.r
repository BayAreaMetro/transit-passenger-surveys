# TM1.5_MoreRecentSurveys_Superdistricts.r
# Script to join BART origins and destinations to VTA geography
# SI
# February 27, 2018


# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(dplyr))

# Set up input and output directories

F_BOARD_TO_BE_GEOCODED   = "M:/Data/Requests/Louisa Leung/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight NO POUND OR SINGLE QUOTE.csv"
F_SPATIAL_GEOCODED = "M:/Data/Requests/Louisa Leung/BART_VTA_Geography.csv"

# Bring in VTA shape file and select VTA TAZs and geometry columns

TAZ <- st_read("M:/Data/Requests/Louisa Leung/TAZ/VTATaz.shp") %>%
  select(TAZ,geometry)
  
# Read in BART origin and destination lat/long, ensure format is numeric

BART <- read.csv(F_BOARD_TO_BE_GEOCODED, stringsAsFactors = FALSE) %>%
  select(ID,OR_ADDRESS_LAT,OR_ADDRESS_LONG,DE_ADDRESS_LAT,DE_ADDRESS_LONG) %>% mutate(
  OR_ADDRESS_LAT = as.numeric(OR_ADDRESS_LAT),
  OR_ADDRESS_LONG = as.numeric(OR_ADDRESS_LONG),
  DE_ADDRESS_LAT = as.numeric(DE_ADDRESS_LAT),
  DE_ADDRESS_LONG = as.numeric(DE_ADDRESS_LONG)
  )
  
# Separate origin and destination into two files, remove missing data

bart_origin <- BART %>%
  select(ID,OR_ADDRESS_LAT,OR_ADDRESS_LONG) %>%
  filter(!is.na(OR_ADDRESS_LAT))

bart_destination <- BART %>%
  select(ID,DE_ADDRESS_LAT,DE_ADDRESS_LONG)%>%
  filter(!is.na(DE_ADDRESS_LAT))

# Assign projection for origin/destination and then convert projection into what's used in Bay Area - NAD83 / UTM zone 10N

bart_origin_space <- st_as_sf(bart_origin, coords = c("OR_ADDRESS_LONG", "OR_ADDRESS_LAT"), crs = 4326)
bart_origin_space <- st_transform(bart_origin_space,crs = 26910)

bart_destination_space <- st_as_sf(bart_destination, coords = c("DE_ADDRESS_LONG", "DE_ADDRESS_LAT"), crs = 4326)
bart_destination_space <- st_transform(bart_destination_space,crs = 26910)

# Convert TAZ shape to same project as BART origins, now NAD83 / UTM zone 10N

TAZ_shape <- st_transform(TAZ,crs = st_crs(bart_origin_space))

# Spatially join origin and destination to shapefile

origin <- st_join(bart_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ)

destination <- st_join(bart_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ)

# Join origin and destination TAZs to original BART file, stripped of all other variables
# Remove geometry columns from origin/destination for join

final <- as.data.frame(BART) %>%
  select(ID)

origin <- as.data.frame(origin) %>%
  select(-geometry)

destination <- as.data.frame(destination) %>%
  select(-geometry)

final <- left_join(final,origin,by="ID")
final <- left_join(final,destination, by="ID")

# Write out three-column file with BART ID, Origin TAZ, Destination TAZ

write.csv(final,F_SPATIAL_GEOCODED,row.names = FALSE)

