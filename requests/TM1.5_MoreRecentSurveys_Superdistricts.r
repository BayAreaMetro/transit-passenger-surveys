# TM1.5_MoreRecentSurveys_Superdistricts.r
# Script to join Marin Transit, All SOlano Operators, and WestCAT to superdistrict
# Summarize ridership by boarding-to-alighting superdistrict combination
# SI
# February 1, 2019


# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(dplyr))

# Set up input and output directories

wd <- "M:/Data/Requests/Binny Paul/"
setwd(wd)

sd_file            <- "M:/Data/GIS layers/TM1_taz/taz1454_SUPERD_dissolve.shp"
sd_name_file       <- "M:/Data/GIS layers/TM1_taz/superdistrict_names.csv"
rail_stations_file <- "M:/Data/GIS layers/Passenger_Rail/Passenger_Rail_Stations_2018.shp"
marin              <- "M:/Data/OnBoard/Data and Reports/Marin Transit/As CSV/Rev_marin transit_data file_weighted to ridership_standardized_011518.csv"
solano             <- "M:/Data/OnBoard/Data and Reports/Solano County/As CSV/All Solano Data.csv"
westcat            <- "M:/Data/OnBoard/Data and Reports/WestCAT/As CSV/WestCAT - Raw Survey Data.csv"
export             <- "MoreRecentSurveys_Superdistrict_Exchanges.csv"

# Bring in superdistrict shape file and select superdistrict and geometry columns

sd_name <- read.csv(sd_name_file,stringsAsFactors = FALSE)

superdistrict <- st_read(sd_file) %>%
  select(SUPERD,geometry) %>%
  left_join(.,sd_name,by="SUPERD")                       # Join superdistrict name

  
# Read agency data, filter weekday data only, apply operator names where they don't exist, rename variables to match

marin_data   <- read.csv(marin, stringsAsFactors = FALSE) %>% mutate(
  Agency="Marin Transit"
) %>%
  filter(weekpart=="WD") %>%
  select(ID=id,Agency,routeboard_lat,routeboard_long,routealight_lat,routealight_long,weight)
  

solano_data  <- read.csv(solano, stringsAsFactors = FALSE) %>%
  filter(Day=="Weekday") %>%
  select(ID,Agency,routeboard_lat=Survey_route_boarding_lat,routeboard_long=Survey_route_boarding_lon,
         routealight_lat=Survey_route_alighting_lat,routealight_long=Survey_route_alighting_lon,weight=Weight)

westcat_data <- read.csv(westcat, stringsAsFactors = FALSE) %>% mutate(
  Agency="WestCAT"
) %>%
  filter(Day==1) %>%
  select(ID,Agency,routeboard_lat=Survey_route_boarding_lat,routeboard_long=Survey_route_boarding_lon,
         routealight_lat=Survey_route_alighting_lat,routealight_long=Survey_route_alighting_lon,weight=Weight)

all_data <- rbind(marin_data,solano_data,westcat_data)         # Append all data in a single file

  
# Separate origin and destination into two files for geocoding, remove missing data

all_origin <- all_data %>%
  select(ID,Agency, routeboard_lat,routeboard_long) %>%
  filter(!is.na(routeboard_lat))

all_destination <- all_data %>%
  select(ID,Agency, routealight_lat,routealight_long)%>%
  filter(!is.na(routealight_lat))

# Assign projection for origin/destination and then convert projection into what's used in Bay Area - NAD83 / UTM zone 10N

all_origin_space <- st_as_sf(all_origin, coords = c("routeboard_long", "routeboard_lat"), crs = 4326)
all_origin_space <- st_transform(all_origin_space,crs = 26910)

all_destination_space <- st_as_sf(all_destination, coords = c("routealight_long", "routealight_lat"), crs = 4326)
all_destination_space <- st_transform(all_destination_space,crs = 26910)

# Convert superdistrict shape to same project as all origins, now NAD83 / UTM zone 10N

superdistrict <- st_transform(superdistrict,crs = st_crs(all_origin_space))

# Spatially join origin and destination to shapefile

origin <- st_join(all_origin_space,superdistrict, join=st_within,left=TRUE)%>%
  rename(Origin_SUPERD=SUPERD,Origin_Name=SD_NAME)

destination <- st_join(all_destination_space,superdistrict, join=st_within,left=TRUE)%>%
  rename(Destination_SUPERD=SUPERD,Destination_Name=SD_NAME)

# Join origin and destination TAZs to original all_data file, stripped of all other variables
# Remove geometry columns from origin/destination for join

final <- as.data.frame(all_data) %>%
  select(ID,Agency,weight)

origin <- as.data.frame(origin) %>%
  select(-geometry)

destination <- as.data.frame(destination) %>%
  select(-geometry)

final <- left_join(final,origin,by=c("ID","Agency"))
final <- left_join(final,destination, by=c("ID","Agency"))

# Summarize movement by operator and superdistrict combination

output <- final %>%
  group_by(Agency,Origin_SUPERD,Origin_Name,Destination_SUPERD,Destination_Name) %>%
  summarize(freq=n(),total=sum(weight))

# Write out three-column file with BART ID, Origin TAZ, Destination TAZ

write.csv(output,export,row.names = FALSE)

# Amtrak stations

rail_stations <- st_read(rail_stations_file) %>%
  filter(routename=="Capitol Corridor") %>% 
  st_transform(st_crs(superdistrict)) %>%
  st_join(.,superdistrict, join=st_within,left=TRUE)

write.csv (rail_stations,"Amtrak_Stations.csv",row.names = FALSE)


