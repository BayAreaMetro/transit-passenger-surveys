library(tidyverse)
library(geosphere)
library(readxl)

# Load BART stations data (latitude, longitude)
bart_stations <- read_csv("M:/Data/GIS layers/Transit_Stops/BART/BART_2024_Stops.csv")  # Assumes columns: stop_name, lat, lon

# Rename one or more station names,as needed

bart_stations <- bart_stations %>% 
  mutate(stop_name=recode(stop_name,
    "Millbrae (Caltrain Transfer Platform)"="Millbrae"
  ))

# Load passenger trip destinations (latitude, longitude)
userprofile     <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir         <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys", "Ongoing TPS", "Individual Operator Efforts")
bart_folder     <- file.path(box_dir, "BART 2024", "BART MTC ETC RSG Project Folder", "001_Working_Data_Files")
passenger_trips <- read_xlsx(file.path(bart_folder,"MTC-BART_OD_Study_SAS_250305_EditedMW_032125_Review1.xlsx"),sheet = "MTC-BART_OD_Study_SAS_250305_Ed") 

find_nearest_station <- function(lat, lon, stations) {
  if (is.na(lat) | is.na(lon)) {
    return(NA)
  }
  
  passenger_matrix <- matrix(c(lon, lat), nrow = 1, ncol = 2)  # Ensure it's a 1-row matrix
  stations_matrix  <- as.matrix(stations[, c("lon", "lat")])  # Ensure it's a 2-column matrix
  
  stations %>%
    mutate(distance = distHaversine(passenger_matrix, stations_matrix)) %>%
    slice_min(distance) %>%
    pull(stop_name)
}

# Apply function to each passenger trip

final <-  passenger_trips %>% 
  mutate(imputed_origin_station = map2_chr(ORIGIN_ADDRESS_LAT,ORIGIN_ADDRESS_LONG, ~find_nearest_station(.x, .y, bart_stations)),
         imputed_destination_station = map2_chr(DESTIN_ADDRESS_LAT, DESTIN_ADDRESS_LONG, ~find_nearest_station(.x, .y, bart_stations))
         ) %>% 
  select(UNIQUE_IDENTIFIER,ORIGIN_ADDRESS_LAT,ORIGIN_ADDRESS_LONG,ENTRY_FNL_NUM,imputed_origin_station,DESTIN_ADDRESS_LAT,DESTIN_ADDRESS_LONG,EXIT_STATION_TEXT,imputed_destination_station)


# Save results
write.csv(final,file=file.path(bart_folder, "imputed_station_locations_SAS_250305_EditedMW_032125.csv"),row.names = F)



