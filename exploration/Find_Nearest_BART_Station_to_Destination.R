library(tidyverse)
library(geosphere)
library(readxl)

# Load BART stations data (latitude, longitude)
bart_stations <- read_csv("M:/Data/GIS layers/Transit_Stops/BART/BART_2024_Stops.csv")  # Assumes columns: stop_name, lat, lon

# Load passenger trip destinations (latitude, longitude)
userprofile     <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_dir         <- file.path(userprofile, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys", "Ongoing TPS", "Individual Operator Efforts")
bart_in         <- file.path(box_dir, "BART 2024", "BART MTC ETC RSG Project Folder", "BART DATA - Final SAS","BART SAS Raw Data Output thru June 20 2024.xlsx")
passenger_trips <- read_xlsx(bart_in,sheet = "SAS Raw Thru June 20")  # Assumes columns: passenger_id, dest_lat, dest_lon

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

trial <- head(passenger_trips,n=3000)

# Apply function to each passenger trip
trial2 <- trial %>%
  rowwise() %>%
  mutate(nearest_station = find_nearest_station(DESTIN_ADDRESS_LAT_, DESTIN_ADDRESS_LONG_, bart_stations)) %>%
  ungroup() %>% 
  relocate(EXIT_STATION,.after = nearest_station)

# Save results
write_csv(passenger_trips, "passenger_trips_with_nearest_station.csv")



