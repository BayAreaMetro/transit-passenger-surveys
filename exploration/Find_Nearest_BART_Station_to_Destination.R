library(tidyverse)
library(geosphere)

# Load BART stations data (latitude, longitude)
bart_stations <- read_csv("bart_stations.csv")  # Assumes columns: station_id, lat, lon

# Load passenger trip destinations (latitude, longitude)
passenger_trips <- read_csv("passenger_trips.csv")  # Assumes columns: passenger_id, dest_lat, dest_lon

# Function to find the nearest station
find_nearest_station <- function(lat, lon, stations) {
  stations %>%
    mutate(distance = distHaversine(matrix(c(lon, lat), nrow=1), 
                                    matrix(c(stations$lon, stations$lat), ncol=2))) %>%
    slice_min(distance) %>%
    pull(station_id)
}

# Apply function to each passenger trip
passenger_trips <- passenger_trips %>%
  rowwise() %>%
  mutate(nearest_station = find_nearest_station(dest_lat, dest_lon, bart_stations)) %>%
  ungroup()

# Save results
write_csv(passenger_trips, "passenger_trips_with_nearest_station.csv")
