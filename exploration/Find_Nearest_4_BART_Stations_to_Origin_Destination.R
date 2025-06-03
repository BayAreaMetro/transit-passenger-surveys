# Find_Nearest_4_BART_Stations_to_Origin_Destination.R
# Find nearest 4 BART stations to trip origin/destination. If reported station doesn't match then flag as false, otherwise true. Missing if NA values.

library(tidyverse)
library(geosphere)
library(readxl)

# Load BART stations data
bart_stations <- read_csv("M:/Data/GIS layers/Transit_Stops/BART/BART_2024_Stops.csv")

# Standardize station names
bart_stations <- bart_stations %>%
  mutate(stop_name = recode(stop_name,
                            "Millbrae (Caltrain Transfer Platform)" = "Millbrae"
  ))

# Load passenger trip data
userprofile <- gsub("\\\\", "/", Sys.getenv("USERPROFILE"))
box_dir     <- file.path(userprofile, "Box", "Modeling and Surveys", "Surveys", "Transit Passenger Surveys", "Ongoing TPS", "Individual Operator Efforts")
bart_folder <- file.path(box_dir, "BART 2024", "BART MTC ETC RSG Project Folder", "001_Working_Data_Files")
passenger_trips <- read_xlsx(
  file.path(bart_folder, "MTC-BART_OD_Study_SAS_250305_EditedMW_032125_Review1.xlsx"),
  sheet = "MTC-BART_OD_Study_SAS_250305_Ed"
)

# Function to return 4 closest station names
find_nearest_stations <- function(lat, lon, stations, n = 4) {
  if (is.na(lat) | is.na(lon)) {
    return(rep(NA, n))
  }
  
  passenger_matrix <- matrix(c(lon, lat), nrow = 1)
  stations_matrix  <- as.matrix(stations[, c("lon", "lat")])
  
  stations %>%
    mutate(distance = distHaversine(passenger_matrix, stations_matrix)) %>%
    slice_min(distance, n = n) %>%
    pull(stop_name)
}

# Main processing with proper NA handling for nearest stations
final <- passenger_trips %>%
  mutate(
    origin_nearest_stations = pmap_chr(
      list(ORIGIN_ADDRESS_LAT, ORIGIN_ADDRESS_LONG),
      function(lat, lon) {
        stations <- find_nearest_stations(lat, lon, bart_stations)
        if (all(is.na(stations))) {
          NA_character_
        } else {
          paste(stations, collapse = ", ")
        }
      }
    ),
    
    entry_station_in_top4 = map2_chr(
      ENTRY_FNL_NUM, origin_nearest_stations,
      ~ if (is.na(.x) || is.na(.y)) {
        "MISSING"
      } else if (.x %in% str_split(.y, ",\\s*")[[1]]) {
        "TRUE"
      } else {
        "FALSE"
      }
    ),
    
    destination_nearest_stations = pmap_chr(
      list(DESTIN_ADDRESS_LAT, DESTIN_ADDRESS_LONG),
      function(lat, lon) {
        stations <- find_nearest_stations(lat, lon, bart_stations)
        if (all(is.na(stations))) {
          NA_character_
        } else {
          paste(stations, collapse = ", ")
        }
      }
    ),
    
    exit_station_in_top4 = map2_chr(
      EXIT_STATION_TEXT, destination_nearest_stations,
      ~ if (is.na(.x) || is.na(.y)) {
        "MISSING"
      } else if (.x %in% str_split(.y, ",\\s*")[[1]]) {
        "TRUE"
      } else {
        "FALSE"
      }
    )
  ) %>%
  select(
    UNIQUE_IDENTIFIER,
    ORIGIN_ADDRESS_LAT, ORIGIN_ADDRESS_LONG, ENTRY_FNL_NUM,
    origin_nearest_stations, entry_station_in_top4,
    DESTIN_ADDRESS_LAT, DESTIN_ADDRESS_LONG, EXIT_STATION_TEXT,
    destination_nearest_stations, exit_station_in_top4
  )

# Optional: export to CSV
write_csv(final, file.path(bart_folder, "imputed_trip_stations_top4.csv"))
