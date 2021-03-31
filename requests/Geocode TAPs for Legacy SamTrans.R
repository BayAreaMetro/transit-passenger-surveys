# Geocode MAZs for Legacy SamTrans.R

#### Libraries

list_of_packages <- c(
  "geosphere",
  "ggplot2",
  "knitr",
  "rlang",
  "sf",
  "tidyverse"
)

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]

if(length(new_packages)) install.packages(new_packages)

for (p in list_of_packages){
  library(p, character.only = TRUE)
}

# The working directory is set as the location of the script. All other paths will be relative.

wd <- ("C:/Users/sisrael/Box/Modeling and Surveys/Share Data/Protected Data/Sijia Wang")
setwd(wd)


dir_path           <- "M:/Data/OnBoard/Data and Reports/"
f_taps_coords_path <- paste0(dir_path, "_geocoding Standardized/TAPs/TM2 TAPS/TM2 tap_node.csv")
f_survey_legacy_path    <- "M:/Data/OnBoard/Data and Reports/_data Standardized/survey_legacy.RData"

load(f_survey_legacy_path)

## Geocode to new TAPs after filtering to SamTrans only

SamTrans <- survey.legacy %>%
  filter(operator=="SamTrans") %>% 
  mutate(unique_ID = paste(ID, operator, survey_year, sep = "___")) %>% 
  select(-first_board_tap,-last_alight_tap)                                 # Remove old tap locations

dup1 <- SamTrans[duplicated(SamTrans),]

survey_board <- SamTrans %>%
  select(unique_ID, first_board_lat, first_board_lon, first_board_tech) %>%
  mutate(first_board_lat = as.numeric(first_board_lat)) %>%
  mutate(first_board_lon = as.numeric(first_board_lon)) %>%
  filter(!is.na(first_board_lat)) %>%
  filter(!is.na(first_board_lon))

survey_alight <- SamTrans %>%
  select(unique_ID, last_alight_lat, last_alight_lon, last_alight_tech) %>%
  mutate(last_alight_lat = as.numeric(last_alight_lat)) %>%
  mutate(last_alight_lon = as.numeric(last_alight_lon)) %>%
  filter(!is.na(last_alight_lat)) %>%
  filter(!is.na(last_alight_lon))

# remove(survey_lat, survey_lon)

## Geocode Transit Locations

taps_coords <- read.csv(f_taps_coords_path, stringsAsFactors = FALSE) %>%
  rename_all(tolower) %>%
  rowwise %>% mutate(mode=as.list(strsplit(mode_recode,","))) %>%                 # Create a list of tap modes for each row
  select(n, mode, lat, lon = long)

# CRS = 4326 sets the lat/long coordinates in the WGS1984 geographic survey
# CRS = 2230 sets the projection for NAD 1983 California Zone 6 in US Feet
taps_spatial <- st_as_sf(taps_coords, coords = c("lon", "lat"), crs = 4326)
taps_spatial <- st_transform(taps_spatial, crs = 2230)
survey_board_spatial <- st_as_sf(survey_board, coords = c("first_board_lon", "first_board_lat"), crs = 4326)
survey_board_spatial <- st_transform(survey_board_spatial, crs = 2230)
survey_alight_spatial <- st_as_sf(survey_alight, coords = c("last_alight_lon", "last_alight_lat"), crs = 4326)
survey_alight_spatial <- st_transform(survey_alight_spatial, crs = 2230)

survey_board_spatial <- survey_board_spatial %>%
  mutate(board_tap = NA)#,
# distance = NA)
survey_alight_spatial <- survey_alight_spatial %>%
  mutate(alight_tap = NA)#,
# distance = NA)

for (item in unique(unlist(taps_spatial$mode))) {
  temp_tap_spatial <- taps_spatial %>%
    filter(item %in% mode)
  temp_tap_spatial <- temp_tap_spatial %>%
    bind_cols(match = 1:nrow(temp_tap_spatial))

  temp_board <- survey_board_spatial %>%
    filter(first_board_tech == item)
  temp_board <- temp_board %>%
    bind_cols(temp_board_tap = st_nearest_feature(temp_board, temp_tap_spatial))

  temp_alight <- survey_alight_spatial %>%
    filter(last_alight_tech == item)
  temp_alight <- temp_alight %>%
    bind_cols(temp_alight_tap = st_nearest_feature(temp_alight, temp_tap_spatial))

  st_geometry(temp_tap_spatial) <- NULL

  temp_board <- temp_board %>%
    left_join(as.data.frame(temp_tap_spatial) %>% select(match, n), by = c("temp_board_tap" = "match")) %>%
    select(-temp_board_tap) %>%
    rename(temp_board_tap = n)

  survey_board_spatial <- survey_board_spatial %>%
    left_join(as.data.frame(temp_board) %>% select(unique_ID, temp_board_tap
                                                   # temp_dist = distance
    ), by = "unique_ID") %>%
    mutate(board_tap = ifelse(!is.na(temp_board_tap), temp_board_tap, board_tap)
           # distance = ifelse(!is.na(temp_dist), temp_dist, distance)
    ) %>%
    select(-temp_board_tap)#, -temp_dist)

  temp_alight <- temp_alight %>%
    left_join(as.data.frame(temp_tap_spatial) %>% select(match, n), by = c("temp_alight_tap" = "match")) %>%
    select(-temp_alight_tap) %>%
    rename(temp_alight_tap = n)

  survey_alight_spatial <- survey_alight_spatial %>%
    left_join(as.data.frame(temp_alight) %>% select(unique_ID, temp_alight_tap
                                                    # temp_dist = distance
    ), by = "unique_ID") %>%
    mutate(alight_tap = ifelse(!is.na(temp_alight_tap), temp_alight_tap, alight_tap)
           # distance = ifelse(!is.na(temp_dist), temp_dist, distance)
    ) %>%
    select(-temp_alight_tap)#, -temp_dist)

  rm(temp_tap_spatial, temp_board, temp_alight)
}

board_coords <- as.data.frame(st_coordinates(survey_board_spatial)) %>%
  rename(first_board_lat = Y,
         first_board_lon = X)
survey_board_spatial <- survey_board_spatial %>%
  bind_cols(board_coords) %>%
  select(-first_board_tech)
st_geometry(survey_board_spatial) <- NULL

alight_coords <- as.data.frame(st_coordinates(survey_alight_spatial)) %>%
  rename(last_alight_lat = Y,
         last_alight_lon = X)
survey_alight_spatial <- survey_alight_spatial %>%
  bind_cols(alight_coords) %>%
  select(-last_alight_tech)
st_geometry(survey_alight_spatial) <- NULL

board_alight_tap <- survey_board_spatial %>%
  left_join(survey_alight_spatial, by = c("unique_ID"))

board_alight_tap <- board_alight_tap %>% 
  select(-first_board_lon,-first_board_lat,-last_alight_lat,-last_alight_lon) %>% 
  rename(first_board_tap=board_tap,last_alight_tap=alight_tap)

SamTrans <- SamTrans %>% 
  left_join(., board_alight_tap,by="unique_ID")

## Write RDS to disk

# Compute trip weight, replace missing weights with zero, and set field language to interview language
SamTrans <- SamTrans %>%
  mutate(weight = ifelse(is.na(weight), 0.0, weight)) %>% 
  select(-unique_ID)

saveRDS(SamTrans, file = "SamTrans033021.RData")
