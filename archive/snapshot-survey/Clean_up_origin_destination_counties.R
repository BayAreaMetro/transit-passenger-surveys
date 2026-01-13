# Note: incomplete code, just stubbed here for future application
# Clean_up_origin_destination_counties.r
# Clean up city case

trial <- snapshot %>%
  filter(`Orig_Lat/Long`=="Unspecified" | `Dest_Lat/Long`=="Unspecified") %>% 
  select(Q3a, Q4a) %>%
  pivot_longer(everything(), values_to = "value") %>%
  distinct(value) 

city_clean <- str_to_title(trial)

mutate(city_clean=string_to_title(value))
print(sort(trial))

# Geocode city to county

library(tidyverse)
library(tidygeocoder)
library(sf)
library(tigris)

# Example input: city and state
cities <- tibble(
  city = c("Los Angeles", "Chicago", "Miami"),
  state = c("CA", "IL", "FL")
)

# Step 1: Geocode cities using OpenStreetMap (no API key needed)
geocoded_cities <- cities %>%
  geocode(city = city, state = state, method = "osm", lat = latitude, long = longitude)

# Step 2: Convert to sf object for spatial operations
cities_sf <- geocoded_cities %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

# Step 3: Load county shapefile (cartographic boundary, sf format)
options(tigris_use_cache = TRUE)
counties_sf <- counties(cb = TRUE, year = 2020, class = "sf")

# Step 4: Spatial join to find which county each city point falls into
matched_sf <- cities_sf %>%
  st_join(counties_sf, join = st_within)

# Step 5: Select and rename output columns
city_to_county <- matched_sf %>%
  transmute(
    city,
    state,
    county = NAME,
    state_fips = STATEFP,
    county_fips = COUNTYFP,
    geoid = GEOID
  )

# View result
print(city_to_county)
