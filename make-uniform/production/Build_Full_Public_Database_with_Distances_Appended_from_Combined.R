# Build_Public_Database_with_Raw_Lat_Long_from_Combined.R
# Get rid of scientific notation

options(scipen = 99999)

# DIRECTORIES AND LIBRARIES

TPS_Dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data"
today = Sys.Date()


suppressMessages(library(tidyverse))
library(reshape2)
library(sf)
library(tigris)
library(geosphere)
library(reldist)

# Read TPS dataset survey data
load(file.path(TPS_Dir,     "survey_combined_2024-03-04.RData"))

# Create operator equivalency with technology

operator = c("AC TRANSIT", "ACE", "AMTRAK", "BART", "Bay Area Shuttles", 
             "BLUE & GOLD FERRY", "BLUE GOLD FERRY", "CALTRAIN", "COUNTY CONNECTION", 
             "DUMBARTON", "DUMBARTON EXPRESS", "EMERY-GO-ROUND", "EMERYVILLE MTA", 
             "FAIRFIELD-SUISUN", "FAST", "GOLDEN GATE FERRY", "GOLDEN GATE TRANSIT", 
             "Greyhound", "LAVTA", "MARIN TRANSIT", "Missing", "MODESTO TRANSIT", 
             "MUNI", "NAPA VINE", "None", "Operator Outside Bay Area", "Other", 
             "OTHER", "PETALUMA TRANSIT", "PRIVATE SHUTTLE", "RIO-VISTA", 
             "SAMTRANS", "SAN JOAQUIN TRANSIT", "San Leandro Links", "SANTA ROSA CITY BUS", 
             "Santa Rosa CityBus", "SANTA ROSA CITYBUS", "SF BAY FERRY", "SMART", 
             "SOLTRANS", "Sonoma County Transit", "SONOMA COUNTY TRANSIT", 
             "STANFORD SHUTTLES", "TRI-DELTA", "UNION CITY", "VACAVILLE CITY COACH", 
             "VALLEJO TRANSIT", "VTA", "WESTCAT", "WHEELS (LAVTA)")

technology = c("LB", "CR", "CR","HR", "LB",
               "FR", "FR", "CR", "LB",
               "EB", "EB", "LB", "LB",
               "LB", "LB", "FR", "EB",
               "EB", "LB", "LB", "None", "LB",
               "LB", "LB", "None", "LB", "LB",
               "LB", "LB", "LB", "LB",
               "LB", "LB", "LB", "LB", 
               "LB", "LB", "FR", "CR",
               "LB", "LB", "LB",
               "LB", "LB", "LB", "LB",
               "LB", "LB", "LB", "LB")

opTechXWalk <- data.frame(operator, technology)

survey_tech = c("commuter rail", "express bus", "ferry", "heavy rail", "light rail", "local bus")
survey_tech_short = c("CR", "EB", "FR", "HR", "LR", "LB")
survey_tech_df <- data.frame(survey_tech, survey_tech_short)

# Remove weekend records, Capitol Corridor
# Also remove "dummy records" (BART, Caltrain, Muni) used for weighting purposes but lacking characteristics

TPS <- survey_combine %>% filter(weekpart=="WEEKDAY" & 
                               operator != "Capitol Corridor") %>% 
                              filter(access_mode!="Missing - Dummy Record" | is.na(access_mode)) 


# Transform survey_tech into simplified values for survey_tech, first_board tech, and last_alight tech

TPS$survey_tech <- survey_tech_df$survey_tech_short[match(TPS$survey_tech, survey_tech_df$survey_tech)]
TPS$first_board_tech <- survey_tech_df$survey_tech_short[match(TPS$first_board_tech, survey_tech_df$survey_tech)]
TPS$last_alight_tech <- survey_tech_df$survey_tech_short[match(TPS$last_alight_tech, survey_tech_df$survey_tech)]

# Detailed Operator Coding
# Edit operator names to show local and express bus and to match names in transit ridership targets

TPS$operator[TPS$operator=="AC Transit" & TPS$survey_tech=="LB"] <- "AC Transit [LOCAL]"
TPS$operator[TPS$operator=="AC Transit" & TPS$survey_tech=="EB"] <- "AC Transit [EXPRESS]"

TPS$operator[TPS$operator=="County Connection" & TPS$survey_tech=="LB"] <- "County Connection [LOCAL]"
TPS$operator[TPS$operator=="County Connection" & TPS$survey_tech=="EB"] <- "County Connection [EXPRESS]"

TPS$operator[TPS$operator=="FAST" & TPS$survey_tech=="LB"] <- "FAST [LOCAL]"
TPS$operator[TPS$operator=="FAST" & TPS$survey_tech=="EB"] <- "FAST [EXPRESS]"

TPS$operator[TPS$operator=="Golden Gate Transit" & TPS$survey_tech=="EB"] <- "Golden Gate Transit [EXPRESS]"
TPS$operator[TPS$operator=="Golden Gate Transit" & TPS$survey_tech=="FR"] <- "Golden Gate Transit [FERRY]"

TPS$operator[TPS$operator=="Napa Vine" & TPS$survey_tech=="LB"] <- "Napa Vine [LOCAL]"
TPS$operator[TPS$operator=="Napa Vine" & TPS$survey_tech=="EB"] <- "Napa Vine [EXPRESS]"

TPS$operator[TPS$operator=="Delta Breeze"] <- "Rio Vista Delta Breeze"

TPS$operator[TPS$operator=="SamTrans" & TPS$survey_tech=="LB"] <- "SamTrans [LOCAL]"
TPS$operator[TPS$operator=="SamTrans" & TPS$survey_tech=="EB"] <- "SamTrans [EXPRESS]"

TPS$operator[TPS$operator=="SF Muni" & TPS$survey_tech=="LB"] <- "SF Muni [LOCAL]"
TPS$operator[TPS$operator=="SF Muni" & TPS$survey_tech=="LR"] <- "SF Muni [LRT]"

TPS$operator[TPS$operator=="Soltrans" & TPS$survey_tech=="LB"] <- "Soltrans [LOCAL]"
TPS$operator[TPS$operator=="Soltrans" & TPS$survey_tech=="EB"] <- "Soltrans [EXPRESS]"

TPS$operator[TPS$operator=="City Coach"] <- "Vacaville City Coach"

TPS$operator[TPS$operator=="VTA" & TPS$survey_tech=="LB"] <- "VTA [LOCAL]"
TPS$operator[TPS$operator=="VTA" & TPS$survey_tech=="EB"] <- "VTA [EXPRESS]"
TPS$operator[TPS$operator=="VTA" & TPS$survey_tech=="LR"] <- "VTA [LRT]"

TPS$operator[TPS$operator=="WestCAT" & TPS$survey_tech=="LB"] <- "WestCAT [LOCAL]"
TPS$operator[TPS$operator=="WestCAT" & TPS$survey_tech=="EB"] <- "WestCAT [EXPRESS]"

# Rename field and select variables to keep 

names(TPS)[names(TPS)=="survey_tech"] <- "SURVEY_MODE"
TPS$nTransfers <- TPS$boardings - 1
TPS$transfer_from_tech <- opTechXWalk$technology[match(TPS$transfer_from, opTechXWalk$operator)]
TPS$transfer_to_tech <- opTechXWalk$technology[match(TPS$transfer_to, opTechXWalk$operator)]

TPS$period[TPS$day_part=="EARLY AM"] <- "EA"
TPS$period[TPS$day_part=="AM PEAK"]  <- "AM"
TPS$period[TPS$day_part=="MIDDAY"]   <- "MD"
TPS$period[TPS$day_part=="PM PEAK"]  <- "PM"
TPS$period[TPS$day_part=="EVENING"]  <- "EV"

full_TPS <- TPS %>% 
  select(c("ID", "operator", "survey_year", "SURVEY_MODE", "access_mode", 
           "depart_hour", "dest_purp", "direction","egress_mode", "eng_proficient", 
           "fare_category", "fare_medium","gender", 
           "hispanic", "household_income", "interview_language", "onoff_enter_station", "onoff_exit_station", 
           "orig_purp", "persons", "return_hour","route", "student_status", 
           "survey_type", "time_period", "transit_type", "trip_purp", "vehicles", 
           "weekpart", "weight", "work_status", "workers", "canonical_operator", "operator_detail", "technology", 
           "approximate_age", "tour_purp", "tour_purp_case", "vehicle_numeric_cat", 
           "worker_numeric_cat", "auto_suff", "first_before_operator_detail", 
           "second_before_operator_detail", "third_before_operator_detail", 
           "first_after_operator_detail", "second_after_operator_detail", 
           "third_after_operator_detail", "first_before_operator", "second_before_operator", 
           "third_before_operator", "first_after_operator", "second_after_operator", 
           "third_after_operator", "first_before_technology", "second_before_technology", 
           "third_before_technology", "first_after_technology", "second_after_technology", 
           "third_after_technology", "transfer_from", "transfer_to", "first_board_tech", 
           "last_alight_tech", "commuter_rail_present", "heavy_rail_present", 
           "express_bus_present", "ferry_present", "light_rail_present", 
           "boardings", "race", "language_at_home", "day_of_the_week", "field_start", 
           "field_end", "day_part", "unique_ID", "dest_tm1_taz", "home_tm1_taz", 
           "orig_tm1_taz", "school_tm1_taz", "workplace_tm1_taz", "dest_tm2_taz", 
           "home_tm2_taz", "orig_tm2_taz", "school_tm2_taz", "workplace_tm2_taz", 
           "dest_tm2_maz", "home_tm2_maz", "orig_tm2_maz", "school_tm2_maz", 
           "workplace_tm2_maz", "board_tap", "alight_tap", "trip_weight", 
           "field_language", "survey_time", "path_access", "path_egress", 
           "path_line_haul", "path_label", "first_board_tap", "last_alight_tap", 
           "survey_batch", "nTransfers", "period", "transfer_from_tech", "transfer_to_tech", 
           "orig_lon","orig_lat","first_board_lon","first_board_lat","survey_board_lon","survey_board_lat",
           "survey_alight_lon","survey_alight_lat",
           "last_alight_lon","last_alight_lat","dest_lon","dest_lat","home_lon","home_lat",
           "workplace_lon","workplace_lat","school_lon","school_lat"))

# Spatial match to counties in California

survey_lat <- TPS %>%
  select(unique_ID, dest = dest_lat, home = home_lat, orig = orig_lat,
         school = school_lat, workplace = workplace_lat, first_board=first_board_lat,
         last_alight=last_alight_lat,survey_board=survey_board_lat,survey_alight=survey_alight_lat)

survey_lon <- TPS %>%
  select(unique_ID, dest = dest_lon, home = home_lon, orig = orig_lon,
         school = school_lon, workplace = workplace_lon, first_board=first_board_lon,
         last_alight=last_alight_lon,survey_board=survey_board_lon,survey_alight=survey_alight_lon)

survey_lat <- survey_lat %>%
  gather(variable, y_coord, -unique_ID)

survey_lon <- survey_lon %>%
  gather(variable, x_coord, -unique_ID)

survey_coords <- left_join(survey_lat, survey_lon, by = c("unique_ID", "variable")) %>%     # remove records with no lat/lon
  mutate(x_coord = as.numeric(x_coord)) %>%
  mutate(y_coord = as.numeric(y_coord)) %>%
  filter(!is.na(x_coord)) %>%
  filter(!is.na(y_coord))

# Create an sf object with the NAD83 / UTM zone 10N (ftUS) projection
utm_ftus <- st_crs(26910, proj4string = "+units=us-ft")

survey_coords_spatial <- st_as_sf(survey_coords, coords = c("x_coord", "y_coord"), crs = 4326)
survey_coords_spatial <- st_transform(survey_coords_spatial,crs = utm_ftus)

# Get counties for spatially matching. CB=false means that boundaries are not clipped to shoreline
# CB=TRUE often omits locations close to the shoreline
# Remove geometry and join county data with survey file


counties <- counties(state = "CA", cb = FALSE, year = 2020)
counties_proj <- st_transform(counties, crs = st_crs(survey_coords_spatial))
matched_counties_interim <- st_join(survey_coords_spatial, counties_proj, join = st_nearest_feature)
matched_counties <- matched_counties_interim
st_geometry(matched_counties) <- NULL
matched_counties <- matched_counties %>% 
  select(unique_ID,variable,NAME) %>% 
  pivot_wider(., names_from = variable, values_from = NAME, values_fill = NA)

# Fewer variables for TPS

fewer_variables_TPS <- left_join (TPS,matched_counties,by="unique_ID") %>% 
  select(c(ID, operator, survey_year, SURVEY_MODE, access_mode, 
           depart_hour, dest_purp, direction,egress_mode, eng_proficient, 
           fare_category, fare_medium,gender, 
           hispanic, household_income, interview_language, onoff_enter_station, onoff_exit_station, 
           orig_purp, persons, return_hour,route, student_status, 
           survey_type, time_period, transit_type, trip_purp, vehicles, 
           weekpart, weight, work_status, workers, canonical_operator, operator_detail, technology, 
           approximate_age, vehicle_numeric_cat, 
           worker_numeric_cat, auto_suff,  transfer_from, transfer_to, first_board_tech, 
           last_alight_tech, boardings, race, language_at_home, day_of_the_week,
           day_part, unique_ID,  trip_weight, field_language, survey_time,
           survey_batch, nTransfers, period,orig_lon,orig_lat,first_board_lon,
           first_board_lat,survey_board_lon,survey_board_lat,
           survey_alight_lon,survey_alight_lat, last_alight_lon,last_alight_lat,dest_lon,dest_lat,home_lon,home_lat,
           workplace_lon,workplace_lat,school_lon,school_lat, dest_county=dest, home_county=home, orig_county=orig, school_county=school, 
           workplace_county=workplace, first_board_county=first_board, last_alight_county=last_alight, 
           survey_board_county=survey_board, survey_alight_county=survey_alight))

# Calculate distances
# Set radius of the earth for Haversine distance calculation
# https://www.space.com/17638-how-big-is-earth.html
# Distance is calculated in miles (3963.20 mi.)
# Alternate distance in meters would be 6378137 m. 

radius_miles <- 3963.2

# Convert coordinate data to numeric format from character
# Assign NA if out of range lat value (appears in only one field for a few records)

temp <- fewer_variables_TPS %>% 
  mutate_at(
    grep("_lat|_lon",names(.)),as.numeric) %>% 
  mutate(
    first_board_lat=if_else(first_board_lat < -90,NA_real_,first_board_lat)
  )

# Calculate distances
# Origin to destination
# Boarding to alighting
# Origin to first boarding
# Origin to survey vehicle boarding
# Survey vehicle alighting to destination
# Last alighting to destination
# Recode age data into useful categories

TPS_distance <- temp %>% 
  rowwise() %>% 
  mutate(orig_dest_dist          = distHaversine(c(orig_lon,orig_lat),c(dest_lon,dest_lat),r=radius_miles),
         board_alight_dist       = distHaversine(c(survey_board_lon,survey_board_lat),c(survey_alight_lon,survey_alight_lat),r=radius_miles),
         orig_firstboard_dist    = distHaversine(c(orig_lon,orig_lat),c(first_board_lon,first_board_lat),r=radius_miles),
         orig_surveyboard_dist   = distHaversine(c(orig_lon,orig_lat),c(survey_board_lon,survey_board_lat),r=radius_miles),
         survey_alight_dest_dist = distHaversine(c(survey_alight_lon,survey_alight_lat),c(dest_lon,dest_lat),r=radius_miles),
         last_alight_dest_dist   = distHaversine(c(last_alight_lon,last_alight_lat),c(dest_lon,dest_lat),r=radius_miles),
  ) %>% 
  ungroup() %>% 
  select(-(grep("_lat|_lon",names(.)))) %>% 
  mutate(age_cat=case_when(
    approximate_age < 18                              ~ "1_under 18",
    approximate_age >= 18 & approximate_age <= 24     ~ "2_18 to 24",
    approximate_age >= 25 & approximate_age <= 34     ~ "3_25 to 34",
    approximate_age >= 35 & approximate_age <= 44     ~ "4_35 to 44",
    approximate_age >= 45 & approximate_age <= 54     ~ "5_45 to 54",
    approximate_age >= 55 & approximate_age <= 64     ~ "6_55 to 64",
    approximate_age >= 65 & approximate_age <= 74     ~ "7_65 to 74",
    approximate_age >= 75                             ~ "8_75 and above",
  ))

# Create a composite race category

TPS_distance<-TPS_distance %>% 
  mutate(composite_race=case_when(
    (hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" | is.na(hispanic)) & race=="WHITE"         ~ "White",
    (hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" | is.na(hispanic)) & race=="BLACK"         ~ "Black",
    (hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" | is.na(hispanic)) & race=="ASIAN"         ~ "Asian",
    (hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" | is.na(hispanic)) & race=="OTHER"         ~ "Other",
    hispanic=="HISPANIC/LATINO OR OF SPANISH ORIGIN"                             ~ "Hispanic",
    race=="Missing"                                                              ~ "Missing",
    hispanic %in% c("missing","Missing")                                         ~ "Missing",
    TRUE                                                                         ~ "Not coded properly"
  ))

# Fix limited-English proficiency by adding "very well" to English-only speakers

TPS_distance <- TPS_distance %>% mutate(
  eng_proficient=if_else(language_at_home=="ENGLISH ONLY","VERY WELL",eng_proficient))

save(TPS_distance, file=file.path(TPS_Dir, "public_version",paste0("TPS_Public_Version_Distances_Appended_",today,".Rdata")))

write.csv(TPS_distance,file.path(TPS_Dir,"public_version",paste0("TPS_Public_Version_Distances_Appended_",today,".csv")),row.names = F)
          

