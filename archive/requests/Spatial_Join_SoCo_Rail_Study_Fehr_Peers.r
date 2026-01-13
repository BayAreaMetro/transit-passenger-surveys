# Spatial_Join_SoCo_Rail_Study_Fehr_Peers.r
# Script to join MAZ and TAZ for a few operators not in the standard dataset
# SI

# Import libraries

library(sf)
library(sp)
library(rgdal)
suppressMessages(library(dplyr))

# Set up input and output directories

Onboard <- "M:/Data/OnBoard/Data and Reports/"
ACE_in  <- paste0(Onboard,"ACE/2019/ACE19_Final Data Add New Route Date Time Columns NO POUND OR SINGLE QUOTE.csv")
UC_in   <- paste0(Onboard,"Union City/2017/Union City Transit_fix_error_add_time_route_NO POUND OR SINGLE QUOTE.csv")
ACT_in  <- paste0(Onboard,"AC Transit/2018/OD_20180703_ACTransit_DraftFinal_Income_Imputation (EasyPassRecode) NO POUND OR SINGLE QUOTE.csv")


TAZ_in     <- "M:/Data/GIS layers/Travel_Analysis_Zones_(TAZ1454)/Travel Analysis Zones.shp"
MAZ_in     <- paste0(Onboard,"_geocoding Standardized/TM2_Zones/mazs.shp")

username        <- Sys.getenv("USERNAME")
output_location <-paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Protected Data/Fehr_Peers/")

# Bring in shapefiles and select TAZs and geometry columns

TAZ <- st_read(TAZ_in) %>%
  select(TAZ=TAZ1454,geometry)

MAZ <- st_read(MAZ_in) %>%
  select(MAZ=MAZ_ORIGIN,geometry)
  
# Bring in operator files, ensuring origin and destination lat/long format is numeric
# ACE

ACE <- read.csv(ACE_in, stringsAsFactors = FALSE) %>% mutate(
  Origin_lat = as.numeric(Origin_lat),
  Origin_lon = as.numeric(Origin_lon),
  Destination_lat = as.numeric(Destination_lat),
  Destination_lon = as.numeric(Destination_lon),
  Home_lat = as.numeric(Home_lat),
  Home_lon = as.numeric(Home_lon),
  Work_lat = as.numeric(Work_lat),
  Work_lon = as.numeric(Work_lon),
  School_lat = as.numeric(School_lat),
  School_lon = as.numeric(School_lon)
  )

# Union City

UC <- read.csv(UC_in, stringsAsFactors = FALSE) %>% mutate(
  startlat = as.numeric(startlat),
  startlon = as.numeric(startlon),
  endlat = as.numeric(endlat),
  endlon = as.numeric(endlon),
  homelat = as.numeric(homelat),
  homelon = as.numeric(homelon),
  worklat = as.numeric(worklat),
  worklon = as.numeric(worklon),
  school_lat = as.numeric(school_lat),
  school_lon = as.numeric(school_lon)
)

# AC Transit

AC <- read.csv(ACT_in, stringsAsFactors = FALSE) %>% mutate(
  final_origin_lat = as.numeric(final_origin_lat),
  final_origin_lon = as.numeric(final_origin_lon),
  final_destin_lat = as.numeric(final_destin_lat),
  final_destin_lon = as.numeric(final_destin_lon),
  home_lat = as.numeric(home_lat),
  home_lon = as.numeric(home_lon),
  workplace_lat = as.numeric(workplace_lat),
  workplace_lon = as.numeric(workplace_lon),
  school_lat = as.numeric(school_lat),
  school_lon = as.numeric(school_lon),
  hotel_lat = as.numeric(hotel_lat),
  hotel_lon = as.numeric(hotel_lon)
)

# Separate locations into separate files, remove missing data

#ACE

ACE_origin <- ACE %>% 
  select(ID,Origin_lat,Origin_lon) %>% 
  filter(!is.na(Origin_lat))

ACE_destination <- ACE %>% 
  select(ID,Destination_lat,Destination_lon) %>% 
  filter(!is.na(Destination_lat))

ACE_home <- ACE %>% 
  select(ID,Home_lat,Home_lon) %>% 
  filter(!is.na(Home_lat))

ACE_work <- ACE %>% 
  select(ID,Work_lat,Work_lon) %>% 
  filter(!is.na(Work_lat))

ACE_school <- ACE %>% 
  select(ID,School_lat,School_lon) %>% 
  filter(!is.na(School_lat))

#Union City

UC_origin <- UC %>% 
  select(id,startlat, startlon) %>% 
  filter(!is.na(startlat))

UC_destination <- UC %>% 
  select(id,endlat,endlon) %>% 
  filter(!is.na(endlat))

UC_home <- UC %>% 
  select(id,homelat,homelon) %>% 
  filter(!is.na(homelat))

UC_work <- UC %>% 
  select(id,worklat,worklon) %>% 
  filter(!is.na(worklat))

UC_school <- UC %>% 
  select(id,school_lat,school_lon) %>% 
  filter(!is.na(school_lat))

#AC Transit

AC_origin <- AC %>% 
  select(id,final_origin_lat,final_origin_lon) %>% 
  filter(!is.na(final_origin_lat))

AC_destination <- AC %>% 
  select(id,final_destin_lat,final_destin_lon) %>% 
  filter(!is.na(final_destin_lat))

AC_home <- AC %>% 
  select(id,home_lat,home_lon) %>% 
  filter(!is.na(home_lat))

AC_work <- AC %>% 
  select(id,workplace_lat,workplace_lon) %>% 
  filter(!is.na(workplace_lat))

AC_school <- AC %>% 
  select(id,school_lat,school_lon) %>% 
  filter(!is.na(school_lat))

AC_hotel <- AC %>% 
  select(id,hotel_lat,hotel_lon) %>% 
  filter(!is.na(hotel_lat))

# CRS = 4326 sets the lat/long coordinates in the WGS1984 geographic survey
# CRS = 2230 sets the projection for NAD 1983 California Zone 6 in US Feet

#ACE

ACE_origin_space <- st_as_sf(ACE_origin, coords = c("Origin_lon", "Origin_lat"), crs = 4326)
ACE_origin_space <- st_transform(ACE_origin_space,crs = 2230)

ACE_destination_space <- st_as_sf(ACE_destination, coords = c("Destination_lon", "Destination_lat"), crs = 4326)
ACE_destination_space <- st_transform(ACE_destination_space,crs = 2230)

ACE_home_space <- st_as_sf(ACE_home, coords = c("Home_lon", "Home_lat"), crs = 4326)
ACE_home_space <- st_transform(ACE_home_space,crs = 2230)

ACE_work_space <- st_as_sf(ACE_work, coords = c("Work_lon", "Work_lat"), crs = 4326)
ACE_work_space <- st_transform(ACE_work_space,crs = 2230)

ACE_school_space <- st_as_sf(ACE_school, coords = c("School_lon", "School_lat"), crs = 4326)
ACE_school_space <- st_transform(ACE_school_space,crs = 2230)

# Union City

UC_origin_space <- st_as_sf(UC_origin, coords = c("startlon", "startlat"), crs = 4326)
UC_origin_space <- st_transform(UC_origin_space,crs = 2230)

UC_destination_space <- st_as_sf(UC_destination, coords = c("endlon", "endlat"), crs = 4326)
UC_destination_space <- st_transform(UC_destination_space,crs = 2230)

UC_home_space <- st_as_sf(UC_home, coords = c("homelon", "homelat"), crs = 4326)
UC_home_space <- st_transform(UC_home_space,crs = 2230)

UC_work_space <- st_as_sf(UC_work, coords = c("worklon", "worklat"), crs = 4326)
UC_work_space <- st_transform(UC_work_space,crs = 2230)

UC_school_space <- st_as_sf(UC_school, coords = c("school_lon", "school_lat"), crs = 4326)
UC_school_space <- st_transform(UC_school_space,crs = 2230)

# AC Transit

AC_origin_space <- st_as_sf(AC_origin, coords = c("final_origin_lon", "final_origin_lat"), crs = 4326)
AC_origin_space <- st_transform(AC_origin_space,crs = 2230)

AC_destination_space <- st_as_sf(AC_destination, coords = c("final_destin_lon", "final_destin_lat"), crs = 4326)
AC_destination_space <- st_transform(AC_destination_space,crs = 2230)

AC_home_space <- st_as_sf(AC_home, coords = c("home_lon", "home_lat"), crs = 4326)
AC_home_space <- st_transform(AC_home_space,crs = 2230)

AC_work_space <- st_as_sf(AC_work, coords = c("workplace_lon", "workplace_lat"), crs = 4326)
AC_work_space <- st_transform(AC_work_space,crs = 2230)

AC_school_space <- st_as_sf(AC_school, coords = c("school_lon", "school_lat"), crs = 4326)
AC_school_space <- st_transform(AC_school_space,crs = 2230)

AC_hotel_space <- st_as_sf(AC_hotel, coords = c("hotel_lon", "hotel_lat"), crs = 4326)
AC_hotel_space <- st_transform(AC_hotel_space,crs = 2230)

# Convert TAZ shape to same project as GGT (and all successive transit files)

TAZ_shape <- st_transform(TAZ,crs = st_crs(ACE_origin_space))
MAZ_shape <- st_transform(MAZ,crs = st_crs(ACE_origin_space))

# Spatially join origin, destination, home, work, and school to shapefile

# ACE

ACE_origin2 <- st_join(ACE_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_MAZ=MAZ)

ACE_destination2 <- st_join(ACE_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_MAZ=MAZ)

ACE_home2 <- st_join(ACE_home_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_MAZ=MAZ)

ACE_work2 <- st_join(ACE_work_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_MAZ=MAZ)

ACE_school2 <- st_join(ACE_school_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_MAZ=MAZ)

# Union City

UC_origin2 <- st_join(UC_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_MAZ=MAZ)

UC_destination2 <- st_join(UC_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_MAZ=MAZ)

UC_home2 <- st_join(UC_home_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_MAZ=MAZ)

UC_work2 <- st_join(UC_work_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_MAZ=MAZ)

UC_school2 <- st_join(UC_school_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_MAZ=MAZ)

# AC Transit

AC_origin2 <- st_join(AC_origin_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Origin_MAZ=MAZ)

AC_destination2 <- st_join(AC_destination_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Destination_MAZ=MAZ)

AC_home2 <- st_join(AC_home_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Home_MAZ=MAZ)

AC_work2 <- st_join(AC_work_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Work_MAZ=MAZ)

AC_school2 <- st_join(AC_school_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(School_MAZ=MAZ)

AC_hotel2 <- st_join(AC_hotel_space,TAZ_shape, join=st_within,left=TRUE)%>%
  rename(Hotel_TAZ=TAZ) %>% 
  st_join(.,MAZ_shape, join=st_within,left=TRUE)%>%
  rename(Hotel_MAZ=MAZ)

# Remove geometry columns from origin/destination for join

ACE_origin2            <- as.data.frame(ACE_origin2) %>% select(-geometry)
ACE_destination2       <- as.data.frame(ACE_destination2) %>% select(-geometry)
ACE_home2              <- as.data.frame(ACE_home2) %>% select(-geometry)
ACE_work2              <- as.data.frame(ACE_work2) %>% select(-geometry)
ACE_school2            <- as.data.frame(ACE_school2) %>% select(-geometry)

UC_origin2            <- as.data.frame(UC_origin2) %>% select(-geometry)
UC_destination2       <- as.data.frame(UC_destination2) %>% select(-geometry)
UC_home2              <- as.data.frame(UC_home2) %>% select(-geometry)
UC_work2              <- as.data.frame(UC_work2) %>% select(-geometry)
UC_school2            <- as.data.frame(UC_school2) %>% select(-geometry)

AC_origin2            <- as.data.frame(AC_origin2) %>% select(-geometry)
AC_destination2       <- as.data.frame(AC_destination2) %>% select(-geometry)
AC_home2              <- as.data.frame(AC_home2) %>% select(-geometry)
AC_work2              <- as.data.frame(AC_work2) %>% select(-geometry)
AC_school2            <- as.data.frame(AC_school2) %>% select(-geometry)
AC_hotel2             <- as.data.frame(AC_hotel2) %>% select(-geometry)

# Join TAZs and MAZs to files by operator, remove PII geography

# ACE

ACE2 <- left_join(ACE,ACE_origin2,by="ID")
ACE2 <- left_join(ACE2,ACE_destination2,by="ID")
ACE2 <- left_join(ACE2,ACE_home2,by="ID")
ACE2 <- left_join(ACE2,ACE_work2,by="ID")
ACE2 <- left_join(ACE2,ACE_school2,by="ID") %>%
  select(ID, Survey_date, Survey_route, Daypart, Daypart_center, 
          Direction, ACE_Boarding, ACE_Alighting, Origin, Origin_other, 
          Destination,Destination_other, Access_mode, Access_mode_other, Access_minutes, Access_miles, 
          Transfer_from_amount, Transfer_from_1st, Agency_transfer_from_1st, 
          Route_transfer_from_1st, Agency_route_transfer_from_1st, 
          Transfer_from_1st_lat, Transfer_from_1st_lon, Transfer_from_1st_address, 
          Transfer_from_2nd, Agency_transfer_from_2nd, Route_transfer_from_2nd, 
          Agency_route_transfer_from_2nd, Transfer_from_2nd_lat, Transfer_from_2nd_lon, 
          Transfer_from_2nd_address, Transfer_from_3rd, Agency_transfer_from_3rd, 
          Route_transfer_from_3rd, Agency_route_transfer_from_3rd, 
          Transfer_from_3rd_lat, Transfer_from_3rd_lon, Transfer_from_3rd_Address, 
          Survey_route_boarding_lat, Survey_route_boarding_lon, Survey_route_boarding_address, 
          Survey_route_alighting_lat, Survey_route_alighting_lon, Survey_route_alighting_address, 
          Transfer_to_amount, Transfer_to_1st, Agency_transfer_to_1st, 
          Route_transfer_to_1st, Agency_route_transfer_to_1st, Transfer_to_1st_lat, 
          Transfer_to_1st_lon, Transfer_to_1st_address, Transfer_to_2nd, 
          Agency_transfer_to_2nd, Route_transfer_to_2nd, Agency_route_transfer_to_2nd, 
          Transfer_to_2nd_lat, Transfer_to_2nd_lon, Transfer_to_2nd_address, 
          Transfer_to_3rd, Agency_transfer_to_3rd, Route_transfer_to_3rd, 
          Agency_route_transfer_to_3rd, Transfer_to_3rd_lat, Transfer_to_3rd_lon, 
          Transfer_to_3rd_address, Egress_mode, Egress_mode_other, 
          Egress_minutes, Egress_miles, Employment_status, Work_before_trip, 
          Work_after_trip, Student_status,Been_2school_today, Will_go2school_today, School_type, 
          Came_from_home, Time_left_home, Will_return_home, 
          Time_return_home, Same_trip_opposite_direction, Time_same_trip_opposite_direction, 
          Rate_overall, Rate_value, Rate_station, Rate_conductors, 
          Rate_schedules, Pay_mode, Pay_mode_other, Fare_type, 
          Fare_type_other, License_status, Persons_HH, Persons_HH_other, 
          Workers_HH, Workers_HH_other, Vehicles_HH, Vehicles_HH_other, 
          Year_born, Hispanic, Race, Race_other, Lang_other_than_english, 
          Other_lang_spoken_at_home, Other_lang_spoken_at_home_other, 
          English_fluency, HH_income, Gender, Weight, Origin_TAZ, Destination_TAZ, Home_TAZ, 
          Work_TAZ, School_TAZ, Origin_MAZ, Destination_MAZ, Home_MAZ, Work_MAZ, School_MAZ)
    
# Union City

UC2 <- left_join(UC,UC_origin2,by="id")
UC2 <- left_join(UC2,UC_destination2,by="id")
UC2 <- left_join(UC2,UC_home2,by="id")
UC2 <- left_join(UC2,UC_work2,by="id")
UC2 <- left_join(UC2,UC_school2,by="id") %>%
  select(X, id, sys_starttime, sys_endtime, run, route, 
         rcode, day, date, dtype, strata, wcode, weight, 
         alt_weight, interview_language, orig_purp, dest_purp, 
         sch, school_name, college_name, access_mode, egress_mode, 
         first_board_lat, first_board_lon, 
         last_alight_lat, last_alight_lon, xfers_before, 
         X1_system_before, X1_route_before, X1_before_lat_start, 
         X1_before_long_start, X1_before_lat_end, X1_before_long_end, 
         X2_system_before, X2_route_before, X2_before_lat_start, 
         X2_before_long_start, X2_before_lat_end, X2_before_long_end, 
         route.1, routeboard_lat, routeboard_long, routealight_lat, 
         routealight_long, xfers_after, X1_after_system, X1_route_after_system, 
         X1_after_lat_start, X1_after_long_start, X1_after_lat_end, 
         X1_after_long_end, X2_after_system, X2_route_after_system, 
         X2_after_lat_start, X2_after_long_start, X2_after_lat_end, 
         X2_after_long_end, fare, farecat, cars, hh, hhwork, 
         yearborn, hisp, race_dmy_asn, race_dmy_blk, race_dmy_hwi, 
         race_dmy_ind, race_dmy_wht, race_other, income, language_at_home_binary, 
         langhh, eng_proficient, resident_status_county,work_status, at_work_after_dest_purp, 
         at_work_prior_to_orig_purp, at_school_after_dest_purp, 
         at_school_prior_to_orig_purp, depart_hour, return_hour, 
         gender, mode, first_route_before_survey_board, second_route_before_survey_board, 
         first_route_after_survey_alight, second_route_after_survey_alight, 
         survey_starttime, Origin_TAZ, Destination_TAZ, Home_TAZ, 
         Work_TAZ, School_TAZ, Origin_MAZ, Destination_MAZ, Home_MAZ, Work_MAZ, School_MAZ)


# AC Transit

AC2 <- left_join(AC,AC_origin2,by="id")
AC2 <- left_join(AC2,AC_destination2,by="id")
AC2 <- left_join(AC2,AC_home2,by="id")
AC2 <- left_join(AC2,AC_work2,by="id")
AC2 <- left_join(AC2,AC_school2,by="id")
AC2 <- left_join(AC2,AC_hotel2,by="id") %>%
  select(id, survey_type, day_type, completed_date, route_surveyed_code, 
         route, change_to_route_surveyed, reason_for_change_to_route_surveyed, 
         final_route_surveyed_code, final_route_surveyed, resident_or_visitor_code, 
         resident_or_visitor, origin_place_type_code, 
         origin_place_type, origin_place_type._other, change_to_origin_type_place, 
         reason_for_change_to_origin_type_place, suggestion_for_change_to_origin_type_place_code, 
         suggestion_for_change_to_origin_type_place, suggestion_for_change_to_origin_type_place_other, 
         final_change_to_origin_type_place, final_reason_for_change_to_origin_type_place, 
         final_suggested_origin_type_place_code, orig_purp, prev_transfers_code, 
         prev_transfers, change_to_prev_transfers, reason_change_to_prev_transfers, 
         final_change_to_prev_transfers, final_reason_change_to_prev_transfers, 
         number_transfers_orig_board, final_suggested_prev_transfers, 
         transfer_from_first_code, first_route_before_survey_board, 
         first_route_before_survey_board_other, change_to_transfer_from_first, 
         reason_for_change_to_transfer_from_first, final_trip_first_route.code., 
         final_trip_first_route, final_trip_first_route_other, transfer_from_second_code, 
         second_route_before_survey_board, second_route_before_survey_board_other, 
         change_to_transfer_from_second, reason_for_change_to_transfer_from_second, 
         final_trip_second_route.code., final_trip_second_route, final_trip_second_route_other, 
         transfer_from_third_code, third_route_before_survey_board, 
         third_route_before_survey_board_other, change_to_transfer_from_third, 
         reason_for_change_to_transfer_from_third, final_trip_third_route.code., 
         final_trip_third_route, final_trip_third_route_other, transfer_from_fourth_code, 
         transfer_from_fourth, transfer_from_fourth_other, change_to_transfer_from_fourth, 
         reason_for_change_to_transfer_from_fourth, final_trip_fourth_route.code., 
         final_trip_fourth_route, access_mode_code, access_mode, 
         access_mode_other, if_change_to_access_mode, change_to_access_mode_code, 
         change_to_access_mode, change_to_access_mode_other, final_reason_for_change_to_access_mode, 
         final_suggested_access_mode_code, final_suggested_access_mode, 
         origin_walk_dist_code, origin_walk_dist, destin_place_type_code, 
         destin_place_type, destin_place_type._other, change_to_destin_type_place, 
         reason_for_change_to_destin_type_place, suggestion_for_change_to_destin_type_place_code, 
         suggestion_for_change_to_destin_type_place, suggestion_for_change_to_destin_type_place_other, 
         final_change_to_destin_type_place, final_reason_for_change_to_destin_type_place, 
         final_suggested_destin_type_place_code, dest_purp,next_transfers_code, 
         next_transfers, change_to_next_transfers, reason_for_change_to_next_transfers, 
         final_change_to_next_transfers, final_reason_for_change_to_next_transfers, 
         number_transfers_alight_dest, final_suggested_next_transfers, 
         transfer_to_first_code, first_route_after_survey_alight, 
         first_route_after_survey_alight_other, change_to_transfer_to_first, 
         reason_for_change_to_transfer_to_first, final_trip_to_first_route.code., 
         final_trip_to_first_route, final_trip_to_first_route_other, 
         transfer_to_second_code, second_route_after_survey_alight, 
         second_route_after_survey_alight_other, change_to_transfer_to_second, 
         reason_for_change_to_transfer_to_second, final_trip_to_second_route.code., 
         final_trip_to_second_route, final_trip_to_second_route_other, 
         transfer_to_third_code, third_route_after_survey_alight, 
         third_route_after_survey_alight_other, change_to_transfer_to_third, 
         reason_for_change_to_transfer_to_third, final_trip_to_third_route.code., 
         final_trip_to_third_route, transfer_to_fourth_code, transfer_to_fourth, 
         transfer_to_fourth_other, change_to_transfer_to_fourth, reason_for_change_to_transfer_to_fourth, 
         final_trip_to_fourth_route.code., final_trip_to_fourth_route, 
         egress_mode_code, egress_mode, egress_mode_other, if_change_to_egress_mode, 
         change_to_egress_mode_code, change_to_egress_mode, change_to_egress_mode_other, 
         final_reason_for_change_to_egress_mode, final_suggested_egress_mode_code, 
         final_suggested_egress_mode, destin_walk_dist_code, destin_walk_dist, 
         boarding_location, boarding_stop_id, survey_board_lat, 
         survey_board_lon, change_to_boarding_location, reason_for_change_to_boarding_location, 
         final_boarding_location, final_boarding_stop_id, final_boarding_lat, 
         final_boarding_lon, alighting_location, alighting_stop_id, 
         survey_alight_lat, survey_alight_lon, change_to_alighting_location, 
         reason_for_change_to_alighting_location, final_alighting_location, 
         final_alighting_stop_id, final_alighting_lat, final_alighting_lon, 
         transfer_from_first_boarding_stopid, transfer_from_first_boarding_lat, 
         transfer_from_first_boarding_lon, change_to_transfer_from_first_boarding, 
         reason_for_change_to_transfer_from_first_boarding, final_transfer_from_first_boarding_stopid, 
         final_transfer_from_first_boarding_lat, final_transfer_from_first_boarding_lon, 
         transfer_from_first_alighting_stopid, transfer_from_first_alighting_lat, 
         transfer_from_first_alighting_lon, change_to_transfer_from_first_alighting, 
         reason_for_change_to_transfer_from_first_alighting, final_transfer_from_first_alighting_stopid, 
         final_transfer_from_first_alighting_lat, final_transfer_from_first_alighting_lon, 
         transfer_from_second_boarding_stopid, transfer_from_second_boarding_lat, 
         transfer_from_second_boarding_lon, change_to_transfer_from_second_boarding, 
         reason_for_change_to_transfer_from_second_boarding, final_transfer_from_second_boarding_stopid, 
         final_transfer_from_second_boarding_lat, final_transfer_from_second_boarding_lon, 
         transfer_from_second_alighting_stopid, transfer_from_second_alighting_lat, 
         transfer_from_second_alighting_lon, change_to_transfer_from_second_alighting, 
         reason_for_change_to_transfer_from_second_alighting, final_transfer_from_second_alighting_stopid, 
         final_transfer_from_second_alighting_lat, final_transfer_from_second_alighting_lon, 
         transfer_from_third_boarding_stopid, transfer_from_third_boarding_lat, 
         transfer_from_third_boarding_lon, change_to_transfer_from_third_boarding, 
         reason_for_change_to_transfer_from_third_boarding, final_transfer_from_third_boarding_stopid, 
         final_transfer_from_third_boarding_lat, final_transfer_from_third_boarding_lon, 
         transfer_from_third_alighting_stopid, transfer_from_third_alighting_lat, 
         transfer_from_third_alighting_lon, change_to_transfer_from_third_alighting, 
         reason_for_change_to_transfer_from_third_alighting, final_transfer_from_third_alighting_stopid, 
         final_transfer_from_third_alighting_lat, final_transfer_from_third_alighting_lon, 
         transfer_from_fourth_boarding_stopid, transfer_from_fourth_boarding_lat, 
         transfer_from_fourth_boarding_lon, change_to_transfer_from_fourth_boarding, 
         reason_for_change_to_transfer_from_fourth_boarding, final_transfer_from_fourth_boarding_stopid, 
         final_transfer_from_fourth_boarding_lat, final_transfer_from_fourth_boarding_lon, 
         transfer_from_fourth_alighting_stopid, transfer_from_fourth_alighting_lat, 
         transfer_from_fourth_alighting_lon, change_to_transfer_from_fourth_alighting, 
         reason_for_change_to_transfer_from_fourth_alighting, final_transfer_from_fourth_alighting_stopid, 
         final_transfer_from_fourth_alighting_lat, final_transfer_from_fourth_alighting_lon, 
         transfer_to_first_boarding_stopid, transfer_to_first_boarding_lat, 
         transfer_to_first_boarding_lon, change_to_transfer_to_first_boarding, 
         reason_for_change_to_transfer_to_first_boarding, final_transfer_to_first_boarding_stopid, 
         final_transfer_to_first_boarding_lat, final_transfer_to_first_boarding_lon, 
         transfer_to_first_alighting_stopid, transfer_to_first_alighting_lat, 
         transfer_to_first_alighting_lon, change_to_transfer_to_first_alighting, 
         reason_for_change_to_transfer_to_first_alighting, final_transfer_to_first_alighting_stopid, 
         final_transfer_to_first_alighting_lat, final_transfer_to_first_alighting_lon, 
         transfer_to_second_boarding_stopid, transfer_to_second_boarding_lat, 
         transfer_to_second_boarding_lon, change_to_transfer_to_second_boarding, 
         reason_for_change_to_transfer_to_second_boarding, final_transfer_to_second_boarding_stopid, 
         final_transfer_to_second_boarding_lat, final_transfer_to_second_boarding_lon, 
         transfer_to_second_alighting_stopid, transfer_to_second_alighting_lat, 
         transfer_to_second_alighting_lon, change_to_transfer_to_second_alighting, 
         reason_for_change_to_transfer_to_second_alighting, final_transfer_to_second_alighting_stopid, 
         final_transfer_to_second_alighting_lat, final_transfer_to_second_alighting_lon, 
         transfer_to_third_boarding_stopid, transfer_to_third_boarding_lat, 
         transfer_to_third_boarding_lon, change_to_transfer_to_third_boarding, 
         reason_for_change_to_transfer_to_third_boarding, final_transfer_to_third_boarding_stopid, 
         final_transfer_to_third_boarding_lat, final_transfer_to_third_boarding_lon, 
         transfer_to_third_alighting_stopid, transfer_to_third_alighting_lat, 
         transfer_to_third_alighting_lon, change_to_transfer_to_third_alighting, 
         reason_for_change_to_transfer_to_third_alighting, final_transfer_to_third_alighting_stopid, 
         final_transfer_to_third_alighting_lat, final_transfer_to_third_alighting_lon, 
         transfer_to_fourth_boarding_stopid, transfer_to_fourth_boarding_lat, 
         transfer_to_fourth_boarding_lon, change_to_transfer_to_fourth_boarding, 
         reason_for_change_to_transfer_to_fourth_boarding, final_transfer_to_fourth_boarding_stopid, 
         final_transfer_to_fourth_boarding_lat, final_transfer_to_fourth_boarding_lon, 
         transfer_to_fourth_alighting_stopid, transfer_to_fourth_alighting_lat, 
         transfer_to_fourth_alighting_lon, change_to_transfer_to_fourth_alighting, 
         reason_for_change_to_transfer_to_fourth_alighting, final_transfer_to_fourth_alighting_stopid, 
         final_transfer_to_fourth_alighting_lat, final_transfer_to_fourth_alighting_lon, 
         time_left_home_code, depart_hour, time_arrive_home_code, 
         return_hour, time_on_code, time_on, time_period, trip_in_oppo_dir_code, 
         trip_in_oppo_dir, oppo_dir_trip_time_code, oppo_dir_trip_time, 
         kind_of_fare_code, kind_of_fare, kind_of_fare_other, fare_category_code, 
         fare_category, fare_category._other, how_paid_for_trip_code, 
         fare_medium, pm1_clipper_code, clipper_detail, pm1_clipper._other, 
         pm2_cash_or_paper_code, pm2_cash_or_paper, pm2_cash_or_paper._other, 
         pm3_by_other_code, pm3_by_other, pm3_by_other_other, fare_related_changes, 
         reasons_for_fare_related_changes, final_suggested_pm3_by_other_other, 
         total_actransit_used_code, total_actransit_used, ride_act_frequency_code, 
         ride_act_frequency, other_travel_means_1, other_travel_means_2, 
         other_travel_means_3, other_travel_means_4, other_travel_means_5, 
         other_travel_means_6, other_travel_means_7, other_travel_means_8, 
         other_travel_means_9, other_travel_means_10, other_travel_means_11, 
         other_travel_means_other, alt_payment_method_1, alt_payment_method_2, 
         alt_payment_method_3, alt_payment_method_4, alt_payment_method_5, 
         alt_payment_method_6, alt_payment_method_other, count_vh_hh_code, 
         vehicles, smart_phone_code, smart_phone, data_internet_code, 
         data_internet, count_member_hh_code, persons, count_employed_hh_code, 
         count_employed_hh, change_to_count_employed_hh, reason_for_change_to_count_employed_hh, 
         final_suggested_count_employed_hh_code, workers, status_employment_code, 
         work_status, worked_before_trip_code, 
         at_work_prior_to_orig_purp, work_after_trip_code, at_work_after_dest_purp, 
         student_status_code, student_status, student_status._other, 
         change_to_student_status_code, reason_for_change_to_student_status_code, 
         final_suggested_student_status_code, final_suggested_student_status, 
         final_suggested_student_status_other, school_type_code, school_type, 
         went2schl_b4_trip_code, at_school_prior_to_orig_purp, go2schl_aftr_trip_code, 
         at_school_after_dest_purp, year_born_four_digit, race_dmy_ltn, race_dmy_blk, race_dmy_asn, 
         race_dmy_mdl_estn, race_dmy_amcn_ind, race_dmy_hwi, race_dmy_whi, 
         race_other_string, change_to_race_ethnicity_columns, reason_for_change_to_race_ethnicity_columns, 
         final_suggested_race_other_string, gender_code, gender, 
         income_code, household_income, home_lang_other_code, language_at_home_binary, 
         home_other_lang_code, language_at_home_detail, home_other_lang._other, 
         english_ability_code, eng_proficient, register_to_win_y_n_code, 
         register_to_win_y_n, first_transit_boarding_lat, first_transit_boarding_lon, 
         last_transit_alighting_lat, last_transit_alighting_lon, first_board_lat, 
         first_board_lon, last_alight_lat, last_alight_lon, survey_language.code., 
         survey_language, race_ethnicity_single_value, weight_factor_name, 
         unlinked_weight_factor, system_transfers, link_multiplier_factor, 
         linked_weight_factor, imputed.income..code., imputed.income..description., 
         title_vi_unlinked_wght, title_vi_linked_wght, Origin_TAZ, Destination_TAZ, Home_TAZ, 
         Work_TAZ, School_TAZ, Hotel_TAZ, Origin_MAZ, Destination_MAZ, Home_MAZ, Work_MAZ, School_MAZ,
         Hotel_MAZ)

# Write out final CSV files

write.csv(ACE2,paste0(output_location,"ACE 2019.csv"),row.names = FALSE)
write.csv(UC2,paste0(output_location,"Union City 2017.csv"),row.names = FALSE)
write.csv(AC2,paste0(output_location,"AC Transit 2018.csv"),row.names = FALSE)


