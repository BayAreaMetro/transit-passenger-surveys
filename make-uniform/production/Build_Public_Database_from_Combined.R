# Build Public Database from Combined.R
# What this script does:
"
1. Brings in combined (legacy + standard) dataset
2. Associates each operator with its technology
3. Removes weekend records and dummy records (placeholder weight values with no characteristic data. Created
   for some operators when ridership data shows a station-to-station combination that wasn't found in the survey.
   An example would be something like a BART trip from North Concord to Castro Valley - rare, but not impossible.)
4. Remove older instances of surveys conducted more than once, with the first survey instance being before 2015.
5. Create aggregate tour purposes. 
6. Create access/egress mode imputation variables - not ultimately used outside of a modeling context. 
7. Name operators in a consistent way, particularly multi-modal operators - e.g., SamTrans [Local]
8. Populate values for transfer_from and transfer_to operator technology.
9. Update 'period' variable with abbreviated names from day_part 
10. Subset variables for export
11. Perform spatial match of lat/long variables to census tract geography
12. Append census tract information, remove lat/long variables, and export dataset to R and CSV versions
"

# Get rid of scientific notation

options(scipen = 99999)

#=========================================================================================================================
# DIRECTORIES AND LIBRARIES
#=========================================================================================================================

TPS_Dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data"
today = Sys.Date()

suppressMessages(library(tidyverse))
library(reshape2)
library(tidyverse)
library(sf)
library(tigris)

# Use megaregion for census spatial aggregation geographies

megaregion <- c("Alameda","Contra Costa","Marin","Napa","San Francisco","San Mateo","Santa Clara","Solano","Sonoma",
                "Santa Cruz","San Benito","Monterey","San Joaquin","Stanislaus","Merced","Yuba","Placer","El Dorado",
                "Sutter","Yolo","Sacramento","Lake","Mendocino")

#=========================================================================================================================
# READ INPUTS
#=========================================================================================================================

# Read TPS dataset survey data
load(file.path(TPS_Dir,     "survey_combined_2024-03-04.RData"))

#=========================================================================================================================
# DEFINITIONS
#=========================================================================================================================

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

SeedIDs <- c(1)


#=========================================================================================================================
# DATA CLEANING, IMPUTATION & TRASNFORMATION
#=========================================================================================================================

# Remove weekend records, all older vintages of operators surveyed more than once
# Also remove "dummy records" (BART, Caltrain, Muni) used for weighting purposes but lacking characteristics
#------------------------
TPS <- survey_combine %>% filter(weekpart=="WEEKDAY" & 
                               !(operator %in% c("AC Transit", "ACE", "County Connection", 
                                                 "Golden Gate Transit", "LAVTA", "Napa Vine", 
                                                 "Petaluma Transit", "Santa Rosa CityBus", 
                                                 "SF Bay Ferry/WETA", "Sonoma County Transit", 
                                                 "TriDelta", "Union City Transit") & survey_year<2015)) %>% 
                              filter(access_mode!="Missing - Dummy Record" | is.na(access_mode)) 

#Aggregate tour purposes
#-------------------------
TPS <- TPS %>%
  mutate(agg_tour_purp = -9) %>% 
  # 1[Work]: work, work-related
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'work' | tour_purp == 'work-related'), 1, agg_tour_purp)) %>% 
  # 2[University]: university, college
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'university' | tour_purp == 'college'), 2, agg_tour_purp)) %>% 
  # 3[School]: school, grade school, high school
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'school' | tour_purp == 'high school' | tour_purp == 'grade school'), 3, agg_tour_purp)) %>% 
  # 4[Maintenance]: escorting, shopping, other maintenace
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'escorting' | tour_purp == 'shopping' | tour_purp == 'other maintenance'), 4, agg_tour_purp)) %>% 
  # 5[Discretionary]: social recreation, eat out, discretionary
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'social recreation' | tour_purp == 'eat out' | tour_purp == 'other discretionary'), 5, agg_tour_purp)) %>% 
  # 6[At-work]: At work
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tour_purp == 'at work'), 6, agg_tour_purp))

# Create new access/egress variables just for modeling, recoding bike as "knr" and recoding missing in a predictable way
# Create new auto sufficiency variable for imputation
#-------------------------

TPS <- TPS %>% 
  mutate(access_mode_imputed=access_mode,
         egress_mode_imputed=egress_mode,
         auto_suff_imputed=auto_suff) 

TPS <- TPS %>% 
  mutate_at(.,vars(access_mode_imputed,egress_mode_imputed),~case_when(
    .=="other"~                          "missing",
    .=="."~                              "missing",
    .=="Missing - Question Not Asked"~   "missing",
    .=="Unknown"~                        "missing",
    is.na(.)~                            "missing",
    TRUE~                                       .)) 

# Summarize operator by access mode

operator_access_mode <- xtabs(trip_weight~operator+access_mode_imputed, data = TPS[TPS$access_mode_imputed!="missing", ])
operator_access_mode <- data.frame(operator_access_mode)
molten <- melt(operator_access_mode, id = c("operator", "access_mode_imputed"))
operator_access_mode <- dcast(molten, operator~access_mode_imputed, sum)

# Create additional access mode variables (totals and shares) for later application

operator_access_mode$tot <- operator_access_mode$walk+operator_access_mode$knr+operator_access_mode$pnr+operator_access_mode$tnc
operator_access_mode$w <- operator_access_mode$walk/operator_access_mode$tot
operator_access_mode$k <- operator_access_mode$knr/operator_access_mode$tot
operator_access_mode$p <- operator_access_mode$pnr/operator_access_mode$tot
operator_access_mode$t <- operator_access_mode$tnc/operator_access_mode$tot
operator_access_mode$c1 <- operator_access_mode$w
operator_access_mode$c2 <- operator_access_mode$w+operator_access_mode$k
operator_access_mode$c3 <- operator_access_mode$w+operator_access_mode$k+operator_access_mode$t 

# Create simple imputation for missing access mode values based on random number generation and prevailing access modes 

returnAccessMode <- function(op)
{
  c1 <- operator_access_mode$c1[operator_access_mode$operator==op]
  c2 <- operator_access_mode$c2[operator_access_mode$operator==op]
  c3 <- operator_access_mode$c3[operator_access_mode$operator==op]
  r <- runif(1)
  return(case_when(
    r<c1 ~          "walk",
    r>=c1 & r<c2  ~ "knr",
    r>=c2 & r<c3 ~  "tnc",
    r>=c3 ~         "pnr",
    TRUE ~          "error"))
}

TPS$access_mode_imputed[TPS$access_mode_imputed=="missing"] <- sapply(as.character(TPS$operator[TPS$access_mode_imputed=="missing"]),function(x) {returnAccessMode(x)} )

# Now do the same thing for egress modes as is done above for access modes

TPS <- TPS %>%
  mutate(egress_mode_imputed = ifelse(is.na(egress_mode_imputed), "missing", egress_mode_imputed))
operator_egress_mode <- xtabs(trip_weight~operator+egress_mode_imputed, data = TPS[TPS$egress_mode_imputed!="missing", ])
operator_egress_mode <- data.frame(operator_egress_mode)
molten <- melt(operator_egress_mode, id = c("operator", "egress_mode_imputed"))
operator_egress_mode <- dcast(molten, operator~egress_mode_imputed, sum)

operator_egress_mode$tot <- operator_egress_mode$walk+operator_egress_mode$knr+operator_egress_mode$pnr+operator_egress_mode$tnc
operator_egress_mode$w <- operator_egress_mode$walk/operator_egress_mode$tot
operator_egress_mode$k <- operator_egress_mode$knr/operator_egress_mode$tot
operator_egress_mode$p <- operator_egress_mode$pnr/operator_egress_mode$tot
operator_egress_mode$t <- operator_egress_mode$tnc/operator_egress_mode$tot
operator_egress_mode$c1 <- operator_egress_mode$w
operator_egress_mode$c2 <- operator_egress_mode$w+operator_egress_mode$k
operator_egress_mode$c3 <- operator_egress_mode$w+operator_egress_mode$k+operator_egress_mode$t

returnEgressMode <- function(op)
{
  c1 <- operator_egress_mode$c1[operator_egress_mode$operator==op]
  c2 <- operator_egress_mode$c2[operator_egress_mode$operator==op]
  c3 <- operator_egress_mode$c3[operator_egress_mode$operator==op]
  r <- runif(1)
  return(case_when(
    r<c1 ~          "walk",
    r>=c1 & r<c2  ~ "knr",
    r>=c2 & r<c3 ~  "tnc",
    r>=c3 ~         "pnr",
    TRUE ~          "error"))
}

TPS$egress_mode_imputed[TPS$egress_mode_imputed=="missing"] <- sapply(as.character(TPS$operator[TPS$egress_mode_imputed=="missing"]),function(x) {returnEgressMode(x)} )

# Auto Sufficiency
#-----------------
# Code missing auto sufficiency, including imputation for missing values

# Remove Capitol Corridor from the auto_suff imputation, then re-add later below
# Populate imputation variable for Capitol Corridor for later ease of binding
# Capitol Corridor didn't collect workers/vehicles, so there's no basis for imputation here

cap_trigger=0                                        # Set trigger if dataset has Capitol Corridor in it.
if ("Capitol Corridor" %in% unique(TPS$operator)) {
  cap_trigger=1
  capitol <- TPS %>% 
    filter(operator=="Capitol Corridor") %>% 
    mutate(auto_suff_imputed=auto_suff)
  TPS <- TPS %>% 
    filter(!(operator=="Capitol Corridor"))}


TPS <- TPS %>%
  mutate(auto_suff_imputed = ifelse(is.na(auto_suff_imputed) | auto_suff_imputed=="Missing", "missing", auto_suff_imputed))
operator_autoSuff <- xtabs(trip_weight~operator+auto_suff_imputed, data = TPS[TPS$auto_suff_imputed!="missing", ])
operator_autoSuff <- data.frame(operator_autoSuff)
molten <- melt(operator_autoSuff, id = c("operator", "auto_suff_imputed"))
operator_autoSuff <- dcast(molten, operator~auto_suff_imputed, sum)
operator_autoSuff$tot <- operator_autoSuff$`zero autos`+operator_autoSuff$`auto sufficient`+operator_autoSuff$`auto negotiating`
operator_autoSuff$as1 <- operator_autoSuff$`zero autos`/operator_autoSuff$tot
operator_autoSuff$as2 <- operator_autoSuff$`auto negotiating`/operator_autoSuff$tot
operator_autoSuff$as3 <- operator_autoSuff$`auto sufficient`/operator_autoSuff$tot
operator_autoSuff$c1 <- operator_autoSuff$as1
operator_autoSuff$c2 <- operator_autoSuff$as1+operator_autoSuff$as2

returnAS <- function(op)
{
  c1 <- operator_autoSuff$c1[operator_autoSuff$operator==op]
  c2 <- operator_autoSuff$c2[operator_autoSuff$operator==op]
  r <- runif(1)
  return(ifelse(r<c1, "zero autos", ifelse(r<c2, "auto negotiating", "auto sufficient")))
}

TPS$auto_suff_imputed[TPS$auto_suff_imputed=="missing"] <- sapply(as.character(TPS$operator[TPS$auto_suff_imputed=="missing"]),function(x) {returnAS(x)} )

# Add Capitol Corridor back in 

if (cap_trigger==1) {
  TPS <- rbind(TPS,capitol)
  cap_trigger=0}

# Transform survey_tech into simplified values for survey_tech, first_board tech, and last_alight tech
#-----------------------------
TPS$survey_tech <- survey_tech_df$survey_tech_short[match(TPS$survey_tech, survey_tech_df$survey_tech)]
TPS$first_board_tech <- survey_tech_df$survey_tech_short[match(TPS$first_board_tech, survey_tech_df$survey_tech)]
TPS$last_alight_tech <- survey_tech_df$survey_tech_short[match(TPS$last_alight_tech, survey_tech_df$survey_tech)]

# Detailed Operator Coding
#-------------------------
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

TPS <- TPS %>% 
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
           "survey_batch", "agg_tour_purp", "access_mode_imputed", "egress_mode_imputed", 
           "auto_suff_imputed", "nTransfers", "period", "transfer_from_tech", "transfer_to_tech", 
           "orig_lon","orig_lat","first_board_lon","first_board_lat","survey_board_lon","survey_board_lat",
           "survey_alight_lon","survey_alight_lat",
           "last_alight_lon","last_alight_lat","dest_lon","dest_lat","home_lon","home_lat",
           "workplace_lon","workplace_lat","school_lon","school_lat"))

# Spatial match to census tract

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

# Get census tracts for spatially matching. CB=false means that boundaries are not clipped to shoreline
# CB=TRUE often omits locations close to the shoreline
# Remove geometry and join census tract data with survey file
# Find nearest feature within a quarter mile

tracts <- tracts(state = "CA", county = megaregion, cb = FALSE, year = 2020)
tracts_proj <- st_transform(tracts, crs = st_crs(survey_coords_spatial))
matched_tracts_interim <- st_join(survey_coords_spatial, tracts_proj, join = st_nearest_feature)
matched_tracts <- matched_tracts_interim
st_geometry(matched_tracts) <- NULL
matched_tracts <- matched_tracts %>% 
  select(unique_ID,variable,GEOID) %>% 
  pivot_wider(., names_from = variable, values_from = GEOID, values_fill = NA)

final <- left_join(TPS,matched_tracts,by="unique_ID") %>% 
  select(unique_ID, operator, ID, survey_year, SURVEY_MODE, access_mode, 
         access_mode_imputed, depart_hour, dest_purp, direction, egress_mode, 
         egress_mode_imputed, eng_proficient, fare_category, fare_medium, 
         gender, hispanic, household_income, interview_language, onoff_enter_station, 
         onoff_exit_station, orig_purp, persons, return_hour, route, student_status, 
         survey_type, time_period, vehicles, weekpart, work_status, workers, canonical_operator, 
         operator_detail, technology, approximate_age, tour_purp, tour_purp_case, 
         vehicle_numeric_cat, worker_numeric_cat, auto_suff, auto_suff_imputed, 
         first_before_operator_detail, second_before_operator_detail, 
         third_before_operator_detail, first_after_operator_detail, 
         second_after_operator_detail, third_after_operator_detail, 
         first_before_operator, second_before_operator, third_before_operator, 
         first_after_operator, second_after_operator, third_after_operator, 
         first_before_technology, second_before_technology, third_before_technology, 
         first_after_technology, second_after_technology, third_after_technology, 
         transfer_from, transfer_to, first_board_tech, last_alight_tech, 
         boardings, race, language_at_home, day_of_the_week, field_start, field_end, 
         day_part, dest_tm1_taz, home_tm1_taz, orig_tm1_taz, school_tm1_taz, 
         workplace_tm1_taz, dest_tm2_taz, home_tm2_taz, orig_tm2_taz, 
         school_tm2_taz, workplace_tm2_taz, dest_tm2_maz, home_tm2_maz, 
         orig_tm2_maz, school_tm2_maz, workplace_tm2_maz, board_tap, 
         alight_tap, field_language, survey_time, path_access, 
         path_egress, path_line_haul, path_label, first_board_tap, 
         last_alight_tap, survey_batch, agg_tour_purp,  
         nTransfers, period, transfer_from_tech, transfer_to_tech, 
         first_board_lon, first_board_lat, survey_board_lon, survey_board_lat, 
         survey_alight_lon, survey_alight_lat, last_alight_lon, last_alight_lat, 
         dest_tract=dest, home_tract=home, orig_tract=orig, school_tract=school, 
         workplace_tract=workplace, first_board_tract=first_board, last_alight_tract=last_alight, 
         survey_board_tract=survey_board, survey_alight_tract=survey_alight,weight, trip_weight)
write.csv(final, file.path(TPS_Dir, "public_version", paste0("TPS_Public_Version_",today,".csv")), row.names = F)
save(final, file=file.path(TPS_Dir, "public_version", paste0("TPS_Public_Version_",today,".Rdata")))




