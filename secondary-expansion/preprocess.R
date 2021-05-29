###########################################################################################################################
### Script to process MTC TPS Database and produce inputs for weighting using PopualtionSim
###
### Author: Binny M Paul, July 2019
### Amended by Shimon Israel, May 2021
###########################################################################################################################
oldw <- getOption("warn")
#options(warn = -1)                      # Ignore all warnings

#=========================================================================================================================
# DIRECTORIES AND LIBRARIES
#=========================================================================================================================

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_drive_dir        <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
TPS_Dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/"
POPSIM_Dir <- paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")
BOX_TM2_Dir     <- file.path(box_drive_dir, "Development", "Travel Model Two Development")
TPS_Anc_Dir     <- file.path(BOX_TM2_Dir, "Observed Data", "Transit", "Onboard Survey", "Data")
TARGETS_Dir     <- file.path(BOX_TM2_Dir, "Observed Data", "Transit", "Scaled Transit Ridership Targets")
VALIDATION_Dir  <- file.path(POPSIM_Dir, "validation")

suppressMessages(library(tidyverse))
library(reshape2)

#=========================================================================================================================
# READ INPUTS
#=========================================================================================================================

# Read TPS dataset survey data
load(file.path(TPS_Dir,     "survey_combined_2021-05-28.RData"))

# Read in target boardings for 2015, with directory coming from Secondary Expansion.Rmd file
boarding_targets <- read.csv(file.path(TARGETS_Dir, "transitRidershipTargets2015.csv"), header = TRUE, stringsAsFactors = FALSE) 

#=========================================================================================================================
# DEFINITIONS
#=========================================================================================================================

# Create operator equivalency with technology

#operator = c("ACE",               "AC TRANSIT",        "AIR BART",         "AMTRAK",              "BART",             
#             "CALTRAIN",          "COUNTY CONNECTION", "FAIRFIELD-SUISUN", "GOLDEN GATE TRANSIT", "GOLDEN GATE FERRY", 
#             "MARIN TRANSIT",     "MUNI",              "NAPA VINE",        "RIO-VISTA",           "SAMTRANS",
#             "SANTA ROSA CITYBUS","SF BAY FERRY",      "SOLTRANS",          "TRI-DELTA",          "UNION CITY",          
#             "WESTCAT",           "VTA",               "OTHER",             "PRIVATE SHUTTLE",  "OTHER AGENCY",        
#             "BLUE GOLD FERRY", "None", "WHEELS (LAVTA)", "MODESTO TRANSIT", "BLUE & GOLD FERRY", 
#             "DUMBARTON EXPRESS", "EMERY-GO-ROUND", "PETALUMA TRANSIT", "SANTA ROSA CITY BUS", "SONOMA COUNTY TRANSIT", 
#             "STANFORD SHUTTLES", "VALLEJO TRANSIT", "SAN JOAQUIN TRANSIT")
#technology = c("CR", "LB", "LB", "CR", "HR", 
#               "CR", "LB", "LB", "EB", "FR",      
#               "LB", "LB", "LB", "LB", "LB",
#               "LB", "FR", "LB", "LB", "LB",     
#               "LB", "LB", "LB", "LB", "LB",     
#               "FR", "None", "LB", "LB", "FR", 
#               "EB", "LB", "LB", "LB", "LB", 
#               "LB", "LB", "LB")

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

# Remove weekend records, all older vintages of operators surveyed more than once, and SMART (not in 2015 network)
# Also remove "dummy records" (BART, Caltrain, Muni) used for weighting purposes but lacking characteristics
#------------------------
temp1 <- data.ready %>% filter(weekpart=="WEEKDAY" & 
                               !(operator %in% c("AC Transit", "ACE", "County Connection", 
                                                 "Golden Gate Transit", "LAVTA", "Napa Vine", 
                                                 "Petaluma Transit", "Santa Rosa CityBus", 
                                                 "SF Bay Ferry/WETA", "Sonoma County Transit", 
                                                 "TriDelta", "Union City Transit") & survey_year<2015)) %>% 
                              filter(!(operator=="SMART")) %>% 
                              mutate(dummy=if_else(access_mode!="Missing - Dummy Record" | is.na(access_mode),0,1)) 

temp1 <- temp1 %>% filter(dummy !=1)


# Remove Capitol Corridor and ACE Records that start and/or end outside the Bay Area
# Create flag then later remove records based on flag

temp2 <- temp1 %>% 
  mutate(flag=if_else(operator=="Capitol Corridor" & 
                        !(onoff_enter_station %in% c("Jack London Square", "Berkeley", "Suisun-fairfield", 
                                    "Emeryville", "Fairfield/Vacaville Station", "Martinez", "San Jose", 
                                    "Richmond", "Santa Clara University", "Santa Clara Great America", 
                                    "Fremont", "Hayward", "Oakland Coliseum") & 
                          onoff_exit_station %in% c("Jack London Square", "Berkeley", "Suisun-fairfield", 
                                     "Emeryville", "Fairfield/Vacaville Station", "Martinez", "San Jose", 
                                     "Richmond", "Santa Clara University", "Santa Clara Great America", 
                                     "Fremont", "Hayward", "Oakland Coliseum")), 1, 
                        if_else(operator=="ACE" &
                                (onoff_enter_station %in% c("Stockton Station", "Lathrop/Manteca Station",
                                                            "Tracy Station") |
                                 onoff_exit_station %in% c("Stockton Station", "Lathrop/Manteca Station",
                                                           "Tracy Station")),1,0)))

TPS <- temp2 %>% 
  filter(flag !=1) %>% 
  select(-flag)

remove(data.ready,temp1, temp2)

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
  mutate(access_mode_model=access_mode,
         egress_mode_model=egress_mode,
         auto_suff_model=auto_suff) 

TPS <- TPS %>% 
  mutate_at(.,vars(access_mode_model,egress_mode_model),~case_when(
            .=="bike"~                           "knr",
            .=="tnc"~                            "knr",
            .=="other"~                          "missing",
            .=="."~                              "missing",
            .=="Missing - Question Not Asked"~  "missing",
            .=="Unknown"~                       "missing",
            is.na(.)~                           "missing",
            TRUE~                                       .)) 

# Summarize operator by access mode

operator_access_mode <- xtabs(trip_weight~operator+access_mode_model, data = TPS[TPS$access_mode_model!="missing", ])
operator_access_mode <- data.frame(operator_access_mode)
molten <- melt(operator_access_mode, id = c("operator", "access_mode_model"))
operator_access_mode <- dcast(molten, operator~access_mode_model, sum)

# Create additional access mode variables (totals and shares) for later application

operator_access_mode$tot <- operator_access_mode$walk+operator_access_mode$knr+operator_access_mode$pnr
operator_access_mode$w <- operator_access_mode$walk/operator_access_mode$tot
operator_access_mode$k <- operator_access_mode$knr/operator_access_mode$tot
operator_access_mode$p <- operator_access_mode$pnr/operator_access_mode$tot
operator_access_mode$c1 <- operator_access_mode$w
operator_access_mode$c2 <- operator_access_mode$w+operator_access_mode$k

# Create simple imputation for missing access mode values based on random number generation and prevailing access modes 

returnAccessMode <- function(op)
{
  c1 <- operator_access_mode$c1[operator_access_mode$operator==op]
  c2 <- operator_access_mode$c2[operator_access_mode$operator==op]
  r <- runif(1)
  return(ifelse(r<c1, "walk", ifelse(r<c2, "knr", "pnr")))
}

TPS$access_mode_model[TPS$access_mode_model=="missing"] <- sapply(as.character(TPS$operator[TPS$access_mode_model=="missing"]),function(x) {returnAccessMode(x)} )

# Now do the same thing for egress modes as is done above for access modes

TPS <- TPS %>%
  mutate(egress_mode_model = ifelse(is.na(egress_mode_model), "missing", egress_mode_model))
operator_egress_mode <- xtabs(trip_weight~operator+egress_mode_model, data = TPS[TPS$egress_mode_model!="missing", ])
operator_egress_mode <- data.frame(operator_egress_mode)
molten <- melt(operator_egress_mode, id = c("operator", "egress_mode_model"))
operator_egress_mode <- dcast(molten, operator~egress_mode_model, sum)
operator_egress_mode$tot <- operator_egress_mode$walk+operator_egress_mode$knr+operator_egress_mode$pnr
operator_egress_mode$w <- operator_egress_mode$walk/operator_egress_mode$tot
operator_egress_mode$k <- operator_egress_mode$knr/operator_egress_mode$tot
operator_egress_mode$p <- operator_egress_mode$pnr/operator_egress_mode$tot
operator_egress_mode$c1 <- operator_egress_mode$w
operator_egress_mode$c2 <- operator_egress_mode$w+operator_egress_mode$k

returnEgressMode <- function(op)
{
  c1 <- operator_egress_mode$c1[operator_egress_mode$operator==op]
  c2 <- operator_egress_mode$c2[operator_egress_mode$operator==op]
  r <- runif(1)
  return(ifelse(r<c1, "walk", ifelse(r<c2, "knr", "pnr")))
}

TPS$egress_mode_model[TPS$egress_mode_model=="missing"] <- sapply(as.character(TPS$operator[TPS$egress_mode_model=="missing"]),function(x) {returnEgressMode(x)} )

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
    mutate(auto_suff_model=auto_suff)
  TPS <- TPS %>% 
    filter(!(operator=="Capitol Corridor"))}


TPS <- TPS %>%
  mutate(auto_suff_model = ifelse(is.na(auto_suff_model) | auto_suff_model=="Missing", "missing", auto_suff_model))
operator_autoSuff <- xtabs(trip_weight~operator+auto_suff_model, data = TPS[TPS$auto_suff_model!="missing", ])
operator_autoSuff <- data.frame(operator_autoSuff)
molten <- melt(operator_autoSuff, id = c("operator", "auto_suff_model"))
operator_autoSuff <- dcast(molten, operator~auto_suff_model, sum)
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

TPS$auto_suff_model[TPS$auto_suff_model=="missing"] <- sapply(as.character(TPS$operator[TPS$auto_suff_model=="missing"]),function(x) {returnAS(x)} )

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

## copy technology from the targets database
#TPS <- merge(x=TPS, y=boarding_targets[boarding_targets$surveyed==1,c("operator","technology")], by="operator", all.x = TRUE)

#=========================================================================================================================
# FILTER RECORDS WITH MISSING DATA
#=========================================================================================================================

# Missing Tour Purpose
#TPS <- TPS[TPS$agg_tour_purp>0,]



#=========================================================================================================================
# FACTOR WEIGHTS TO MATCH 2015 RIDERSHIP TARGETS
#=========================================================================================================================

# Factor weights to match 2015 targets for surveyed operators
#-------------------------------------------------------------

# Sum survey TPS boardings by operator
TPS_operator_totals <- aggregate(TPS$weight, by = list(operator = TPS$operator), sum)
TPS_operator_totals <- data.frame(TPS_operator_totals)
colnames(TPS_operator_totals) <- c("operator", "boardWeight")

# Sum target 2015 boards by operator
target_operator_totals <- aggregate(boarding_targets$targets2015, by = list(operator = boarding_targets$operator), sum)
target_operator_totals <- data.frame(target_operator_totals)
colnames(target_operator_totals) <- c("operator", "targetBoardings")

# Append target totals to operator totals into new data frame and calculate expansion factor
expansion_factors <- TPS_operator_totals
expansion_factors$targetBoardings <- target_operator_totals$targetBoardings[match(expansion_factors$operator, target_operator_totals$operator)]

expansion_factors <- expansion_factors %>%
  mutate(exp_factor = targetBoardings/boardWeight) 

# Append expansion factor and create new board weight and trip weight variables based on this value
TPS$exp_factor <- expansion_factors$exp_factor[match(TPS$operator, expansion_factors$operator)]
TPS$boardWeight_2015 <- TPS$weight * TPS$exp_factor
TPS$tripWeight_2015 <- TPS$trip_weight * TPS$exp_factor

# Non-surveyed operators to be added later

## Factor weights to match 2015 targets for non-surveyed operators
##-----------------------------------------------------------------
#
## Boarding targets for non-surveyed operators
#non_surveyed_targets <- boarding_targets[boarding_targets$surveyed==0,]
#
## TPS boards by operator
#TPS_tech_totals <- aggregate(TPS$boardWeight_2015, by = list(technology = TPS$technology), sum)
#TPS_tech_totals <- data.frame(TPS_tech_totals)
#colnames(TPS_tech_totals) <- c("technology", "boardWeight")
#
## Target 2015 boards by technology [including non-surveyed operators]
#target_tech_totals <- aggregate(boarding_targets$targets2015, by = list(technology = boarding_targets$technology), sum)
#target_tech_totals <- data.frame(target_tech_totals)
#colnames(target_tech_totals) <- c("technology", "targetBoardings")
#
## Compute expansion factors
#expansion_factors <- TPS_tech_totals
#expansion_factors$targetBoardings <- target_tech_totals$targetBoardings[match(expansion_factors$technology, target_tech_totals$technology)]
#
#expansion_factors <- expansion_factors %>%
#  mutate(exp_factor = targetBoardings/boardWeight) 
#
## Compute final 2015 factored weights [accounting for non-surveyed operators]
#TPS$exp_factor <- expansion_factors$exp_factor[match(TPS$technology, expansion_factors$technology)]
#TPS$boardWeight_2015 <- TPS$boardWeight_2015 * TPS$exp_factor
#TPS$tripWeight_2015 <- TPS$tripWeight_2015 * TPS$exp_factor



#=========================================================================================================================
# CODE VARIABLES FOR PopulatoinSim WEIGHTING
#=========================================================================================================================

# Number of transfers (boardings minus 1)
#------------------------
TPS$nTransfers <- TPS$boardings - 1

# Time period (commented out version from previous iteration)
#----------------
#TPS$period[TPS$depart_hour<6] <- "EA"
#TPS$period[TPS$depart_hour>=6 & TPS$depart_hour<9] <- "AM"
#TPS$period[TPS$depart_hour>=9 & TPS$depart_hour<15] <- "MD"
#TPS$period[TPS$depart_hour>=15 & TPS$depart_hour<19] <- "PM"
#TPS$period[TPS$depart_hour>=19] <- "EV"

TPS$period[TPS$day_part=="EARLY AM"] <- "EA"
TPS$period[TPS$day_part=="AM PEAK"]  <- "AM"
TPS$period[TPS$day_part=="MIDDAY"]   <- "MD"
TPS$period[TPS$day_part=="PM PEAK"]  <- "PM"
TPS$period[TPS$day_part=="EVENING"]  <- "EV"



# BEST Mode for transfer_from and transfer_to tech
#--------------------
TPS$transfer_from_tech <- opTechXWalk$technology[match(TPS$transfer_from, opTechXWalk$operator)]
TPS$transfer_to_tech <- opTechXWalk$technology[match(TPS$transfer_to, opTechXWalk$operator)]

# Code Mode Set Type, creating dummy values (1) for each technology used
TPS <- TPS %>%
  mutate(usedLB = ifelse(first_board_tech=="LB" 
                         | transfer_from_tech=="LB"
                         | survey_tech=="LB"
                         | transfer_to_tech=="LB"
                         | last_alight_tech=="LB",1,0)) %>%
  mutate(usedCR = ifelse(first_board_tech=="CR" 
                         | transfer_from_tech=="CR"
                         | survey_tech=="CR"
                         | transfer_to_tech=="CR"
                         | last_alight_tech=="CR",1,0)) %>%
  mutate(usedHR = ifelse(first_board_tech=="HR" 
                         | transfer_from_tech=="HR"
                         | survey_tech=="HR"
                         | transfer_to_tech=="HR"
                         | last_alight_tech=="HR",1,0)) %>%
  mutate(usedEB = ifelse(first_board_tech=="EB" 
                         | transfer_from_tech=="EB"
                         | survey_tech=="EB"
                         | transfer_to_tech=="EB"
                         | last_alight_tech=="EB",1,0)) %>%
  mutate(usedLR = ifelse(first_board_tech=="LR" 
                         | transfer_from_tech=="LR"
                         | survey_tech=="LR"
                         | transfer_to_tech=="LR"
                         | last_alight_tech=="LR",1,0)) %>%
  mutate(usedFR = ifelse(first_board_tech=="FR" 
                         | transfer_from_tech=="FR"
                         | survey_tech=="FR"
                         | transfer_to_tech=="FR"
                         | last_alight_tech=="FR",1,0))

# Input zero values for NAs
TPS$usedLB[is.na(TPS$usedLB)] <- 0
TPS$usedEB[is.na(TPS$usedEB)] <- 0
TPS$usedLR[is.na(TPS$usedLR)] <- 0
TPS$usedFR[is.na(TPS$usedFR)] <- 0
TPS$usedHR[is.na(TPS$usedHR)] <- 0
TPS$usedCR[is.na(TPS$usedCR)] <- 0

# Total technologies used

TPS$usedTotal <- TPS$usedLB+TPS$usedEB+TPS$usedLR+TPS$usedFR+TPS$usedHR+TPS$usedCR

# Recode used fields based on path line haul variable code

TPS$usedLB[TPS$usedTotal==0 & TPS$path_line_haul=="LOC"] <- 1
TPS$usedEB[TPS$usedTotal==0 & TPS$path_line_haul=="EXP"] <- 1
TPS$usedLR[TPS$usedTotal==0 & TPS$path_line_haul=="LRF"] <- 1
TPS$usedHR[TPS$usedTotal==0 & TPS$path_line_haul=="HVY"] <- 1
TPS$usedCR[TPS$usedTotal==0 & TPS$path_line_haul=="COM"] <- 1

TPS$usedTotal <- TPS$usedLB+TPS$usedEB+TPS$usedLR+TPS$usedFR+TPS$usedHR+TPS$usedCR

# Hierarchy of local bus through commuter rail

TPS$BEST_MODE <- "LB"
TPS$BEST_MODE[TPS$usedEB==1] <- "EB"
TPS$BEST_MODE[TPS$usedFR==1] <- "FR"
TPS$BEST_MODE[TPS$usedLR==1] <- "LR"
TPS$BEST_MODE[TPS$usedHR==1] <- "HR"
TPS$BEST_MODE[TPS$usedCR==1] <- "CR"


#Transfer Types [across all surveys], creating dummy for all types of transfers, applying mode hierarchy
#----------------------

TPS <- TPS %>%
  mutate(LB_CR = ifelse((usedLB==1 & usedCR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LB_HR = ifelse((usedLB==1 & usedHR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LB_LR = ifelse((usedLB==1 & usedLR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LB_FR = ifelse((usedLB==1 & usedFR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LB_EB = ifelse((usedLB==1 & usedEB==1 & nTransfers>0), 1, 0)) %>%
  mutate(LB_LB = ifelse((usedLB==1 & usedTotal==1 & nTransfers>0), 1, 0)) %>%
  mutate(EB_CR = ifelse((usedEB==1 & usedCR==1 & nTransfers>0), 1, 0)) %>%
  mutate(EB_HR = ifelse((usedEB==1 & usedHR==1 & nTransfers>0), 1, 0)) %>%
  mutate(EB_LR = ifelse((usedEB==1 & usedLR==1 & nTransfers>0), 1, 0)) %>%
  mutate(EB_FR = ifelse((usedEB==1 & usedFR==1 & nTransfers>0), 1, 0)) %>%
  mutate(EB_EB = ifelse((usedEB==1 & usedTotal==1 & nTransfers>0), 1, 0)) %>%
  mutate(FR_CR = ifelse((usedFR==1 & usedCR==1 & nTransfers>0), 1, 0)) %>%
  mutate(FR_HR = ifelse((usedFR==1 & usedHR==1 & nTransfers>0), 1, 0)) %>%
  mutate(FR_LR = ifelse((usedFR==1 & usedLR==1 & nTransfers>0), 1, 0)) %>%
  mutate(FR_FR = ifelse((usedFR==1 & usedTotal==1 & nTransfers>0), 1, 0)) %>%
  mutate(LR_CR = ifelse((usedLR==1 & usedCR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LR_HR = ifelse((usedLR==1 & usedHR==1 & nTransfers>0), 1, 0)) %>%
  mutate(LR_LR = ifelse((usedLR==1 & usedTotal==1 & nTransfers>0), 1, 0)) %>%
  mutate(HR_CR = ifelse((usedHR==1 & usedCR==1 & nTransfers>0), 1, 0)) %>%
  mutate(HR_HR = ifelse((usedHR==1 & usedTotal==1 & nTransfers>0), 1, 0)) %>%
  mutate(CR_CR = ifelse((usedCR==1 & usedTotal==1 & nTransfers>0), 1, 0))


TPS$TRANSFER_TYPE <- "OTHER"
TPS$TRANSFER_TYPE[TPS$nTransfers==0] <- "NO_TRANSFERS"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedCR==1 & TPS$nTransfers>0] <- "LB_CR"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedHR==1 & TPS$nTransfers>0] <- "LB_HR"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedLR==1 & TPS$nTransfers>0] <- "LB_LR"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedFR==1 & TPS$nTransfers>0] <- "LB_FR"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedEB==1 & TPS$nTransfers>0] <- "LB_EB"
TPS$TRANSFER_TYPE[TPS$usedLB==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "LB_LB"
TPS$TRANSFER_TYPE[TPS$usedEB==1 & TPS$usedCR==1 & TPS$nTransfers>0] <- "EB_CR"
TPS$TRANSFER_TYPE[TPS$usedEB==1 & TPS$usedHR==1 & TPS$nTransfers>0] <- "EB_HR"
TPS$TRANSFER_TYPE[TPS$usedEB==1 & TPS$usedLR==1 & TPS$nTransfers>0] <- "EB_LR"
TPS$TRANSFER_TYPE[TPS$usedEB==1 & TPS$usedFR==1 & TPS$nTransfers>0] <- "EB_FR"
TPS$TRANSFER_TYPE[TPS$usedEB==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "EB_EB"
TPS$TRANSFER_TYPE[TPS$usedFR==1 & TPS$usedCR==1 & TPS$nTransfers>0] <- "FR_CR"
TPS$TRANSFER_TYPE[TPS$usedFR==1 & TPS$usedHR==1 & TPS$nTransfers>0] <- "FR_HR"
TPS$TRANSFER_TYPE[TPS$usedFR==1 & TPS$usedLR==1 & TPS$nTransfers>0] <- "FR_LR"
TPS$TRANSFER_TYPE[TPS$usedFR==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "FR_FR"
TPS$TRANSFER_TYPE[TPS$usedLR==1 & TPS$usedCR==1 & TPS$nTransfers>0] <- "LR_CR"
TPS$TRANSFER_TYPE[TPS$usedLR==1 & TPS$usedHR==1 & TPS$nTransfers>0] <- "LR_HR"
TPS$TRANSFER_TYPE[TPS$usedLR==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "LR_LR"
TPS$TRANSFER_TYPE[TPS$usedHR==1 & TPS$usedCR==1 & TPS$nTransfers>0] <- "HR_CR"
TPS$TRANSFER_TYPE[TPS$usedHR==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "HR_HR"
TPS$TRANSFER_TYPE[TPS$usedCR==1 & TPS$usedTotal==1 & TPS$nTransfers>0] <- "CR_CR"



#=========================================================================================================================
# RENAME FIELDS
#=========================================================================================================================

names(TPS)[names(TPS)=="survey_tech"] <- "SURVEY_MODE"



#=========================================================================================================================
# PREPARE INPUTS FOR POPULATIONSIM WEIGHTING EXERCISE
#=========================================================================================================================


# MARGINAL CONTROLS
#--------------------
marginalControls <- data.frame(GEOID = SeedIDs)

# Create a matrix of boarding targets by operator
boardingsTargets <- data.frame(xtabs(boardWeight_2015~operator, data = TPS))
boardingsTargets <- data.frame(t(boardingsTargets), stringsAsFactors = F)
colnames(boardingsTargets) <- boardingsTargets[1,]
boardingsTargets <- boardingsTargets[2,]
for (col in names(boardingsTargets)){
  boardingsTargets[[col]] <- as.integer(boardingsTargets[[col]])
}

# Rename column names to convert blank spaces and hyphens to underscores
colnames(boardingsTargets) <- unlist(lapply(colnames(boardingsTargets), function(x)(gsub(" ", "_", x))))
colnames(boardingsTargets) <- unlist(lapply(colnames(boardingsTargets), function(x)(gsub("-", "_", x))))

# Combine all marginal controls
marginalControls <- cbind(marginalControls, boardingsTargets)

# Total linked trips
totalLinkedTrips <- sum(TPS$tripWeight_2015)
marginalControls$totalLinkedTrips <- totalLinkedTrips

# Sum boardings by suvey_mode and transfer combination type

linkedtrips_best_mode_xfer <- TPS %>%
  group_by(SURVEY_MODE) %>%
  summarise(LB_CR = sum(LB_CR * boardWeight_2015), 
            LB_HR = sum(LB_HR * boardWeight_2015), 
            LB_LR = sum(LB_LR * boardWeight_2015), 
            LB_FR = sum(LB_FR * boardWeight_2015), 
            LB_EB = sum(LB_EB * boardWeight_2015), 
            LB_LB = sum(LB_LB * boardWeight_2015),
            EB_CR = sum(EB_CR * boardWeight_2015), 
            EB_HR = sum(EB_HR * boardWeight_2015), 
            EB_LR = sum(EB_LR * boardWeight_2015), 
            EB_FR = sum(EB_FR * boardWeight_2015), 
            EB_EB = sum(EB_EB * boardWeight_2015), 
            FR_CR = sum(FR_CR * boardWeight_2015), 
            FR_HR = sum(FR_HR * boardWeight_2015), 
            FR_LR = sum(FR_LR * boardWeight_2015), 
            FR_FR = sum(FR_FR * boardWeight_2015), 
            LR_CR = sum(LR_CR * boardWeight_2015), 
            LR_HR = sum(LR_HR * boardWeight_2015), 
            LR_LR = sum(LR_LR * boardWeight_2015), 
            HR_CR = sum(HR_CR * boardWeight_2015), 
            HR_HR = sum(HR_HR * boardWeight_2015), 
            CR_CR = sum(CR_CR * boardWeight_2015))

linkedtrips_best_mode_xfer <- data.frame(t(linkedtrips_best_mode_xfer), stringsAsFactors = F)  # Transpose data frame
colnames(linkedtrips_best_mode_xfer) <- linkedtrips_best_mode_xfer[c(1),]                      # Use names from first row
linkedtrips_best_mode_xfer <- linkedtrips_best_mode_xfer[-c(1),]                               # Delete first row
linkedtrips_best_mode_xfer <- cbind(TRANSFER_TYPE = row.names(linkedtrips_best_mode_xfer), linkedtrips_best_mode_xfer) # Append first column using row names

# transfer targets must be from the survey reporting the maximum transfers
#marginalControls$XFERS_LB_EB <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_EB" & TPS$SURVEY_MODE=="EB"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_EB" & TPS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_LR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_LR" & TPS$SURVEY_MODE=="LR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_LR" & TPS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_FR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_FR" & TPS$SURVEY_MODE=="FR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_FR" & TPS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_CR" & TPS$SURVEY_MODE=="CR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_CR" & TPS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_HR" & TPS$SURVEY_MODE=="HR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LB_HR" & TPS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LR_CR" & TPS$SURVEY_MODE=="CR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="LR_CR" & TPS$SURVEY_MODE=="LR"])))
#marginalControls$XFERS_HR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="HR_CR" & TPS$SURVEY_MODE=="CR"]), 
#                                               sum(TPS$boardWeight_2015[TPS$TRANSFER_TYPE=="HR_CR" & TPS$SURVEY_MODE=="HR"])))

# Creating a new variable in the marginal controls data frame with the max sum of 2015 boarding weight between each mode of a transfer combination


marginalControls$XFERS_LB_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_CR==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_HR==1 & TPS$SURVEY_MODE=="HR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_HR==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_LR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_LR==1 & TPS$SURVEY_MODE=="LR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_LR==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_FR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_FR==1 & TPS$SURVEY_MODE=="FR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_FR==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_EB <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_EB==1 & TPS$SURVEY_MODE=="EB"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_EB==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_LB <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LB_LB==1 & TPS$SURVEY_MODE=="LB"]), 
                                               sum(TPS$boardWeight_2015[TPS$LB_LB==1 & TPS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_EB_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$EB_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$EB_CR==1 & TPS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$EB_HR==1 & TPS$SURVEY_MODE=="HR"]), 
                                               sum(TPS$boardWeight_2015[TPS$EB_HR==1 & TPS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_LR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$EB_LR==1 & TPS$SURVEY_MODE=="LR"]), 
                                               sum(TPS$boardWeight_2015[TPS$EB_LR==1 & TPS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_FR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$EB_FR==1 & TPS$SURVEY_MODE=="FR"]), 
                                               sum(TPS$boardWeight_2015[TPS$EB_FR==1 & TPS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_EB <- as.integer(max(sum(TPS$boardWeight_2015[TPS$EB_EB==1 & TPS$SURVEY_MODE=="EB"]), 
                                               sum(TPS$boardWeight_2015[TPS$EB_EB==1 & TPS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_FR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$FR_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$FR_CR==1 & TPS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$FR_HR==1 & TPS$SURVEY_MODE=="HR"]), 
                                               sum(TPS$boardWeight_2015[TPS$FR_HR==1 & TPS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_LR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$FR_LR==1 & TPS$SURVEY_MODE=="LR"]), 
                                               sum(TPS$boardWeight_2015[TPS$FR_LR==1 & TPS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_FR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$FR_FR==1 & TPS$SURVEY_MODE=="FR"]), 
                                               sum(TPS$boardWeight_2015[TPS$FR_FR==1 & TPS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_LR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LR_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LR_CR==1 & TPS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_LR_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LR_HR==1 & TPS$SURVEY_MODE=="HR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LR_HR==1 & TPS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_LR_LR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$LR_LR==1 & TPS$SURVEY_MODE=="LR"]), 
                                               sum(TPS$boardWeight_2015[TPS$LR_LR==1 & TPS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_HR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$HR_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$HR_CR==1 & TPS$SURVEY_MODE=="HR"])))
marginalControls$XFERS_HR_HR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$HR_HR==1 & TPS$SURVEY_MODE=="HR"]), 
                                               sum(TPS$boardWeight_2015[TPS$HR_HR==1 & TPS$SURVEY_MODE=="HR"])))
marginalControls$XFERS_CR_CR <- as.integer(max(sum(TPS$boardWeight_2015[TPS$CR_CR==1 & TPS$SURVEY_MODE=="CR"]), 
                                               sum(TPS$boardWeight_2015[TPS$CR_CR==1 & TPS$SURVEY_MODE=="CR"])))


# Create SEED HOUSEHOLD dataset, including only records with a non-zero trip weight
#--------------------

# Select variables [exclude records with zero weight]
seed_households <- TPS[TPS$tripWeight_2015>0,c("unique_ID", "SURVEY_MODE", "operator", "route", "TRANSFER_TYPE", 
                                               "BEST_MODE", "period", "boardings", "tripWeight_2015", "LB_CR", "LB_HR", "LB_LR", "LB_FR", "LB_EB", 
                                               "LB_LB", "EB_CR", "EB_HR", "EB_LR", "EB_FR", "EB_EB", "FR_CR", "FR_HR", "FR_LR", "FR_FR", "LR_CR", 
                                               "LR_HR", "LR_LR", "HR_CR", "HR_HR", "CR_CR")]

# Rename fields to make it ready for PopSim
names(seed_households)[names(seed_households)=="unique_ID"] <- "UNIQUE_ID"
names(seed_households)[names(seed_households)=="operator"] <- "OPERATOR"
names(seed_households)[names(seed_households)=="route"] <- "ROUTE"
names(seed_households)[names(seed_households)=="period"] <- "PERIOD"
names(seed_households)[names(seed_households)=="boardings"] <- "BOARDINGS"
names(seed_households)[names(seed_households)=="tripWeight_2015"] <- "HHWGT"

# Generate sequential HH ID
seed_households$HHNUM <- seq(1, nrow(seed_households))

# Add GEOID
seed_households$GEOID <- 1


# Create SEED PERSON dataset, creating a new "person" (actually boarding record) for n# of boardings
# "Person number" becomes a unique row number within each "HH" representing each boarding within a survey record
#--------------------
seed_persons <- seed_households[rep(seed_households$HHNUM,seed_households$BOARDINGS),] %>%
  group_by(HHNUM) %>%
  mutate(PNUM = row_number()) %>%
  ungroup()

# GEOG XWALK
#--------------------
geogXWalk <- data.frame(Region = 1, GEOID = SeedIDs)


#=========================================================================================================================
# WRITE OUT SUMMARIES
#=========================================================================================================================

# Target boardings by MODE
target_boardings_mode <- xtabs(targets2015~technology, data = boarding_targets[boarding_targets$surveyed==1,])
write.table("target_boardings_mode", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",")
write.table(target_boardings_mode, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Target boardings by operator
target_boardings_operator <- xtabs(targets2015~operator, data = boarding_targets)
write.table("target_boardings_operator", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(target_boardings_operator, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by SURVEY MODE
boardings_survey_mode <- xtabs(boardWeight_2015~SURVEY_MODE, data = TPS)
write.table("boardings_survey_mode", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_survey_mode, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by OPERATOR
boardings_operator <- xtabs(boardWeight_2015~operator, data = TPS)
write.table("boardings_operator", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_operator, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by SURVEY MODE
linkedtrips_survey_mode <- xtabs(tripWeight_2015~SURVEY_MODE, data = TPS)
write.table("linkedtrips_survey_mode", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_survey_mode, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by BEST MODE
linkedtrips_best_mode <- xtabs(tripWeight_2015~BEST_MODE, data = TPS)
write.table("linkedtrips_best_mode", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by BEST MODE and transfer type
#linkedtrips_best_mode_xfer <- xtabs(boardWeight_2015~TRANSFER_TYPE+SURVEY_MODE, data = TPS)
write.table("linkedtrips_best_mode_xfer", file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode_xfer, file.path(VALIDATION_Dir, "TPS_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)




#=========================================================================================================================
# WRITE OUT POPULATIONSIM INPUT DATA FILES
#=========================================================================================================================

write.csv(seed_households, file.path(POPSIM_Dir, "data", "seed_households.csv"), row.names = F)
write.csv(seed_persons, file.path(POPSIM_Dir, "data", "seed_persons.csv"), row.names = F)
write.csv(marginalControls, file.path(POPSIM_Dir, "data", "seed_controls.csv"), row.names = F)
write.csv(geogXWalk, file.path(POPSIM_Dir, "data", "geogXWalk.csv"), row.names = F)
write.csv(TPS, file.path(POPSIM_Dir, "data", "TPS_processed.csv"), row.names = F)


# # transfer rate when surveyed mode is CR
# sum(TPS$weight[TPS$survey_tech=="CR"])/sum(TPS$trip_weight[TPS$survey_tech=="CR"])
# 
# # transfer rate when surveyed mode is not CR but transferred to/from CR
# sum(TPS$weight[TPS$survey_tech!="CR" & TPS$usedCR==1])/sum(TPS$trip_weight[TPS$survey_tech!="CR" & TPS$usedCR==1])
# 
# View(TPS[TPS$survey_tech!="CR" & TPS$usedCR==1,c("boardings","first_board_tech", "transfer_from_tech", "transfer_from",
#                                                    "survey_tech", "transfer_to", "transfer_to_tech", "last_alight_tech")])
# 
# xtabs(~transfer_from_tech+survey_tech, data = TPS)


# Turn back warnings;
#options(warn = oldw)




