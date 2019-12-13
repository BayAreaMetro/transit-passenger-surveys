###########################################################################################################################
### Script to process MTC OBS Database and produce inputs for weighting using PopualtionSim
###
### Author: Binny M Paul, July 2019
###########################################################################################################################
oldw <- getOption("warn")
options(warn = -1)

#=========================================================================================================================
# READ INPUTS
#=========================================================================================================================

# Read OBS dataset
load(file.path(OBS_Dir,     "survey.rdata"))
load(file.path(OBS_Anc_Dir, "ancillary_variables.rdata"))

# Read in target boardings for 2015
boarding_targets <- read.csv(file.path(TARGETS_Dir, "transitRidershipTargets2015.csv"), header = TRUE, stringsAsFactors = FALSE)
boarding_targets$technology[boarding_targets$technology=="Ferry"] <- "FR"

#=========================================================================================================================
# DEFINITIONS
#=========================================================================================================================

operator = c("ACE",               "AC TRANSIT",        "AIR BART",         "AMTRAK",              "BART",             
             "CALTRAIN",          "COUNTY CONNECTION", "FAIRFIELD-SUISUN", "GOLDEN GATE TRANSIT", "GOLDEN GATE FERRY", 
             "MARIN TRANSIT",     "MUNI",              "NAPA VINE",        "RIO-VISTA",           "SAMTRANS",
             "SANTA ROSA CITYBUS","SF BAY FERRY",      "SOLTRANS",          "TRI-DELTA",          "UNION CITY",          
             "WESTCAT",           "VTA",               "OTHER",             "PRIVATE SHUTTLE",  "OTHER AGENCY",        
             "BLUE GOLD FERRY", "None", "WHEELS (LAVTA)", "MODESTO TRANSIT", "BLUE & GOLD FERRY", 
             "DUMBARTON EXPRESS", "EMERY-GO-ROUND", "PETALUMA TRANSIT", "SANTA ROSA CITY BUS", "SONOMA COUNTY TRANSIT", 
             "STANFORD SHUTTLES", "VALLEJO TRANSIT", "SAN JOAQUIN TRANSIT")
technology = c("CR", "LB", "LB", "CR", "HR", 
               "CR", "LB", "LB", "EB", "FR",      
               "LB", "LB", "LB", "LB", "LB",
               "LB", "FR", "LB", "LB", "LB",     
               "LB", "LB", "LB", "LB", "LB",     
               "FR", "None", "LB", "LB", "FR", 
               "EB", "LB", "LB", "LB", "LB", 
               "LB", "LB", "LB")
opTechXWalk <- data.frame(operator, technology)

survey_tech = c("commuter rail", "express bus", "ferry", "heavy rail", "light rail", "local bus")
survey_tech_short = c("CR", "EB", "FR", "HR", "LR", "LB")
survey_tech_df <- data.frame(survey_tech, survey_tech_short)

SeedIDs <- c(1)


# Asserted BEST MODE TRANSFER RATE
#------------------------------------
best_modes = c("CR", "HR", "LR", "FR", "EB", "LB")
asserted_xfer_rates = c(1.0, 1.0, 1.0, 1.0, 1.26, 1.29)
asserted_xfer_df <- data.frame(BEST_MODE = best_modes, TRANSFER_RATE = asserted_xfer_rates)



#=========================================================================================================================
# DATA CLEANING, IMPUTATION & TRASNFORMATION
#=========================================================================================================================

# Remove weekend records
#------------------------
OBS <- survey[!(survey$day_of_the_week %in% c("SATURDAY", "SUNDAY")),]
OBS_ancillary <- ancillary_df
remove(survey)
remove(ancillary_df)


#Aggregate tour purposes
#-------------------------
OBS <- OBS %>%
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


# Access/Egress Modes
#-------------------------
# replace bike access mode with pnr
OBS$access_mode[OBS$access_mode=="bike"] <- 'pnr'
OBS$egress_mode[OBS$egress_mode=="bike"] <- 'pnr'
OBS$access_mode[OBS$access_mode=="bie"] <- 'pnr'
OBS$egress_mode[OBS$egress_mode=="bie"] <- 'pnr'

# Code missing access/egress mode
OBS$access_mode[OBS$access_mode=="."] <- "missing"
OBS$egress_mode[OBS$egress_mode=="."] <- "missing"

OBS <- OBS %>%
  mutate(access_mode = ifelse(is.na(access_mode), "missing", access_mode))
operator_access_mode <- xtabs(trip_weight~operator+access_mode, data = OBS[OBS$access_mode!="missing", ])
operator_access_mode <- data.frame(operator_access_mode)
molten <- melt(operator_access_mode, id = c("operator", "access_mode"))
operator_access_mode <- dcast(molten, operator~access_mode, sum)
operator_access_mode$tot <- operator_access_mode$walk+operator_access_mode$knr+operator_access_mode$pnr
operator_access_mode$w <- operator_access_mode$walk/operator_access_mode$tot
operator_access_mode$k <- operator_access_mode$knr/operator_access_mode$tot
operator_access_mode$p <- operator_access_mode$pnr/operator_access_mode$tot
operator_access_mode$c1 <- operator_access_mode$w
operator_access_mode$c2 <- operator_access_mode$w+operator_access_mode$k

returnAccessMode <- function(op)
{
  c1 <- operator_access_mode$c1[operator_access_mode$operator==op]
  c2 <- operator_access_mode$c2[operator_access_mode$operator==op]
  r <- runif(1)
  return(ifelse(r<c1, "walk", ifelse(r<c2, "knr", "pnr")))
}

OBS$access_mode[OBS$access_mode=="missing"] <- sapply(as.character(OBS$operator[OBS$access_mode=="missing"]),function(x) {returnAccessMode(x)} )
#-------------------------------------------------------------------------------------
OBS <- OBS %>%
  mutate(egress_mode = ifelse(is.na(egress_mode), "missing", egress_mode))
operator_egress_mode <- xtabs(trip_weight~operator+egress_mode, data = OBS[OBS$egress_mode!="missing", ])
operator_egress_mode <- data.frame(operator_egress_mode)
molten <- melt(operator_egress_mode, id = c("operator", "egress_mode"))
operator_egress_mode <- dcast(molten, operator~egress_mode, sum)
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

OBS$egress_mode[OBS$egress_mode=="missing"] <- sapply(as.character(OBS$operator[OBS$egress_mode=="missing"]),function(x) {returnEgressMode(x)} )

# Auto Sufficiency
#-----------------
# Code missing auto sufficiency
OBS <- OBS %>%
  mutate(auto_suff = ifelse(is.na(auto_suff), "missing", auto_suff))
operator_autoSuff <- xtabs(trip_weight~operator+auto_suff, data = OBS[OBS$auto_suff!="missing", ])
operator_autoSuff <- data.frame(operator_autoSuff)
molten <- melt(operator_autoSuff, id = c("operator", "auto_suff"))
operator_autoSuff <- dcast(molten, operator~auto_suff, sum)
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

OBS$auto_suff[OBS$auto_suff=="missing" | OBS$auto_suff=="Missing"] <- sapply(as.character(OBS$operator[OBS$auto_suff=="missing" | OBS$auto_suff=="Missing"]),function(x) {returnAS(x)} )

# Transform survey_tech
#-----------------------------
OBS$survey_tech <- survey_tech_df$survey_tech_short[match(OBS$survey_tech, survey_tech_df$survey_tech)]
OBS$first_board_tech <- survey_tech_df$survey_tech_short[match(OBS$first_board_tech, survey_tech_df$survey_tech)]
OBS$last_alight_tech <- survey_tech_df$survey_tech_short[match(OBS$last_alight_tech, survey_tech_df$survey_tech)]

# Detailed Operator Coding
#-------------------------
# Edit operator names to show local and express bus
OBS$operator[OBS$operator=="AC Transit" & OBS$survey_tech=="LB"] <- "AC Transit [LOCAL]"
OBS$operator[OBS$operator=="AC Transit" & OBS$survey_tech=="EB"] <- "AC Transit [EXPRESS]"

OBS$operator[OBS$operator=="County Connection" & OBS$survey_tech=="LB"] <- "County Connection [LOCAL]"
OBS$operator[OBS$operator=="County Connection" & OBS$survey_tech=="EB"] <- "County Connection [EXPRESS]"

OBS$operator[OBS$operator=="Golden Gate Transit (bus)" & OBS$survey_tech=="LB"] <- "Golden Gate Transit [LOCAL]"
OBS$operator[OBS$operator=="Golden Gate Transit (bus)" & OBS$survey_tech=="EB"] <- "Golden Gate Transit [EXPRESS]"

OBS$operator[OBS$operator=="Napa Vine" & OBS$survey_tech=="LB"] <- "Napa Vine [LOCAL]"
OBS$operator[OBS$operator=="Napa Vine" & OBS$survey_tech=="EB"] <- "Napa Vine [EXPRESS]"

OBS$operator[OBS$operator=="SamTrans" & OBS$survey_tech=="LB"] <- "SamTrans [LOCAL]"
OBS$operator[OBS$operator=="SamTrans" & OBS$survey_tech=="EB"] <- "SamTrans [EXPRESS]"

OBS$operator[OBS$operator=="SF Muni" & OBS$survey_tech=="LB"] <- "SF Muni [LOCAL]"
OBS$operator[OBS$operator=="SF Muni" & OBS$survey_tech=="LR"] <- "SF Muni [LRT]"

OBS$operator[OBS$operator=="VTA" & OBS$survey_tech=="LB"] <- "VTA [LOCAL]"
OBS$operator[OBS$operator=="VTA" & OBS$survey_tech=="EB"] <- "VTA [EXPRESS]"
OBS$operator[OBS$operator=="VTA" & OBS$survey_tech=="LR"] <- "VTA [LRT]"

## copy technology from the targets database
OBS <- merge(x=OBS, y=boarding_targets[boarding_targets$surveyed==1,c("operator","technology")], by="operator", all.x = TRUE)
OBS$technology[OBS$technology=="Ferry"] <- "FR"



#=========================================================================================================================
# FILTER RECORDS WITH MISSING DATA
#=========================================================================================================================

# Missing Tour Purpose
OBS <- OBS[OBS$agg_tour_purp>0,]



#=========================================================================================================================
# FACTOR WEIGHTS TO MATCH 2015 RIDERSHIP TARGETS
#=========================================================================================================================

# Factor weights to match 2015 targets for surveyed operators
#-------------------------------------------------------------

# OBS boards by operator
obs_operator_totals <- aggregate(OBS$weight, by = list(operator = OBS$operator), sum)
obs_operator_totals <- data.frame(obs_operator_totals)
colnames(obs_operator_totals) <- c("operator", "boardWeight")

# Target 2015 boards by operator
target_operator_totals <- aggregate(boarding_targets$targets2015, by = list(operator = boarding_targets$operator), sum)
target_operator_totals <- data.frame(target_operator_totals)
colnames(target_operator_totals) <- c("operator", "targetBoardings")

# Compute expansion factors
expansion_factors <- obs_operator_totals
expansion_factors$targetBoardings <- target_operator_totals$targetBoardings[match(expansion_factors$operator, target_operator_totals$operator)]

expansion_factors <- expansion_factors %>%
  mutate(exp_factor = targetBoardings/boardWeight) 

# Compute 2015 factored weights
OBS$exp_factor <- expansion_factors$exp_factor[match(OBS$operator, expansion_factors$operator)]
OBS$boardWeight_2015 <- OBS$weight * OBS$exp_factor
OBS$tripWeight_2015 <- OBS$trip_weight * OBS$exp_factor

# Non-surveyed operators to be added later

## Factor weights to match 2015 targets for non-surveyed operators
##-----------------------------------------------------------------
#
## Boarding targets for non-surveyed operators
#non_surveyed_targets <- boarding_targets[boarding_targets$surveyed==0,]
#
## OBS boards by operator
#obs_tech_totals <- aggregate(OBS$boardWeight_2015, by = list(technology = OBS$technology), sum)
#obs_tech_totals <- data.frame(obs_tech_totals)
#colnames(obs_tech_totals) <- c("technology", "boardWeight")
#
## Target 2015 boards by technology [including non-surveyed operators]
#target_tech_totals <- aggregate(boarding_targets$targets2015, by = list(technology = boarding_targets$technology), sum)
#target_tech_totals <- data.frame(target_tech_totals)
#colnames(target_tech_totals) <- c("technology", "targetBoardings")
#
## Compute expansion factors
#expansion_factors <- obs_tech_totals
#expansion_factors$targetBoardings <- target_tech_totals$targetBoardings[match(expansion_factors$technology, target_tech_totals$technology)]
#
#expansion_factors <- expansion_factors %>%
#  mutate(exp_factor = targetBoardings/boardWeight) 
#
## Compute final 2015 factored weights [accounting for non-surveyed operators]
#OBS$exp_factor <- expansion_factors$exp_factor[match(OBS$technology, expansion_factors$technology)]
#OBS$boardWeight_2015 <- OBS$boardWeight_2015 * OBS$exp_factor
#OBS$tripWeight_2015 <- OBS$tripWeight_2015 * OBS$exp_factor



#=========================================================================================================================
# CODE VARIABLES FOR PopulatoinSim WEIGHTING
#=========================================================================================================================

# Number of transfers
#------------------------
OBS$nTransfers <- OBS$boardings - 1

# Time period
#----------------
OBS$period[OBS$depart_hour<6] <- "EA"
OBS$period[OBS$depart_hour>=6 & OBS$depart_hour<9] <- "AM"
OBS$period[OBS$depart_hour>=9 & OBS$depart_hour<15] <- "MD"
OBS$period[OBS$depart_hour>=15 & OBS$depart_hour<19] <- "PM"
OBS$period[OBS$depart_hour>=19] <- "EV"


# BEST Mode
#--------------------
OBS$transfer_from_tech <- opTechXWalk$technology[match(OBS$transfer_from, opTechXWalk$operator)]
OBS$transfer_to_tech <- opTechXWalk$technology[match(OBS$transfer_to, opTechXWalk$operator)]

OBS$transfer_from_tech[OBS$transfer_from=="WHEELS (LAVTA)" | OBS$transfer_from=="MODESTO TRANSIT"] <- "LB"
OBS$transfer_to_tech[OBS$transfer_to=="WHEELS (LAVTA)" | OBS$transfer_to=="MODESTO TRANSIT"] <- "LB"

# Code Mode Set Type
OBS <- OBS %>%
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

# recode used fields based on path line haul code
OBS$usedLB[is.na(OBS$usedLB)] <- 0
OBS$usedEB[is.na(OBS$usedEB)] <- 0
OBS$usedLR[is.na(OBS$usedLR)] <- 0
OBS$usedFR[is.na(OBS$usedFR)] <- 0
OBS$usedHR[is.na(OBS$usedHR)] <- 0
OBS$usedCR[is.na(OBS$usedCR)] <- 0

OBS$usedTotal <- OBS$usedLB+OBS$usedEB+OBS$usedLR+OBS$usedFR+OBS$usedHR+OBS$usedCR

OBS$usedLB[OBS$usedTotal==0 & OBS$path_line_haul=="LOC"] <- 1
OBS$usedEB[OBS$usedTotal==0 & OBS$path_line_haul=="EXP"] <- 1
OBS$usedLR[OBS$usedTotal==0 & OBS$path_line_haul=="LRF"] <- 1
OBS$usedHR[OBS$usedTotal==0 & OBS$path_line_haul=="HVY"] <- 1
OBS$usedCR[OBS$usedTotal==0 & OBS$path_line_haul=="COM"] <- 1

OBS$usedTotal <- OBS$usedLB+OBS$usedEB+OBS$usedLR+OBS$usedFR+OBS$usedHR+OBS$usedCR

OBS$BEST_MODE <- "LB"
OBS$BEST_MODE[OBS$usedEB==1] <- "EB"
OBS$BEST_MODE[OBS$usedFR==1] <- "FR"
OBS$BEST_MODE[OBS$usedLR==1] <- "LR"
OBS$BEST_MODE[OBS$usedHR==1] <- "HR"
OBS$BEST_MODE[OBS$usedCR==1] <- "CR"


#Transfer Types [across all surveys]
#----------------------

OBS <- OBS %>%
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


OBS$TRANSFER_TYPE <- "OTHER"
OBS$TRANSFER_TYPE[OBS$nTransfers==0] <- "NO_TRANSFERS"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedCR==1 & OBS$nTransfers>0] <- "LB_CR"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedHR==1 & OBS$nTransfers>0] <- "LB_HR"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedLR==1 & OBS$nTransfers>0] <- "LB_LR"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedFR==1 & OBS$nTransfers>0] <- "LB_FR"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedEB==1 & OBS$nTransfers>0] <- "LB_EB"
OBS$TRANSFER_TYPE[OBS$usedLB==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "LB_LB"
OBS$TRANSFER_TYPE[OBS$usedEB==1 & OBS$usedCR==1 & OBS$nTransfers>0] <- "EB_CR"
OBS$TRANSFER_TYPE[OBS$usedEB==1 & OBS$usedHR==1 & OBS$nTransfers>0] <- "EB_HR"
OBS$TRANSFER_TYPE[OBS$usedEB==1 & OBS$usedLR==1 & OBS$nTransfers>0] <- "EB_LR"
OBS$TRANSFER_TYPE[OBS$usedEB==1 & OBS$usedFR==1 & OBS$nTransfers>0] <- "EB_FR"
OBS$TRANSFER_TYPE[OBS$usedEB==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "EB_EB"
OBS$TRANSFER_TYPE[OBS$usedFR==1 & OBS$usedCR==1 & OBS$nTransfers>0] <- "FR_CR"
OBS$TRANSFER_TYPE[OBS$usedFR==1 & OBS$usedHR==1 & OBS$nTransfers>0] <- "FR_HR"
OBS$TRANSFER_TYPE[OBS$usedFR==1 & OBS$usedLR==1 & OBS$nTransfers>0] <- "FR_LR"
OBS$TRANSFER_TYPE[OBS$usedFR==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "FR_FR"
OBS$TRANSFER_TYPE[OBS$usedLR==1 & OBS$usedCR==1 & OBS$nTransfers>0] <- "LR_CR"
OBS$TRANSFER_TYPE[OBS$usedLR==1 & OBS$usedHR==1 & OBS$nTransfers>0] <- "LR_HR"
OBS$TRANSFER_TYPE[OBS$usedLR==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "LR_LR"
OBS$TRANSFER_TYPE[OBS$usedHR==1 & OBS$usedCR==1 & OBS$nTransfers>0] <- "HR_CR"
OBS$TRANSFER_TYPE[OBS$usedHR==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "HR_HR"
OBS$TRANSFER_TYPE[OBS$usedCR==1 & OBS$usedTotal==1 & OBS$nTransfers>0] <- "CR_CR"



#=========================================================================================================================
# RENAME FIELDS
#=========================================================================================================================

names(OBS)[names(OBS)=="survey_tech"] <- "SURVEY_MODE"



#=========================================================================================================================
# PREPARE INPUTS FOR POPULATIONSIM WEIGHTING EXERCISE
#=========================================================================================================================


# MARGINAL CONTROLS
#--------------------
marginalControls <- data.frame(GEOID = SeedIDs)

# Boardings by operator
boardingsTargets <- data.frame(xtabs(boardWeight_2015~operator, data = OBS))
boardingsTargets <- data.frame(t(boardingsTargets), stringsAsFactors = F)
colnames(boardingsTargets) <- boardingsTargets[1,]
boardingsTargets <- boardingsTargets[2,]
for (col in names(boardingsTargets)){
  boardingsTargets[[col]] <- as.integer(boardingsTargets[[col]])
}
colnames(boardingsTargets) <- unlist(lapply(colnames(boardingsTargets), function(x)(gsub(" ", "_", x))))
colnames(boardingsTargets) <- unlist(lapply(colnames(boardingsTargets), function(x)(gsub("-", "_", x))))

# Combine all marginal controls
marginalControls <- cbind(marginalControls, boardingsTargets)

# Total linked trips
totalLinkedTrips <- sum(OBS$tripWeight_2015)
marginalControls$totalLinkedTrips <- totalLinkedTrips

# Boardings by transfer type

linkedtrips_best_mode_xfer <- OBS %>%
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

linkedtrips_best_mode_xfer <- data.frame(t(linkedtrips_best_mode_xfer), stringsAsFactors = F)
colnames(linkedtrips_best_mode_xfer) <- linkedtrips_best_mode_xfer[c(1),]
linkedtrips_best_mode_xfer <- linkedtrips_best_mode_xfer[-c(1),]
linkedtrips_best_mode_xfer <- cbind(TRANSFER_TYPE = row.names(linkedtrips_best_mode_xfer), linkedtrips_best_mode_xfer)

# transfer targets must be from the survey reporting the maximum transfers
#marginalControls$XFERS_LB_EB <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_EB" & OBS$SURVEY_MODE=="EB"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_EB" & OBS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_LR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_LR" & OBS$SURVEY_MODE=="LR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_LR" & OBS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_FR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_FR" & OBS$SURVEY_MODE=="FR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_FR" & OBS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_CR" & OBS$SURVEY_MODE=="CR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_CR" & OBS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LB_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_HR" & OBS$SURVEY_MODE=="HR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LB_HR" & OBS$SURVEY_MODE=="LB"])))
#marginalControls$XFERS_LR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LR_CR" & OBS$SURVEY_MODE=="CR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="LR_CR" & OBS$SURVEY_MODE=="LR"])))
#marginalControls$XFERS_HR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="HR_CR" & OBS$SURVEY_MODE=="CR"]), 
#                                               sum(OBS$boardWeight_2015[OBS$TRANSFER_TYPE=="HR_CR" & OBS$SURVEY_MODE=="HR"])))

marginalControls$XFERS_LB_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_CR==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_HR==1 & OBS$SURVEY_MODE=="HR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_HR==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_LR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_LR==1 & OBS$SURVEY_MODE=="LR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_LR==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_FR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_FR==1 & OBS$SURVEY_MODE=="FR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_FR==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_EB <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_EB==1 & OBS$SURVEY_MODE=="EB"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_EB==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_LB_LB <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LB_LB==1 & OBS$SURVEY_MODE=="LB"]), 
                                               sum(OBS$boardWeight_2015[OBS$LB_LB==1 & OBS$SURVEY_MODE=="LB"])))
marginalControls$XFERS_EB_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$EB_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$EB_CR==1 & OBS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$EB_HR==1 & OBS$SURVEY_MODE=="HR"]), 
                                               sum(OBS$boardWeight_2015[OBS$EB_HR==1 & OBS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_LR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$EB_LR==1 & OBS$SURVEY_MODE=="LR"]), 
                                               sum(OBS$boardWeight_2015[OBS$EB_LR==1 & OBS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_FR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$EB_FR==1 & OBS$SURVEY_MODE=="FR"]), 
                                               sum(OBS$boardWeight_2015[OBS$EB_FR==1 & OBS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_EB_EB <- as.integer(max(sum(OBS$boardWeight_2015[OBS$EB_EB==1 & OBS$SURVEY_MODE=="EB"]), 
                                               sum(OBS$boardWeight_2015[OBS$EB_EB==1 & OBS$SURVEY_MODE=="EB"])))
marginalControls$XFERS_FR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$FR_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$FR_CR==1 & OBS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$FR_HR==1 & OBS$SURVEY_MODE=="HR"]), 
                                               sum(OBS$boardWeight_2015[OBS$FR_HR==1 & OBS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_LR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$FR_LR==1 & OBS$SURVEY_MODE=="LR"]), 
                                               sum(OBS$boardWeight_2015[OBS$FR_LR==1 & OBS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_FR_FR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$FR_FR==1 & OBS$SURVEY_MODE=="FR"]), 
                                               sum(OBS$boardWeight_2015[OBS$FR_FR==1 & OBS$SURVEY_MODE=="FR"])))
marginalControls$XFERS_LR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LR_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LR_CR==1 & OBS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_LR_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LR_HR==1 & OBS$SURVEY_MODE=="HR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LR_HR==1 & OBS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_LR_LR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$LR_LR==1 & OBS$SURVEY_MODE=="LR"]), 
                                               sum(OBS$boardWeight_2015[OBS$LR_LR==1 & OBS$SURVEY_MODE=="LR"])))
marginalControls$XFERS_HR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$HR_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$HR_CR==1 & OBS$SURVEY_MODE=="HR"])))
marginalControls$XFERS_HR_HR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$HR_HR==1 & OBS$SURVEY_MODE=="HR"]), 
                                               sum(OBS$boardWeight_2015[OBS$HR_HR==1 & OBS$SURVEY_MODE=="HR"])))
marginalControls$XFERS_CR_CR <- as.integer(max(sum(OBS$boardWeight_2015[OBS$CR_CR==1 & OBS$SURVEY_MODE=="CR"]), 
                                               sum(OBS$boardWeight_2015[OBS$CR_CR==1 & OBS$SURVEY_MODE=="CR"])))


# SEED HOUSEHOLD
#--------------------

# Select variables [exclude records with zero weight]
seed_households <- OBS[OBS$tripWeight_2015>0,c("Unique_ID", "SURVEY_MODE", "operator", "route", "TRANSFER_TYPE", 
                                               "BEST_MODE", "period", "boardings", "tripWeight_2015", "LB_CR", "LB_HR", "LB_LR", "LB_FR", "LB_EB", 
                                               "LB_LB", "EB_CR", "EB_HR", "EB_LR", "EB_FR", "EB_EB", "FR_CR", "FR_HR", "FR_LR", "FR_FR", "LR_CR", 
                                               "LR_HR", "LR_LR", "HR_CR", "HR_HR", "CR_CR")]

# Rename fields
names(seed_households)[names(seed_households)=="Unique_ID"] <- "UNIQUE_ID"
names(seed_households)[names(seed_households)=="operator"] <- "OPERATOR"
names(seed_households)[names(seed_households)=="route"] <- "ROUTE"
names(seed_households)[names(seed_households)=="period"] <- "PERIOD"
names(seed_households)[names(seed_households)=="boardings"] <- "BOARDINGS"
names(seed_households)[names(seed_households)=="tripWeight_2015"] <- "HHWGT"

# Generate sequential HH ID
seed_households$HHNUM <- seq(1, nrow(seed_households))

# Add GEOID
seed_households$GEOID <- 1


# SEED PERSON
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
write.table("target_boardings_mode", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",")
write.table(target_boardings_mode, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Target boardings by operator
target_boardings_operator <- xtabs(targets2015~operator, data = boarding_targets)
write.table("target_boardings_operator", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(target_boardings_operator, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by SURVEY MODE
boardings_survey_mode <- xtabs(boardWeight_2015~SURVEY_MODE, data = OBS)
write.table("boardings_survey_mode", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_survey_mode, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by OPERATOR
boardings_operator <- xtabs(boardWeight_2015~operator, data = OBS)
write.table("boardings_operator", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_operator, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by SURVEY MODE
linkedtrips_survey_mode <- xtabs(tripWeight_2015~SURVEY_MODE, data = OBS)
write.table("linkedtrips_survey_mode", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_survey_mode, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by BEST MODE
linkedtrips_best_mode <- xtabs(tripWeight_2015~BEST_MODE, data = OBS)
write.table("linkedtrips_best_mode", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by BEST MODE and transfer type
#linkedtrips_best_mode_xfer <- xtabs(boardWeight_2015~TRANSFER_TYPE+SURVEY_MODE, data = OBS)
write.table("linkedtrips_best_mode_xfer", file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode_xfer, file.path(VALIDATION_Dir, "OBS_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)




#=========================================================================================================================
# WRITE OUT POPULATIONSIM INPUT DATA FILES
#=========================================================================================================================

write.csv(seed_households, file.path(POPSIM_Dir, "data", "seed_households.csv"), row.names = F)
write.csv(seed_persons, file.path(POPSIM_Dir, "data", "seed_persons.csv"), row.names = F)
write.csv(marginalControls, file.path(POPSIM_Dir, "data", "seed_controls.csv"), row.names = F)
write.csv(geogXWalk, file.path(POPSIM_Dir, "data", "geogXWalk.csv"), row.names = F)
write.csv(OBS, file.path(POPSIM_Dir, "data", "obs_processed.csv"), row.names = F)


# # transfer rate when surveyed mode is CR
# sum(OBS$weight[OBS$survey_tech=="CR"])/sum(OBS$trip_weight[OBS$survey_tech=="CR"])
# 
# # transfer rate when surveyed mode is not CR but transferred to/from CR
# sum(OBS$weight[OBS$survey_tech!="CR" & OBS$usedCR==1])/sum(OBS$trip_weight[OBS$survey_tech!="CR" & OBS$usedCR==1])
# 
# View(OBS[OBS$survey_tech!="CR" & OBS$usedCR==1,c("boardings","first_board_tech", "transfer_from_tech", "transfer_from",
#                                                    "survey_tech", "transfer_to", "transfer_to_tech", "last_alight_tech")])
# 
# xtabs(~transfer_from_tech+survey_tech, data = OBS)


# Turn back warnings;
options(warn = oldw)




