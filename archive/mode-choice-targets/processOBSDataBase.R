#######################################################
### Script to summarize MTC OBS Database
### Author: Binny M Paul, binny.mathewpaul@rsginc.com
#######################################################
oldw <- getOption("warn")
options(warn = -1)

library(plyr)
library(dplyr)
#install.packages("reshape2")
library(reshape2)

# User Inputs
OBS_Dir <- "C:\\Users\\binny.paul.I-RSG\\Documents\\Projects\\MTC\\Data\\"
BoxData_Dir <- "C:\\Users\\binny.paul.I-RSG\\Documents\\Projects\\MTC\\BoxData\\Regional On-board Survey Processing\\_WORKING\\Mode choice targets\\data\\"
MUNI_Dir <- "2_MUNI - Boarding Data\\"
SCVTA_Dir <- "3_SCVTA Data\\"
NTD_Dir <- "NTD\\"
outFile <- "OBS_SummaryStatistics.csv"


#----------------------------------------------------------------------------------------
# System-wide and BART Data
# -------------------------

OBS <- read.csv(paste(OBS_Dir, "survey.csv", sep = ""), stringsAsFactors = FALSE)

# Assume trabsfer rate of 1 for BART [actual transfer rate is greater than 1]
OBS$trip_weight[OBS$operator=="BART"] <- OBS$weight[OBS$operator=="BART"]

#BART_OBS <- read.csv(paste(OBS_Dir, "BART_Final_Database_Mar18_SUBMITTED_with_station_xy.csv", sep = ""), stringsAsFactors = FALSE)
OBS_ancillary <- read.csv(paste(OBS_Dir, "ancillary_df.csv", sep = ""), stringsAsFactors = FALSE)

# Process data for calibration targets preparation
OBS$work_before <- OBS_ancillary$at_work_prior_to_orig_purp[match(OBS$Unique_ID, OBS_ancillary$Unique_ID)]
OBS$work_after <- OBS_ancillary$at_work_after_dest_purp[match(OBS$Unique_ID, OBS_ancillary$Unique_ID)]

OBS$school_before <- OBS_ancillary$at_school_prior_to_orig_purp[match(OBS$Unique_ID, OBS_ancillary$Unique_ID)]
OBS$school_after <- OBS_ancillary$at_school_after_dest_purp[match(OBS$Unique_ID, OBS_ancillary$Unique_ID)]

OBS$work_before[OBS$work_before=="at work before surveyed trip"] <- 'Y'
OBS$work_before[OBS$work_before=="not at work before surveyed trip"] <- 'N'
OBS$work_before[OBS$work_before=="not relevant"] <- 'NA'

OBS$work_after[OBS$work_after=="at work after surveyed trip"] <- 'Y'
OBS$work_after[OBS$work_after=="not at work after surveyed trip"] <- 'N'
OBS$work_after[OBS$work_after=="not relevant"] <- 'NA'

OBS$school_before[OBS$school_before=="at school before surveyed trip"] <- 'Y'
OBS$school_before[OBS$school_before=="not at school before surveyed trip"] <- 'N'
OBS$school_before[OBS$school_before=="not relevant"] <- 'NA'

OBS$school_after[OBS$school_after=="at school after surveyed trip"] <- 'Y'
OBS$school_after[OBS$school_after=="not at school after surveyed trip"] <- 'N'
OBS$school_after[OBS$school_after=="not relevant"] <- 'NA'


#Aggregate tour purposes
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


# Tour access/egress mode

# replace bike access mode with pnr
OBS$access_mode[OBS$access_mode=="bike"] <- 'pnr'
OBS$egress_mode[OBS$egress_mode=="bike"] <- 'pnr'

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

#-------------------------------------------------------------------------------------

OBS <- OBS %>%
  mutate(outbound = 1) %>% 
  # based on home destination
  mutate(outbound = ifelse(outbound == 1 & dest_purp == 'home', 0, outbound)) %>% 
  # if neither end is home and egress mode is PNR/KNR
  mutate(outbound = ifelse(outbound == 1 & orig_purp != 'home' & dest_purp != 'home' & (egress_mode == 'knr' | egress_mode == 'pnr'), 0, outbound)) %>% 
  # if neither end is home but have been to school/work before this trip
  mutate(outbound = ifelse(outbound == 1 & orig_purp != 'home' & dest_purp != 'home' & (school_before == 'Y' | work_before == 'Y'), 0, outbound))

#set NAs to 1 for outbound [Assume all missing to be outbound]
OBS$outbound[is.na(OBS$outbound)] <- 1

OBS <- OBS %>%
  mutate(tour_access_mode = access_mode) %>% 
  mutate(tour_access_mode = ifelse(outbound == 0, egress_mode, tour_access_mode))

# code missing access/egress mode as "walk"
OBS <- OBS %>%
  mutate(tour_access_mode = ifelse(tour_access_mode == "missing", "walk", tour_access_mode))

OBS <- OBS %>%
  # Access mode for at-work is always walk
  mutate(tour_access_mode = ifelse(agg_tour_purp == 6, 'walk', tour_access_mode))

# Trip mode with anchor access [at home end]
OBS <- OBS %>%
  mutate(trip_mode = paste(tour_access_mode, "-", survey_tech, "-", operator, sep = ""))

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

OBS_collapsed <- data.frame()
for (i in 1:6){
  t <- xtabs(trip_weight~trip_mode+auto_suff, data = OBS[OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  OBS_collapsed <- rbind(OBS_collapsed,t)
}
colnames(OBS_collapsed) <- c("tripMode", "autoSuff", "trips", "tourPurpose")

temp <- data.frame()
for (i in 1:6){
  t <- xtabs(weight~trip_mode+auto_suff, data = OBS[OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  temp <- rbind(temp,t)
}
colnames(temp) <- c("tripMode", "autoSuff", "boards", "tourPurpose")

OBS_collapsed$boards <- temp$boards[match(paste(OBS_collapsed$tripMode, OBS_collapsed$autoSuff, OBS_collapsed$tourPurpose), 
                                          paste(temp$tripMode, temp$autoSuff, temp$tourPurpose))]

#xwalk_mode <- unique(OBS[,c("trip_mode", "operator", "survey_tech")])
#OBS_collapsed$operator <- xwalk_mode$operator[match(OBS_collapsed$tripMode, xwalk_mode$trip_mode)]
#OBS_collapsed$survey_tech <- xwalk_mode$survey_tech[match(OBS_collapsed$tripMode, xwalk_mode$trip_mode)]

#----------------------------------------------------------------------------------------
# 2004 MUNI OBS data processing
# -------------------------
MUNI_OBS <- read.csv(paste(BoxData_Dir,MUNI_Dir, "MUNI_coded_for_MTC.csv", sep = ""), header = TRUE, stringsAsFactors = FALSE)

#Aggregate tour purposes
MUNI_OBS <- MUNI_OBS %>%
  mutate(agg_tour_purp = -9) %>% 
  # 1[Work]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Work'), 1, agg_tour_purp)) %>% 
  # 2[University]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'University'), 2, agg_tour_purp)) %>% 
  # 3[School]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'School'), 3, agg_tour_purp)) %>% 
  # 4[Maintenance]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Maintenance'), 4, agg_tour_purp)) %>% 
  # 5[Discretionary]:
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Discretionary'), 5, agg_tour_purp)) %>% 
  # 6[At-work Subtour]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'At-work Subtour'), 6, agg_tour_purp))

MUNI_OBS$tour_access_mode[MUNI_OBS$anchorAccessMode=="Walk"] <- 'walk'
MUNI_OBS$tour_access_mode[MUNI_OBS$anchorAccessMode=="PNR"] <- 'pnr'
MUNI_OBS$tour_access_mode[MUNI_OBS$anchorAccessMode=="KNR"] <- 'knr'

# Trip mode with anchor access [at home end]
MUNI_OBS <- MUNI_OBS %>%
  mutate(trip_mode = paste(anchorAccessMode, "-", survey_tech, "-", operator, sep = ""))

MUNI_OBS$auto_suff[MUNI_OBS$autoSuff=="0 Autos"] <- "zero autos"
MUNI_OBS$auto_suff[MUNI_OBS$autoSuff=="Autos<Workers"] <- "auto negotiating"
MUNI_OBS$auto_suff[MUNI_OBS$autoSuff=="Autos>=Workers"] <- "auto sufficient"

OBS_MUNI_collapsed <- data.frame()
for (i in 1:6){
  t <- xtabs(tripWeight~trip_mode+auto_suff, data = MUNI_OBS[MUNI_OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  OBS_MUNI_collapsed <- rbind(OBS_MUNI_collapsed,t)
}
colnames(OBS_MUNI_collapsed) <- c("tripMode", "autoSuff", "trips", "tourPurpose")

temp <- data.frame()
for (i in 1:6){
  t <- xtabs(boardWeight~trip_mode+auto_suff, data = MUNI_OBS[MUNI_OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  temp <- rbind(temp,t)
}
colnames(temp) <- c("tripMode", "autoSuff", "boards", "tourPurpose")

OBS_MUNI_collapsed$boards <- temp$boards[match(paste(OBS_MUNI_collapsed$tripMode, OBS_MUNI_collapsed$autoSuff, OBS_MUNI_collapsed$tourPurpose), 
                                          paste(temp$tripMode, temp$autoSuff, temp$tourPurpose))]

#xwalk_mode <- unique(MUNI_OBS[,c("trip_mode", "operator", "survey_tech")])
#OBS_MUNI_collapsed$operator <- xwalk_mode$operator[match(OBS_MUNI_collapsed$tripMode, xwalk_mode$trip_mode)]
#OBS_MUNI_collapsed$survey_tech <- xwalk_mode$survey_tech[match(OBS_MUNI_collapsed$tripMode, xwalk_mode$trip_mode)]

#----------------------------------------------------------------------------------------
# 2013 VTA OBS data processing
# -------------------------

VTA_OBS <- read.csv(paste(BoxData_Dir,SCVTA_Dir, "VTA2013 Expanded.csv", sep = ""), header = TRUE, stringsAsFactors = FALSE)

#Aggregate tour purposes
VTA_OBS <- VTA_OBS %>%
  mutate(agg_tour_purp = -9) %>% 
  # 1[Work]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Work'), 1, agg_tour_purp)) %>% 
  # 2[University]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'University'), 2, agg_tour_purp)) %>% 
  # 3[School]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'School'), 3, agg_tour_purp)) %>% 
  # 4[Maintenance]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Maintenance'), 4, agg_tour_purp)) %>% 
  # 5[Discretionary]:
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'Discretionary'), 5, agg_tour_purp)) %>% 
  # 6[At-work Subtour]: 
  mutate(agg_tour_purp = ifelse(agg_tour_purp == -9 & (tourPurpAgg == 'At-Work Subtour'), 6, agg_tour_purp))

VTA_OBS$tour_access_mode[VTA_OBS$anchorAccessMode=="Walk"] <- 'walk'
VTA_OBS$tour_access_mode[VTA_OBS$anchorAccessMode=="PNR"] <- 'pnr'
VTA_OBS$tour_access_mode[VTA_OBS$anchorAccessMode=="KNR"] <- 'knr'

# Trip mode with anchor access [at home end]
VTA_OBS <- VTA_OBS %>%
  mutate(trip_mode = paste(anchorAccessMode, "-", surveyTechnology, "-", operator, sep = ""))

VTA_OBS$auto_suff[VTA_OBS$autoSuff=="0 Autos"] <- "zero autos"
VTA_OBS$auto_suff[VTA_OBS$autoSuff=="Autos<Workers"] <- "auto negotiating"
VTA_OBS$auto_suff[VTA_OBS$autoSuff=="Autos>=Workers"] <- "auto sufficient"

OBS_VTA_collapsed <- data.frame()
for (i in 1:6){
  t <- xtabs(tripWeight~trip_mode+auto_suff, data = VTA_OBS[VTA_OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  OBS_VTA_collapsed <- rbind(OBS_VTA_collapsed,t)
}
colnames(OBS_VTA_collapsed) <- c("tripMode", "autoSuff", "trips", "tourPurpose")

temp <- data.frame()
for (i in 1:6){
  t <- xtabs(boardWeight~trip_mode+auto_suff, data = VTA_OBS[VTA_OBS$agg_tour_purp==i,])
  t <- data.frame(t)
  t$purpose <- i
  temp <- rbind(temp,t)
}
colnames(temp) <- c("tripMode", "autoSuff", "boards", "tourPurpose")

OBS_VTA_collapsed$boards <- temp$boards[match(paste(OBS_VTA_collapsed$tripMode, OBS_VTA_collapsed$autoSuff, OBS_VTA_collapsed$tourPurpose), 
                                               paste(temp$tripMode, temp$autoSuff, temp$tourPurpose))]

#xwalk_mode <- unique(VTA_OBS[,c("trip_mode", "operator", "surveyTechnology")])
#OBS_VTA_collapsed$operator <- xwalk_mode$operator[match(OBS_VTA_collapsed$tripMode, xwalk_mode$trip_mode)]
#OBS_VTA_collapsed$survey_tech <- xwalk_mode$surveyTechnology[match(OBS_VTA_collapsed$tripMode, xwalk_mode$trip_mode)]


#-----------------------------------------------------------------------------------------
# Combine all collapsed datasets
unified_collapsed <- rbind(OBS_collapsed, OBS_MUNI_collapsed, OBS_VTA_collapsed)
unified_collapsed$accessMode <- sapply(as.character(unified_collapsed$tripMode),function(x) unlist(strsplit(x, "-"))[[1]] )
unified_collapsed$accessMode[unified_collapsed$accessMode=="Walk"] <- "walk"
unified_collapsed$accessMode[unified_collapsed$accessMode=="PNR"] <- "pnr"
unified_collapsed$accessMode[unified_collapsed$accessMode=="KNR"] <- "knr"
unified_collapsed$survey_tech <- sapply(as.character(unified_collapsed$tripMode),function(x) unlist(strsplit(x, "-"))[[2]] )
unified_collapsed$operator <- sapply(as.character(unified_collapsed$tripMode),function(x) unlist(strsplit(x, "-"))[[3]] )
unified_collapsed$operator[unified_collapsed$operator=="Tri"] <- "Tri-Delta"

# Edit operator names to show local and express bus
unified_collapsed$operator[unified_collapsed$operator=="AC Transit" & unified_collapsed$survey_tech=="local bus"] <- "AC Transit [LOCAL]"
unified_collapsed$operator[unified_collapsed$operator=="AC Transit" & unified_collapsed$survey_tech=="express bus"] <- "AC Transit [EXPRESS]"

unified_collapsed$operator[unified_collapsed$operator=="County Connection" & unified_collapsed$survey_tech=="local bus"] <- "County Connection [LOCAL]"
unified_collapsed$operator[unified_collapsed$operator=="County Connection" & unified_collapsed$survey_tech=="express bus"] <- "County Connection [EXPRESS]"

unified_collapsed$operator[unified_collapsed$operator=="Golden Gate Transit (bus)" & unified_collapsed$survey_tech=="local bus"] <- "Golden Gate Transit [LOCAL]"
unified_collapsed$operator[unified_collapsed$operator=="Golden Gate Transit (bus)" & unified_collapsed$survey_tech=="express bus"] <- "Golden Gate Transit [EXPRESS]"

unified_collapsed$operator[unified_collapsed$operator=="Napa Vine" & unified_collapsed$survey_tech=="local bus"] <- "Napa Vine [LOCAL]"
unified_collapsed$operator[unified_collapsed$operator=="Napa Vine" & unified_collapsed$survey_tech=="express bus"] <- "Napa Vine [EXPRESS]"

unified_collapsed$operator[unified_collapsed$operator=="SamTrans" & unified_collapsed$survey_tech=="local bus"] <- "SamTrans [LOCAL]"
unified_collapsed$operator[unified_collapsed$operator=="SamTrans" & unified_collapsed$survey_tech=="express bus"] <- "SamTrans [EXPRESS]"

# code technology
survey_tech <- c("commuter rail", "express bus", "ferry", "heavy rail", "local bus",  "metro", "cable car", "SCVTA Express", "SCVTA Local", "SCVTA LRT")
technology <- c("CR", "EB", "Ferry", "HR", "LB", "")

# Calculate group total boarding by operator
obs_operator_totals <- aggregate(unified_collapsed$boards, by = list(operator = unified_collapsed$operator), sum)
obs_operator_totals <- data.frame(obs_operator_totals)
colnames(obs_operator_totals) <- c("operator", "boardWeight")

# Read in target boardings for 2010
boarding_targets <- read.csv(paste(BoxData_Dir, "boardingTargets.csv", sep = ""), header = TRUE, stringsAsFactors = FALSE)
target_operator_totals <- aggregate(boarding_targets$target_boardings, by = list(operator = boarding_targets$operator), sum)
target_operator_totals <- data.frame(target_operator_totals)
colnames(target_operator_totals) <- c("operator", "targetBoardings")

# Compute expansion factors
expansion_factors <- obs_operator_totals
expansion_factors$targetBoardings <- target_operator_totals$targetBoardings[match(expansion_factors$operator, target_operator_totals$operator)]

expansion_factors <- expansion_factors %>%
  mutate(exp_factor = targetBoardings/boardWeight) 

unified_collapsed$exp_factor <- expansion_factors$exp_factor[match(unified_collapsed$operator, expansion_factors$operator)]
unified_collapsed$boardWeight_2010 <- unified_collapsed$boards * unified_collapsed$exp_factor
unified_collapsed$tripWeight_2010 <- unified_collapsed$trips * unified_collapsed$exp_factor

#check final expanded boardings
obs_operator_totals_check <- aggregate(unified_collapsed$boardWeight_2010, by = list(operator = unified_collapsed$operator), sum)
obs_operator_totals_check <- data.frame(obs_operator_totals_check)
colnames(obs_operator_totals_check) <- c("operator", "boardWeight2010")
obs_operator_totals_check$targetBoardings <- target_operator_totals$targetBoardings[match(obs_operator_totals_check$operator, target_operator_totals$operator)]
obs_operator_totals_check

#Calculate distribution of trips/boardings by operator, tour purpose, access mode and auto sufficiency
obs_operator_trips <- aggregate(unified_collapsed$tripWeight_2010, by = list(operator = unified_collapsed$operator), sum)
colnames(obs_operator_trips) <- c("operator", "op_totTrips")
unified_collapsed <- merge(x=unified_collapsed, y=obs_operator_trips, by="operator", all.x = TRUE)

obs_operator_brdngs <- aggregate(unified_collapsed$boardWeight_2010, by = list(operator = unified_collapsed$operator), sum)
colnames(obs_operator_brdngs) <- c("operator", "op_totBrdngs")
unified_collapsed <- merge(x=unified_collapsed, y=obs_operator_brdngs, by="operator", all.x = TRUE)

unified_collapsed$shares_trips <- unified_collapsed$tripWeight_2010/unified_collapsed$op_totTrips
unified_collapsed$shares_brdngs <- unified_collapsed$boardWeight_2010/unified_collapsed$op_totBrdngs

write.csv(unified_collapsed, paste(OBS_Dir, "Reports//unified_collapsed.csv", sep = ""), row.names = FALSE)

# Copy technology from target boardings
unified_collapsed <- merge(x=unified_collapsed, y=boarding_targets[boarding_targets$surveyed==1,c("operator","technology")], by="operator", all.x = TRUE)

#Calculate distribution of trips/boardings by technology, tour purpose, access mode and auto sufficiency
unified_collapsed_technology <- aggregate(cbind(exp_factor, boards, trips, boardWeight_2010, tripWeight_2010)~technology+tourPurpose+accessMode+autoSuff, data = unified_collapsed, sum)
unified_collapsed_technology$exp_factor <- NA

obs_technology_trips <- aggregate(unified_collapsed_technology$tripWeight_2010, by = list(technology = unified_collapsed_technology$technology), sum)
colnames(obs_technology_trips) <- c("technology", "op_totTrips")
unified_collapsed_technology <- merge(x=unified_collapsed_technology, y=obs_technology_trips, by="technology", all.x = TRUE)

obs_technology_brdngs <- aggregate(unified_collapsed_technology$boardWeight_2010, by = list(technology = unified_collapsed_technology$technology), sum)
colnames(obs_technology_brdngs) <- c("technology", "op_totBrdngs")
unified_collapsed_technology <- merge(x=unified_collapsed_technology, y=obs_technology_brdngs, by="technology", all.x = TRUE)

unified_collapsed_technology$shares_trips <- unified_collapsed_technology$tripWeight_2010/unified_collapsed_technology$op_totTrips
unified_collapsed_technology$shares_brdngs <- unified_collapsed_technology$boardWeight_2010/unified_collapsed_technology$op_totBrdngs

write.csv(unified_collapsed_technology, paste(OBS_Dir, "Reports//unified_collapsed_technology.csv", sep = ""), row.names = FALSE)

#Remaining total boardings by technology to be distributed
other_operators <- aggregate(boarding_targets$target_boardings[boarding_targets$surveyed==0], by = list(tech = boarding_targets$technology[boarding_targets$surveyed==0]), sum)

# Calculate transfer rates by operator
transfer_data <- aggregate(cbind(boards, trips)~operator, data = unified_collapsed, sum, na.rm = TRUE)
transfer_data <- transfer_data %>%
  mutate(transfer_rate = boards/trips) 
transfer_data

# Caculate transfer rate by technology
transfer_data_tech <- aggregate(cbind(boards, trips)~technology, data = unified_collapsed, sum, na.rm = TRUE)
transfer_data_tech <- transfer_data_tech %>%
  mutate(transfer_rate = boards/trips) 
transfer_data_tech

#Other operator commuter rail targets [Caltrain transfer rates and distribution used]
CR_boardings <- other_operators$x[other_operators$tech=="CR"]
CR_trips <- CR_boardings/transfer_data$transfer_rate[transfer_data$operator=="Caltrain"]
other_CR_distribution <- unified_collapsed[unified_collapsed$operator=="Caltrain",]
other_CR_distribution$operator <- "Other_CR"
other_CR_distribution$boardWeight_2010 <- CR_boardings*other_CR_distribution$shares_brdngs
other_CR_distribution$tripWeight_2010 <- CR_trips*other_CR_distribution$shares_trips
other_CR_distribution$boards <- NA
other_CR_distribution$trips <- NA
other_CR_distribution$exp_factor <- NA

#Other operator local bus targets [LAVTA transfer rates and distribution used]
LB_boardings <- other_operators$x[other_operators$tech=="LB"]
LB_trips <- LB_boardings/transfer_data$transfer_rate[transfer_data$operator=="LAVTA"]
other_LB_distribution <- unified_collapsed[unified_collapsed$operator=="LAVTA",]
other_LB_distribution$operator <- "Other_LB"
other_LB_distribution$boardWeight_2010 <- LB_boardings*other_LB_distribution$shares_brdngs
other_LB_distribution$tripWeight_2010 <- LB_trips*other_LB_distribution$shares_trips
other_LB_distribution$boards <- NA
other_LB_distribution$trips <- NA
other_LB_distribution$exp_factor <- NA

#Other operator express bus targets
EB_boardings <- other_operators$x[other_operators$tech=="EB"]
EB_trips <- EB_boardings/transfer_data_tech$transfer_rate[transfer_data_tech$technology=="EB"]
other_EB_distribution <- unified_collapsed_technology[unified_collapsed_technology$technology=="EB",]
other_EB_distribution$operator <- "Other_EB"
other_EB_distribution$boardWeight_2010 <- EB_boardings*other_EB_distribution$shares_brdngs
other_EB_distribution$tripWeight_2010 <- EB_trips*other_EB_distribution$shares_trips
other_EB_distribution$boards <- NA
other_EB_distribution$trips <- NA
other_EB_distribution$exp_factor <- NA

# by operator
unified_collapsed <- unified_collapsed[,c("operator", "tourPurpose", "accessMode", "autoSuff", "boards", 
										  "trips", "exp_factor", "boardWeight_2010", "tripWeight_2010", "shares_trips", "shares_brdngs")]
other_LB_distribution <- other_LB_distribution[,c("operator", "tourPurpose", "accessMode", "autoSuff", "boards", 
										  "trips", "exp_factor", "boardWeight_2010", "tripWeight_2010", "shares_trips", "shares_brdngs")]
other_EB_distribution <- other_EB_distribution[,c("operator", "tourPurpose", "accessMode", "autoSuff", "boards", 
										  "trips", "exp_factor", "boardWeight_2010", "tripWeight_2010", "shares_trips", "shares_brdngs")]
other_CR_distribution <- other_CR_distribution[,c("operator", "tourPurpose", "accessMode", "autoSuff", "boards", 
										  "trips", "exp_factor", "boardWeight_2010", "tripWeight_2010", "shares_trips", "shares_brdngs")]
all_collapsed_operators <- rbind(unified_collapsed, other_LB_distribution,other_EB_distribution,other_CR_distribution)

# by technology
all_collapsed_technology <- all_collapsed_operators
all_collapsed_technology <- merge(x=all_collapsed_technology, y=boarding_targets[boarding_targets$surveyed==1,c("operator","technology")], by = "operator", all.x = TRUE)
all_collapsed_technology$technology[all_collapsed_technology$operator=="Other_CR"] <- "CR"
all_collapsed_technology$technology[all_collapsed_technology$operator=="Other_EB"] <- "EB"
all_collapsed_technology$technology[all_collapsed_technology$operator=="Other_LB"] <- "LB"
all_collapsed_technology <- aggregate(cbind(tripWeight_2010)~technology+tourPurpose+accessMode+autoSuff, data = all_collapsed_technology, sum)

write.csv(all_collapsed_operators, paste(OBS_Dir, "Reports\\transit_trip_targets_operators.csv", sep = ""), row.names = FALSE)
write.csv(all_collapsed_technology, paste(OBS_Dir, "Reports\\transit_trip_targets_technology.csv", sep = ""), row.names = FALSE)

#write.csv(trips, paste(OBS_Dir,"transitTripSummary_OBS.csv", sep = ""), row.names = FALSE)


## Compute summary statistics
#write.table("number of records", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE)
#write.table(nrow(OBS), file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#
#write.table("number of unlinked trips", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#unlinkedtrips <- xtabs(OBS$weight~OBS$operator)
#unlinkedtrips <- as.data.frame(unlinkedtrips)
#write.table(unlinkedtrips, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#write.table("number of linked trips", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#linkedtrips <- xtabs(OBS$trip_weight~OBS$operator)
#linkedtrips <- as.data.frame(linkedtrips)
#write.table(linkedtrips, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#destPurpose <- table(OBS$dest_purp)
#destPurpose <- as.data.frame(destPurpose)
#write.table("destPurpose", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(destPurpose, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#tourPurpose <- table(OBS$tour_purp)
#tourPurpose <- as.data.frame(tourPurpose)
#write.table("tourPurpose", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(tourPurpose, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#lineHaul <- table(OBS$path_line_haul)
#lineHaul <- as.data.frame(lineHaul)
#write.table("lineHaul", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(lineHaul, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#accessMode <- table(OBS$access_mode)
#accessMode <- as.data.frame(accessMode)
#write.table("accessMode", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(accessMode, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#egressMode <- table(OBS$egress_mode)
#egressMode <- as.data.frame(egressMode)
#write.table("egressMode", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(egressMode, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)
#
#autoSuff <- table(OBS$auto_suff)
#autoSuff <- as.data.frame(autoSuff)
#write.table("autoSuff", file = paste(OBS_Dir, outFile, sep = ""), row.names = FALSE, append = TRUE)
#write.table(autoSuff, file = paste(OBS_Dir, outFile, sep = ""),sep = ",", row.names = FALSE, append = TRUE)


#View(OBS[(OBS$tour_purp=='missing'), c("operator", "work_status", "student_status", "approximate_age", "work_before",
#                                 "work_after", "school_before", "school_after","orig_purp",  
#                                 "dest_purp", "tour_purp")])


# Turn back warnings;
options(warn = oldw)
