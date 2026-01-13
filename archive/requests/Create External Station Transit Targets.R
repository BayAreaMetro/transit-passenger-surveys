# Create External Station Transit Targets.R
# Bring in raw data and keep external stations to develop targets

#=========================================================================================================================
# DIRECTORIES AND LIBRARIES
#=========================================================================================================================

USERPROFILE     <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_drive_dir   <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
TPS_Dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data"
wd              <- "M:/Data/OnBoard/Bespoke/External Transit Trip Matrix Creation"
setwd(wd)


suppressMessages(library(tidyverse))


#=========================================================================================================================
# READ INPUTS
#=========================================================================================================================

# Read TPS dataset survey data
load(file.path(TPS_Dir,     "survey_combined_2021-09-02.RData"))


#=========================================================================================================================
# DATA CLEANING, IMPUTATION & TRASNFORMATION
#=========================================================================================================================

# Remove weekend records, all older vintages of operators surveyed more than once, and SMART (not in 2015 network)
# Also remove "dummy records" (BART, Caltrain, Muni) used for weighting purposes but lacking characteristics
#------------------------
TPS <- survey_combine %>% filter(weekpart=="WEEKDAY" & 
                               !(operator %in% c("AC Transit", "ACE", "County Connection", 
                                                 "Golden Gate Transit", "LAVTA", "Napa Vine", 
                                                 "Petaluma Transit", "Santa Rosa CityBus", 
                                                 "SF Bay Ferry/WETA", "Sonoma County Transit", 
                                                 "TriDelta", "Union City Transit") & survey_year<2015)) %>% 
                              filter(operator!="SMART" | is.na(operator)) %>% 
                              filter(access_mode!="Missing - Dummy Record" | is.na(access_mode)) 

# Code Capitol Corridor and ACE Records that start and/or end inside the Bay Area

bay_cap <- c("Jack London Square", "Berkeley", "Suisun-fairfield", 
             "Emeryville", "Fairfield/Vacaville Station", "Martinez", "San Jose", 
             "Richmond", "Santa Clara University", "Santa Clara Great America", 
             "Fremont", "Hayward", "Oakland Coliseum")

bay_ace <- c("SAN JOSE", "SANTA CLARA", "GREAT AMERICA", "FREMONT", "PLEASANTON", 
             "VASCO ROAD", "LIVERMORE")

# Create file just for use with ACE and Capitol Corridor

TPS_ACE_CC <- TPS %>% 
  filter(operator %in% c("ACE","Capitol Corridor")) %>% 
  select(ID,operator,day_part,onoff_enter_station,onoff_exit_station,weight) %>% 
  mutate(ace_ix = if_else(operator=="ACE" & onoff_enter_station %in% bay_ace &
                            !(onoff_exit_station %in% bay_ace),weight,0),
         ace_xi = if_else(operator=="ACE" & !(onoff_enter_station %in% bay_ace) &
                            onoff_exit_station %in% bay_ace,weight,0),
         cc_ix  = if_else(operator=="Capitol Corridor" & onoff_enter_station %in% bay_cap &
                            !(onoff_exit_station %in% bay_cap),weight,0),
         cc_xi  = if_else(operator=="Capitol Corridor" & !(onoff_enter_station %in% bay_cap) &
                            onoff_exit_station %in% bay_cap,weight,0))


final <- TPS_ACE_CC %>% 
  group_by(operator,day_part) %>% 
  summarize(ace_ix=sum(ace_ix),
            ace_xi=sum(ace_xi),
            cc_ix =sum(cc_ix),
            cc_xi =sum(cc_xi))

write.csv(final, file="ACE and Capitol Corridor External Transit Trips.csv", row.names = F)


