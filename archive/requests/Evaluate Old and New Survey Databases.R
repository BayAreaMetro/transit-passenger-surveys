# Evaluate Old and New Survey Databases.R
# Compare transit passenger survey databases for RSG

# Import Library

suppressMessages(library(tidyverse))

# Input survey file

userprofile   <- Sys.getenv("USERPROFILE")
BOX_TM        <- file.path(userprofile,"Box","Modeling and Surveys","Share Data","Protected Data","Joel Freedman")
OLD_SURVEY_IN <- file.path(BOX_TM,"OBS_27Dec17","OBS_processed_weighted_RSG.csv")
NEW_SURVEY_IN <- file.path(BOX_TM,"TPS_Model_Version_PopulationSim_Weights2021-07-27.Rdata")
old_survey    <- read.csv(OLD_SURVEY_IN, header = T) %>% 
  mutate(access_concat=paste0(access_mode,"-",survey_tech))
load (NEW_SURVEY_IN)
new_survey    <- TPS %>% 
  mutate(access_concat=paste0(access_mode_model,"-",BEST_MODE))

# Summarize data for 2015

sum_2015_old <- old_survey %>% 
  group_by(access_concat) %>% 
  summarize(old_2015_targets_trips=sum(trip_weight2015), old_2015_targets_boardings=sum(board_weight2015))

sum_2015_new <- new_survey %>% 
  group_by(access_concat) %>% 
  summarize(new_2015_targets_trips=sum(final_tripWeight_2015), new_2015_targets_boardings=sum(final_boardWeight_2015)) 
  


