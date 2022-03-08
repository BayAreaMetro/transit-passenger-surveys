# Muni Transit Passenger Survey 2017 with Access and Egress Distances Appended.R

# Import Libraries

suppressMessages(library(tidyverse))

# Input standardized survey file, subset Muni and remove PII variables

survey_dir <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/"
survey_in  <- file.path(survey_dir,"TPS_Model_Version_PopulationSim_Weights_Distances_2021-12-14.Rdata")
output_dir <- "M:/Data/Requests/Greg Erhardt"
load (survey_in)

final <- TPS_distance %>% 
  filter(operator %in% c("SF Muni [LOCAL]","SF Muni [LRT]")) %>% 
  select(-grep("_lat|_lon|_maz|_taz",names(.)))
  
# Export

write.csv(final,file = file.path(output_dir,"Muni 2017 TPS Access and Egress Distances.csv"),row.names = F)
