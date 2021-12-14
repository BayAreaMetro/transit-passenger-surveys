# Transit Passenger Survey Extract for SFCTA.R
# Extract a version of the updated onboard survey data for SFCTA, removing MAZs

# Import Library

suppressMessages(library(tidyverse))

# Input TPS

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
load (TPS_SURVEY_IN)

# Output location

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
BOX_TM               <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
Output               <- file.path(BOX_TM,"Share Data","Protected Data","Drew Cooper")

# Remove MAZ-level variables

final <- TPS %>% 
  select(!grep("maz",names(TPS),ignore.case = T))

write.csv(final, file.path(Output,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZ_Only.csv"), row.names = FALSE, quote = T)


 