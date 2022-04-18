# Transit Passenger Survey Extract for KSU.R
# Extract a version of the updated onboard survey data for KSU, purging geography other than TAPs

# Import Library

suppressMessages(library(tidyverse))

# Input TPS

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
load (TPS_SURVEY_IN)

# Output location

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
BOX_TM               <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
Output               <- file.path(BOX_TM,"Share Data","Protected Data","Greg Newmark")

# Remove MAZ-level variables

final <- TPS %>% 
  select(!grep("maz|taz",names(TPS),ignore.case = T)) %>% 
  filter(grepl("vta",TPS$operator,ignore.case = T)) %>% 
  filter(!(operator=="LAVTA"))

write.csv(final, file.path(Output,"VTA_2017_Model_Version_PopulationSim_Weights2021-09-02_TAP_Only.csv"), row.names = FALSE, quote = T)


 
