# Data Requests for Link 21 and CCTA.R
# Bring in raw data and exclude columns with MAZ-level information

# Directories and libraries

TPS_Dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
USERPROFILE     <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
box_drive_dir   <- file.path(USERPROFILE, "Box", "Modeling and Surveys")
CCTA            <- file.path(box_drive_dir,"Share Data","Protected Data","CCTA")
Link21          <- file.path(box_drive_dir,"Share Data","Protected Data","BART Link 21")

suppressMessages(library(tidyverse))


# Read in TPS dataset

load(TPS_Dir)

# Remove MAZ-level records

final <- TPS %>% 
  select(-(grep("MAZ",names(.),value = T,ignore.case = T)))

# Export data to both Link21 and CCTA locations

write.csv(final, file.path(Link21,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZs.csv"),row.names = F, quote = T)
write.csv(final, file.path(CCTA,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZs.csv"),row.names = F, quote = T)


