# Investigate Muni NX N Express Bus Service.r

# Set working directory

#wd <- "C:/Users/sisrael/Box/Modeling and Surveys/Share Data/
#setwd(wd)

# Import libraries

library(tidyverse)

# Set up input and output directories
TPS_in          <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights_lat_lon_2021-12-14.Rdata"

# Bring in TPS file
load(TPS_in)

# Subset Muni and NX bus service in separate files

Muni <- TPS %>% 
  filter(grepl("muni",.$operator,ignore.case = T))

NX_bus <- Muni %>% 
  filter(grepl("nx",.$route,ignore.case = T))

print(sum(NX_bus$final_boardWeight_2015))
  
# Write out final CSV files

#write.csv(final,file.path(wd,"Transit_Passenger_Survey_Bicounty_TAZ_063022.csv"),row.names = FALSE)


