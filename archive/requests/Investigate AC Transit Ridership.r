# Investigate AC Transit Ridership.r

# Import libraries

library(tidyverse)

# Set up input and output directories
TPS_in          <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights_lat_lon_2021-12-14.Rdata"

# Bring in TPS file
load(TPS_in)

# Subset AC Transit bus service

ACTransit <- TPS %>% 
  filter(grepl("ac transit",.$operator,ignore.case = T))

# Summarize by route

route <- ACTransit %>% 
  group_by(route) %>% 
  summarize(total=sum(final_boardWeight_2015))

# Save version of data

write.csv(route,file = "M:/Data/Requests/Flavia Tsang/TPS2015 AC Transit Route Ridership.csv",row.names = F)


