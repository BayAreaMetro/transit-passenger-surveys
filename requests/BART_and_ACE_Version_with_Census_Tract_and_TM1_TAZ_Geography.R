# BART and ACE Version with Census Tract and TM1 TAZ Geography

# Library

library(tidyverse)

# Input public data file

input <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/public_version/TPS_Public_Version_2023-05-16.Rdata"

load(input)

# Take out lat/long and TM2 geography, keep in TM1 and census tract geos
# Filter out BART and ACE records only

export <- final %>% 
  select(-grep("lat|lon|tm2",names(.)))
