# BART and ACE Version with Census Tract and TM1 TAZ Geography

# Library

library(tidyverse)

# Input public data file

input <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/public_version/TPS_Public_Version_2023-05-16.Rdata"

load(input)

# Take out TAP, and TM2 geography; keep in TM1 and census tract geos
# Leave in lat/lon for boarding/alighting locations only
# Filter for BART 2015 and ACE 2019 records only

export <- final %>% 
  select(-grep("tm2|tap",names(.))) %>% 
  filter(operator %in% c("ACE","BART"))

# Export dataset

write.csv(export,file = "M:/Data/Requests/Valley Link Transit/MTC Dataset BART and ACE.csv",row.names = F)
