# LAVTA_with_Census_Tract_and_TM1_TAZ_Geography.R

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
  filter(operator %in% c("LAVTA"))

# Export dataset

write.csv(export,file = "M:/Data/Requests/Mike Iswalt/MTC Dataset with LAVTA.csv",row.names = F)
