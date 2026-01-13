# San_Mateo_Residents_Pre-Pandemic.R
# Calculate ridership for San Mateo residents on different operators before Covid

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))

# Set file directories for input and output

data_in <- "M:/Data/OnBoard/Data and Reports/_data_Standardized/share_data/public_version/TPS_Public_Version_2023-05-16.Rdata"
output_dir     <- "M:/Data/Requests/Raleigh McCoy"

load(data_in)

# Summarize weekday use by operator for San Mateo Residents
# Use the first part of the tract residence string, which includes county, and also TAZs within San Mateo County. 
# Some of the records have one or the other while most have both

san_mateo <- final %>%
  filter(startsWith(home_tract, "06081") | home_tm1_taz %in% 191:346) %>% 
  group_by(operator) %>% 
  summarize(total=sum(weight),.groups = "drop")

# Export data

write.csv(san_mateo,file.path(output_dir,"Pre-Covid_San_Mateo_Residents_Operators.csv"),row.names=F)


