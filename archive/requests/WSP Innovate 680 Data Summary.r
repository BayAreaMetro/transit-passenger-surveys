# WSP Innovate 680 Data Summary.r
# SI


# Set working directory

wd   <- "M:/Data/Requests/Christopher Duddy"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Data location

tps_data <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-05-06.RData"

# Bring in data and subset LAVTA and Tri-Delta Surveys

load(tps_data)

wsp_tps <- data.ready %>% 
  filter(operator %in% c("TriDelta","LAVTA"), survey_year %in% c(2018,2019), grepl("70X|200|201",route)) %>% 
  select(operator,survey_year,weekpart,route,orig_tm2_maz,dest_tm2_maz,weight)

write.csv(wsp_tps,"MTC Transit Passenger Survey 70x_200_201.csv",row.names = FALSE,quote=T)






