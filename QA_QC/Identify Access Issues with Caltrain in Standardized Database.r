# Identify Access Issues with Caltrain in Standardized Database.r
# SI


# Set working directory

output_location <-("M:/Data/OnBoard/Data and Reports/QA_QC_Results/")

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

username        <- Sys.getenv("USERNAME")
Box_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
load(Box_location)

f_dict_standard <- "C:/Users/sisrael/Documents/GitHub/onboard-surveys/make-uniform/production/Dictionary for Standard Database.csv"

# Output suspect records for Caltrain

caltrain <- onboard %>% 
  filter(weekpart=="WEEKDAY" & operator %in% c("Caltrain")) 

suspect_records <- caltrain %>% 
  filter(onoff_enter_station=="San Francisco" & access_mode=="pnr") %>% 
  arrange(.,by="ID")









