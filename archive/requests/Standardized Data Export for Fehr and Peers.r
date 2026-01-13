# Standardized Data Export for Fehr and Peers.r
# SI


# Set working directory

username        <- Sys.getenv("USERNAME")
output_location <-paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Protected Data/Fehr_Peers/")

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

Box_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
load(Box_location)

# Output weekday data for relevant operators

final <- onboard %>% 
  filter(weekpart=="WEEKDAY" & operator %in% c(
    "ACE",                          "Union City",
    "AC Transit [EXPRESS]",         "AC Transit [LOCAL]",                             
    "BART",                         "Caltrain",                        
    "SamTrans [EXPRESS]",           "SamTrans [LOCAL]",             
    "VTA [EXPRESS]",                "VTA [LOCAL]",                
    "VTA [LRT]")) 

# Write out final CSV files

write.csv(final,paste0(output_location,"MTC_Onboard_Survey_Consolidated.csv"),row.names = FALSE,quote=T)






