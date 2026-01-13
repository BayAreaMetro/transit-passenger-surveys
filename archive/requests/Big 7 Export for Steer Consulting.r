# Big 7 Export for Steer Consulting.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Bill Bacon/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

username      <- Sys.getenv("USERNAME")
Box_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
load(Box_location)

# Summarize auto sufficiency for Big 7, weekday only

final <- onboard %>% 
  filter(weekpart=="WEEKDAY" & operator %in% c(
    "AC Transit [EXPRESS]",         "AC Transit [LOCAL]",                             
    "BART",                         "Caltrain",                        
    "Golden Gate Transit (ferry)",  "Golden Gate Transit [EXPRESS]",          
    "SamTrans [EXPRESS]",           "SamTrans [LOCAL]",             
    "SF Muni [LOCAL]",              "SF Muni [LRT]",                                  
    "VTA [EXPRESS]",                "VTA [LOCAL]",                
    "VTA [LRT]")) %>% 
  select(-(grep("_maz", names(.), value=TRUE)))               # Select out any variables with the "_maz" suffix

# Write out final CSV files

write.csv(final,"MTC_Onboard_Survey_7.csv",row.names = FALSE,quote=T)






