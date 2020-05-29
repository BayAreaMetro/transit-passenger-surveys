# Big 7 by auto sufficiency.r
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
  group_by(operator,auto_suff) %>% 
  summarize(total=sum(boardWeight_2015)) %>% 
  spread(auto_suff,total)

# Write out final CSV files

write.csv(final,"Big7_Auto_Sufficiency.csv",row.names = FALSE)





