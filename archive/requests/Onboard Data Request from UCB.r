# Onboard Data Request from UCB.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Meiqing Li/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

username      <- Sys.getenv("USERNAME")
Box_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
load(Box_location)

# Summarize auto sufficiency for Big 7, weekday only

final <- onboard %>% 
  filter(operator %in% c(                                     # Filter the operators requested
    "AC Transit [EXPRESS]",         "AC Transit [LOCAL]",                             
    "BART",                         "Caltrain",                        
    "County Connection [EXPRESS]",  "County Connection [LOCAL]",
    "Tri-Delta",
    "SF Muni [LOCAL]",              "SF Muni [LRT]",                                  
    "VTA [EXPRESS]",                "VTA [LOCAL]",                
    "VTA [LRT]")) %>% 
  select(-(grep("_maz", names(.), value=TRUE)))               # Select out any variables with the "_maz" suffix

# Write out final CSV files

write.csv(final,"MTC_Onboard_Survey_7.csv",row.names = FALSE,quote=T)





