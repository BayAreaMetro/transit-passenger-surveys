# Muni Export for Georgia Tech.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Georgia Tech/"
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
    "SF Muni [LOCAL]",              "SF Muni [LRT]")) 

write.csv(final,"MTC_Onboard_Survey_Muni.csv",row.names = FALSE,quote=T)






