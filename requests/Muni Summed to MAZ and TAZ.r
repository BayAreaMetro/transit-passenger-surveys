# Onboard Data Request from UCB.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Matt Lavrinets/"
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
    "SF Muni [LOCAL]","SF Muni [LRT]")) 

MAZ <- final %>% 
  group_by(home_maz) %>% 
  summarize(MAZ_Total=n())

TAZ <- final %>% 
  group_by(home_taz) %>% 
  summarize(MAZ_Total=n())


# Write out final CSV files

write.csv(MAZ,"Muni_Onboard_Survey_MAZ.csv",row.names = FALSE,quote=T)
write.csv(TAZ,"Muni_Onboard_Survey_TAZ.csv",row.names = FALSE,quote=T)





