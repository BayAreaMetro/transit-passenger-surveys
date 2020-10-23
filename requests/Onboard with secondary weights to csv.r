# Onboard with secondary weights to csv.r
# SI

# Import libraries

suppressMessages(library(tidyverse))

# Set up input directory

username      <- Sys.getenv("USERNAME")
Box_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
load(Box_location)

# Set working directory

wd <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Protected Data/BART Second Crossing/")
setwd(wd)


# Write out final CSV file

write.csv(onboard,"MTC Consolidated Onboard Survey.csv",row.names = FALSE)





