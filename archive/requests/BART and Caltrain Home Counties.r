# BART and Caltrain Home Counties.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Alix Bockelman/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

username         <- Sys.getenv("USERNAME")
survey_location  <- paste0("C:/Users/",username,"/Box/Modeling and Surveys/Share Data/Onboard-Surveys/Survey_Database_122717/OBS_PopulationSim_Weights.rdata")
county_location  <- "M:/Crosswalks/taz1454/taz1454_tract.csv"
load(survey_location)
county           <- read.csv(county_location,header = TRUE, as.is = TRUE) %>% 
  select(TAZ1454,County_Name)

# Summarize home counties for BART and Caltrain, weekday only

final <- onboard %>% 
  filter(weekpart=="WEEKDAY" & operator %in% c(
    "BART",                         "Caltrain")) %>% 
  left_join(.,county, by = c("home_taz"="TAZ1454")) %>% 
  rename("Home_County_Name"="County_Name") %>% 
  group_by(operator,Home_County_Name) %>% 
  summarize(total=sum(final_boardWeight_2015)) %>% 
  spread(operator,total)

# Write out final CSV files

write.csv(final,"BART and Caltrain Home Counties 042821.csv",row.names = FALSE)





