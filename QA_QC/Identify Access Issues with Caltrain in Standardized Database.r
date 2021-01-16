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

# Check dictionary code

dictionary_all <- read.csv(f_dict_standard, 
                           header = TRUE) %>%
  rename_all(tolower)

# Write out final CSV files

write.csv(final,paste0(output_location,"MTC_Onboard_Survey_Consolidated.csv"),row.names = FALSE,quote=T)






