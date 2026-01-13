# Transit Passenger Survey Fare Discount Analysis.R
# Analyze operator discount information for transit passengers based on fare category

# Import Library

suppressMessages(library(tidyverse))

# Input person PUMS file

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-06-09.Rdata"
OUTPUT = "M:/Data/Requests/Joel Freedman/"
load (TPS_SURVEY_IN)

# Summarize TPS by operator and fare category

data.summary <- TPS %>% 
  filter(!(is.na(fare_category))) %>% 
  filter(fare_category!="") %>% 
  group_by(operator,fare_category) %>% 
  summarize(total=sum(final_boardWeight_2015))

write.csv(data.summary, paste0(OUTPUT, "TPS by operator and fare category.csv"), row.names = FALSE, quote = T)


 