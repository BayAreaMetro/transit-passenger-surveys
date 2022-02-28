# Data Requests for Link 21 and CCTA.R
# Bring in raw data and exclude columns with MAZ-level information

# Directories and libraries

tps_dir         <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata"
output_location <- "M:/Data/OnBoard/Bespoke/Caltrain Shuttle Summaries"

suppressMessages(library(tidyverse))

# Read in TPS dataset and subset Caltrain records and stations, access/egress, weight Columns

load(tps_dir)
temp1 <- TPS %>% 
  filter(operator=="Caltrain") %>% 
  select(ID,
         access_mode,
         access_mode_model,
         egress_mode,
         egress_mode_model,
         onoff_enter_station,
         onoff_exit_station,
         grep("operator|detail|technology|transfer|final_",names(.)))

# Create summary access/egress variables that include shuttle and non-transit shuttle
# Convert from wide to long format for use in Tableau

temp2 <- temp1 %>% 
  mutate(
    summary_access=if_else(is.na(transfer_from),access_mode,transfer_from),
    summary_egress=if_else(is.na(transfer_to),egress_mode,transfer_to))
    
working <- temp2 %>% 
  select(operator,ID, access_mode, access_mode_model, egress_mode, egress_mode_model, 
         onoff_enter_station, onoff_exit_station, operator, first_before_operator_detail, 
         second_before_operator_detail, third_before_operator_detail, 
         first_after_operator_detail, second_after_operator_detail, 
         third_after_operator_detail, first_before_operator, second_before_operator, 
         third_before_operator, first_after_operator, second_after_operator, 
         third_after_operator, first_before_technology, second_before_technology, 
         third_before_technology, first_after_technology, second_after_technology, 
         third_after_technology, transfer_from, transfer_to, transfer_from_tech, 
         transfer_to_tech, final_tripWeight_2015, final_boardWeight_2015, 
         final_expansionFactor, summary_access, summary_egress) %>% 
  gather(.,variable, values,-operator,-ID,-onoff_enter_station,-onoff_exit_station)

# Remove MAZ-level records

final <- TPS %>% 
  select(-(grep("MAZ",names(.),value = T,ignore.case = T)))

# Export data to both Link21 and CCTA locations

write.csv(final, file.path(Link21,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZs.csv"),row.names = F, quote = T)
write.csv(final, file.path(CCTA,"TPS_Model_Version_PopulationSim_Weights2021-09-02_TAZs.csv"),row.names = F, quote = T)


