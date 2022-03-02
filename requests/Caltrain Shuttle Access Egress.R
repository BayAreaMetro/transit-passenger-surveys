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
         transfer_to_tech,summary_access, summary_egress,final_tripWeight_2015, final_boardWeight_2015) 
 
# Append shuttle entity by transfer from and transfer to fields
# Within the transfer succession, apply the detail for the most recent transfer
# Select only ID and shuttle variables for later joining

shuttle_before <- temp2 %>% 
  filter(transfer_from=="Bay Area Shuttles") %>% mutate(
    shuttle_from = case_when(
      is.na(second_before_operator_detail)             ~ first_before_operator_detail,
      is.na(third_before_operator_detail)              ~ second_before_operator_detail,
      !(is.na(third_before_operator_detail))           ~ third_before_operator_detail,
      TRUE                                             ~ "coding_mistake"
    )
  ) %>% select(ID,shuttle_from)

shuttle_after <- temp2 %>% 
  filter(transfer_to=="Bay Area Shuttles") %>% mutate(
    shuttle_to = first_after_operator_detail) %>% 
      select(ID,shuttle_to)

# Join shuttle detail data frames and subset data for export

working <- temp2 %>% 
  left_join(.,shuttle_before,by="ID") %>% 
  left_join(.,shuttle_after,by="ID") %>% 
  select(operator,ID, access_mode, access_mode_model, egress_mode, egress_mode_model, 
         onoff_enter_station, onoff_exit_station, operator, first_before_operator_detail, 
         second_before_operator_detail, third_before_operator_detail, 
         first_after_operator_detail, second_after_operator_detail, 
         third_after_operator_detail, first_before_operator, second_before_operator, 
         third_before_operator, first_after_operator, second_after_operator, 
         third_after_operator, first_before_technology, second_before_technology, 
         third_before_technology, first_after_technology, second_after_technology, 
         third_after_technology, transfer_from, transfer_to, transfer_from_tech, 
         transfer_to_tech,shuttle_from,shuttle_to, summary_access, summary_egress,
         final_tripWeight_2015, final_boardWeight_2015) 


# Export data 

write.csv(working, file.path(output_location,"TPS_Caltrain_Summarized_AccessEgress.csv"),row.names = F, quote = T)

