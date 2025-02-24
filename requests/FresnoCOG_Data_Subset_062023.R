data_in <- "M:/Data/OnBoard/Data and Reports/_data_Standardized/share_data/public_version/TPS_Public_Version_2023-05-16.Rdata"
load(data_in)


export <- final %>% 
  filter(operator %in% c("ACE","SMART","VTA [LRT]")) %>% 
  select(-c(grep("tract|tm2",names(.))))


write.csv(export,file = paste0("M:/Data/Requests/FresnoCOG/","MTC_SMART_ACE_VTA_LRT_022425.csv"),row.names=FALSE)

