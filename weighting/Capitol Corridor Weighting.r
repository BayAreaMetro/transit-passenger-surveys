# Capitol Corridor Weighting.r
# SI


# Set working directory

wd <- "M:/Data/OnBoard/Data and Reports/Capitol Corridor/OD Survey 2019"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories

CAPCO_data_in <- file.path(wd,"As CSV","CAPCO19 Data-For MTC_NO POUND OR SINGLE QUOTE.csv")


# Summarize auto sufficiency for Big 7, weekday only

final <- onboard %>% 
  filter(weekpart=="WEEKDAY" & operator %in% c(
    "AC Transit [EXPRESS]",         "AC Transit [LOCAL]",                             
    "BART",                         "Caltrain",                        
    "Golden Gate Transit (ferry)",  "Golden Gate Transit [EXPRESS]",          
    "SamTrans [EXPRESS]",           "SamTrans [LOCAL]",             
    "SF Muni [LOCAL]",              "SF Muni [LRT]",                                  
    "VTA [EXPRESS]",                "VTA [LOCAL]",                
    "VTA [LRT]")) %>% 
  group_by(operator,auto_suff) %>% 
  summarize(total=sum(boardWeight_2015)) %>% 
  spread(auto_suff,total)

# Write out final CSV files

write.csv(final,"Big7_Auto_Sufficiency 052920.csv",row.names = FALSE)





