library(tidyverse)

data <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/public_version/TPS_Public_Version_2023-05-16.Rdata"
load(data)

export <- final %>% 
  filter(operator %in% c("AC Transit [LOCAL]","AC Transit [EXPRESS]",
                         "County Connection [LOCAL]","County Connection [EXPRESS]",
                         "TriDelta","WestCAT [EXPRESS]","WestCAT [LOCAL]", "BART" ))


write.csv(export,file = paste0("M:/Data/Requests/Mike Iswalt/","TPS_Extract_062023.csv"),row.names=FALSE)
