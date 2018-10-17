
library(tidyverse)
options(stringsAsFactors = FALSE)

user_list <- data.frame(
  user = c("helseljw"), 
  path = c("../../Data and Reports/")
)

me <- Sys.getenv("USERNAME")
dir_path <- user_list %>%
  filter(user == me) %>%
  .$path

sf_muni_path <- paste0(dir_path, 
  "Muni/As CSV/MUNI_DRAFTFINAL_20171114 NO POUND OR SINGLE QUOTE.csv")

standard_routes <- data.frame()

sf_muni <- read.csv(sf_muni_path) %>%
  rename_all(tolower)
