library(tidyverse)
library(rjson)
library(httr)
library(jsonlite)
library(sf)
library(geojson)
library(geojsonio)
options(stringsAsFactors = FALSE)

tl_base <- "https://transit.land/"
# operator_endpoint <- paste0(tl_base, "api/v1/operators?per_page=1000")
route_endpoint <- paste0(tl_base, "/api/v1/routes?served_by=o-9q9-BART")

# # 
# # test <- GET(operator_endpoint)
# operator_df <- fromJSON(operator_endpoint)
# op_temp <- operator_df
# 
# max_limit <- 1
# 
# while (!is.na(op_temp$meta$next & max_limit < 100)) {
#   operator_endpoint <- op_temp$meta$next
#   op_temp <- fromJSON(operator_endpoint)
#   operator_df <-  bind_rows(operator_df,
#                             op_temp)
#   
#   max_limit <- max_limit + 1
# }

route_df <- fromJSON(route_endpoint)

routes <- data.frame(operator_onestop = route_df$routes[["operated_by_onestop_id"]],
                     operator = route_df$routes[["operated_by_name"]],
                     route_onestop = route_df$routes[["onestop_id"]],
                     route_name = route_df$routes[["name"]])
route_endpoint <- route_df$meta$`next`
max_limit <- 1

while (!is.na(route_endpoint) & max_limit < 10) {
  route_temp_df <- fromJSON(route_endpoint)
  
  route_temp <- data.frame(operator_onestop = route_temp_df$routes[["operated_by_onestop_id"]],
                           operator = route_temp_df$routes[["operated_by_name"]],
                           route_onestop = route_temp_df$routes[["onestop_id"]],
                           route_name = route_temp_df$routes[["name"]])
  routes <- bind_rows(routes, route_temp)
  
  route_endpoint <- rt_temp$meta$`next`
  max_limit <- max_limit + 1
}


sfmta_routes <- fromJSON(file = "sfmta_routes.json")
bart_routes <- fromJSON(file = "bart_routes.json")
sf_10 <- fromJSON(file = "sf_10.json")
sf_100 <- fromJSON(file = "sf_100.json")
sf_1000 <- fromJSON(file = "sf_1000.json")
sf_offset <- fromJSON(file = "sf_offset_100.json")


sf_muni <- read.csv("../../Data and Reports/Muni/As CSV/MUNI_Data_08102016_09062016_revised_working NO POUND OR SINGLE QUOTE.csv") %>%
  rename_all(tolower)

  muni_routes <- sf_muni %>%
    select_at(vars(contains("transfer"))) %>%
  gather(id, value)
