library(tidyverse)
library(httr)
library(jsonlite)
library(sf)
options(stringsAsFactors = FALSE)

tl_base <- "https://transit.land/"
# operator_endpoint <- paste0(tl_base, "api/v1/operators?per_page=1000")
route_endpoint <- paste0(tl_base, "/api/v1/routes?served_by=o-9q9-BART?per_page=100")

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

