library(tidyverse)

load("~/GitHub/onboard-surveys/Data and Reports/_data Standardized/survey_standard_previous.Rdata")
previous_df <- survey.standard %>%
  rename(unique_ID = Unique_ID)

load("~/GitHub/onboard-surveys/Data and Reports/_data Standardized/survey_standard.Rdata")
current_df <- survey_standard

find_differences <- function(anti_outcomes_df, diffed_df) {
  
  variable_vector <- colnames(anti_outcomes_df) 
  
  df <- data.frame(unique_ID = character(), 
                   var_name = character(),
                   previous_outcome = character(),
                   current_outcome = character())
  
  for (variable in variable_vector) {
    
    if (variable == "unique_ID") next
    
    p_df <- anti_outcomes_df %>%
      select(unique_ID, one_of(variable)) %>%
      mutate(var_name = variable)
    
    c_df <- diffed_df %>%
      select(unique_ID, one_of(variable)) %>%
      mutate(var_name = variable)
    
    colnames(p_df) <- c("unique_ID", "previous_outcome", "var_name")
    colnames(c_df) <- c("unique_ID", "current_outcome", "var_name")
    
    w_df <- left_join(p_df, c_df, by = c("unique_ID", "var_name")) %>%
      mutate(previous_outcome = paste(previous_outcome)) %>%
      mutate(current_outcome = paste(current_outcome)) %>%
      mutate(keep = !(previous_outcome == current_outcome)) %>%
      mutate(keep = ifelse(is.na(previous_outcome) & !is.na(current_outcome), TRUE, keep)) %>%
      mutate(keep = ifelse(!is.na(previous_outcome) & is.na(current_outcome), TRUE, keep)) %>%
      filter(keep)
    
    df <- bind_rows(df, w_df)
    
  }
  
  return(df)
  
}

anti_df <- anti_join(previous_df, current_df, by = c("unique_ID"))
diff_df <- find_differences(anti_df, current_df)

relevant_df <- diff_df %>%
  filter(!(previous_outcome == "missing" & current_outcome == "NA"))

table(thin_df$var_name)

