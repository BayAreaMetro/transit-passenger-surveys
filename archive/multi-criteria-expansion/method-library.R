# method-library.R
#
# Purpose: A container for all the methods needed to carry out the multi-criteria expansion procedures.
#

# Overhead
library(stringr)
library(optimx)
library(reshape2)
suppressMessages(library(dplyr))

# Parameters
all_routes_string = "all_routes"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Method 1: Function to be optimized
# Inputs
# 1. `x` - a vector of length M representing each of the unique survey weights that need to be calculated
# 2. `obs_target_v` - a vector of length N containing each expansion target
# 3. `import_v` - a vector of length N containing the importance weight for each expansion target
# 4. `inc_mtx` - a matrix of dimensions M x N containing a dummy variable denoting the relevance of 
# each unique survey weight to each expansion target

optimization_function <- function(x, obs_target_v, import_v, inc_mtx) {
  
  # Compute estimated targets 
  est_target_v <- x %*% inc_mtx
  
  # Compute importance-weighted errors, which is the objective function
  error_df <- data.frame(obs_target_v, est_target_v, import_v)
  error_df <- error_df %>%
    mutate(error <- import_v * abs(est_target_v - obs_target_v))
  
  # Return errors
  return(sum(error_df$error))
  
}
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Method 2: Prepare optimization inputs
# Inputs
# 1. `target_counts_df` - a data frame of observed targets with the following fields:
# (a) `target_id` - integer, a unique integer for the target;
# (b) `target_count` - float, the target value for the target (i.e., the weights will be adjusted to 
# match the targets);
# (c) `importance_weight` - float, the importance weight for the target;
# (d) `target_category` - string, a label for each group of targets;
# (e) {survey variable names} - any number of columns that define the targets composition using 
# the survey variable names, e.g., route, direction, time_period.  Each string MUST correspond to a 
# variable in the survey data. 
#
# 2. `targets_defn_df` - a data frame of observed target definitions with the following fields:
# (a) `target_category` - string, a label for each group of targets;
# (b) `survey_variable` - string, a label for the variables that comprise the target, with each variable 
# being entered on a separate row in the database.  Each string MUST correspond to a variable in the 
# survey data, with the exception of `all_routes` which denotes a target that is applied to all records 
# in the survey.
#
# 3. `survey_df` - a data frame of the survey data with the following fields:
# (a) {survey variable names} - any number of columns, though it must contain the fields in 
# the `target_counts_df` that make up the targets
#
# 4. `rw_lower_scalar` - float scalar, the lower bound for the optimal survey record weight
#
# 5. `rw_upper_scalar` - float scalar, the upper bound for the optimal survey record weight
#
# Outputs (a vector)
# 1.  `survey_with_results` - survey_df joined with weights from the optimization results
execute_optimization <- function(target_counts_df, 
                                 targets_defn_df, 
                                 survey_df,
                                 rw_lower_scalar,
                                 rw_upper_scalar){
  
  # Prepare the observed targets vector
  observed_targets_vector <- target_counts_df$target_count
  
  # Prepare the importance weights vector
  importance_weights_vector <- target_counts_df$importance_weight
  
  # Prepare the incidence matrix
  
  # Make sure the defined survey_variables are strings
  targets_defn_df <- targets_defn_df %>%
    mutate(survey_variable = as.character(survey_variable))
  
  # Extract a vector of the unique variables in the targets
  unique_variables <- data.frame(survey_variable = unique(targets_defn_df$survey_variable))
  
  # Remove special case "all_routes"
  unique_variables <- unique_variables %>%
    filter(survey_variable != all_routes_string) %>%
    mutate(survey_variable = as.character(survey_variable))
  
  # Throw an error if target variables are not in the survey
  check_variables <- unique_variables$survey_variable
  for (i in length(check_variables)) {
    if (!(check_variables[i] %in% names(survey_df))) {
      cat("Error: The target definitions file includes \'", check_variables[i], "\', but the survey file does not.\n")
      cat("The survey data is returned without weights computed.")
      return(survey_df)
    }
  }
  
  # Condensed the survey to the set of unique weights needed
  unique_weights <- survey_df %>%
    group_by_(.dots = unique_variables$survey_variable) %>%
    summarise(records = n()) %>%
    ungroup()
  
  # Add the special case all_routes incidence column
  all_routes_column <- targets_defn_df %>%
    filter(survey_variable == all_routes_string) %>%
    mutate(target_category_id = as.character(target_category_id))
  
  all_routes_column <- left_join(all_routes_column, target_counts_df, by = c("target_category_id"))
  
  all_routes_column_name <- all_routes_column$target_id
  
  incidence_matrix <- unique_weights %>%
    mutate(one = 1)
  
  # If all_routes is present, re-name row of ones, else delete the row of ones
  if (nrow(all_routes_column) > 0) {
    names(incidence_matrix)[names(incidence_matrix) == "one"] <- all_routes_column_name
  } else {
    incidence_matrix <- select(incidence_matrix, -one)
  } 
  
  # To create the rest of the incidence matrix, add a column of ones to the targets dataframe
  target_counts_df <- target_counts_df %>%
    mutate(one = 1)
  
  # Get the vector of target_categories
  target_categories_vector <- unique(targets_defn_df$target_category_id)
  
  # Loop over the target categories
  for (i in 1:length(target_categories_vector)) {
    
    # Loop over the variable names in the target category
    variable_names <- targets_defn_df %>%
      filter(target_category_id == target_categories_vector[i])
    
    variable_names <- variable_names$survey_variable
    
    # Account for all_routes exception
    if (variable_names[1] == all_routes_string) next
    
    # Build the formula string that we'll use in the casting
    formula_string <- ""
    for (j in 1:length(variable_names)) {
      
      if (j > 1) formula_string <- paste(formula_string, "+", sep = " ")
      
      formula_string <- paste(formula_string, variable_names[j], sep = " ")
      
    } # end for j
    
    formula_string <- paste(formula_string, "~ target_id", sep = " ")
    
    # select the relevant columns to cast
    these_targets <- target_counts_df %>%
      filter(target_category_id == target_categories_vector[i])
    
    casted <- dcast(these_targets, formula_string, value.var = "one")
    casted[is.na(casted)] <- 0
    
    incidence_matrix <- left_join(incidence_matrix, casted, by = variable_names)
    
  } # end for i
  
  # Trim the survey variable columns
  unique_variables_v <- unique_variables$survey_variable
  for (i in 1:length(unique_variables_v)) {
    incidence_matrix <- incidence_matrix %>%
      select(-matches(unique_variables_v[i]))
  }
  
  # Trim the records column
  incidence_matrix <- incidence_matrix %>%
    select(-records)
  
  # Fill the NAs with 0
  incidence_matrix[is.na(incidence_matrix)] <- 0
  
  # Save as matrix
  incidence_matrix <- data.matrix(incidence_matrix)
  
  # Prepare the upper and lower bound vectors
  unique_weights <- unique_weights %>%
    mutate(minimum_weights  = records * rw_lower_scalar) %>%
    mutate(maximum_weights  = records * rw_upper_scalar) %>%
    mutate(midpoint_weights = (minimum_weights + maximum_weights)/2.0)
  
  # Set the starting weights as the mid-point between the minimum and maximum weights
  starting_weights_vector <- unique_weights$minimum_weights
  
  # Run the optimization and record the time
  start_time <- proc.time()
  optimx_results <- optimx(starting_weights_vector,
                           fn = optimization_function,
                           method = "L-BFGS-B",
                           itnmax = 500,
                           lower = unique_weights$minimum_weights,
                           upper = unique_weights$maximum_weights,
                           obs_target_v = observed_targets_vector,
                           import_v = importance_weights_vector,
                           inc_mtx = incidence_matrix)
  
  cat("Information: Optimization run time = ", 
      as.character(round((proc.time() - start_time)[1], digits = 2)),
      " seconds; ",
      as.character(round(((proc.time() - start_time)[1])/60.0, digits = 2)),
      "minutes.\n")
  
  sum_unique_weights <- as.data.frame(t(coef(optimx_results)))
  names(sum_unique_weights)[1] <- "sum_weights"
  
  survey_summary <- cbind(unique_weights, sum_unique_weights)
  
  survey_summary <- survey_summary %>%
    mutate(record_weight = sum_weights / records) %>%
    select(-sum_weights, -records, -minimum_weights, -maximum_weights, -midpoint_weights)
  
  survey_df <- left_join(survey_df, survey_summary, by = unique_variables$survey_variable)
  
  return(survey_df)
}

