# Impute_Continuous_Income_Value.R
# Use PUMS data to impute a continuous income value from categorical data
# Use the appropriate PUMS year for each survey (from the "survey_year" field)
# An exception is that we don't yet have 2023 PUMS data for the Snapshot Survey and ACE/Golden Gate
# Instead of 2023, use 2022 for those (but update once 2023 PUMS data is available)

###################################################################################
# Can remove the below if incorporated as a function as part of the workflow
library(tidyverse)
library(rvest) # For web scraping CPI data
input_df <- read_csv("M:/Data/OnBoard/Data and Reports/_data_Standardized/standardized_2024-09-23/survey_combined.csv")
# Actions required on lines 40
###################################################################################

# Create the function to impute income

impute_continuous_income_f <- function(input_df) {
  
  # Set seed for reproducibility and remove scientific notation
  
  set.seed(123)
  options(scipen = 999)
  
  # Helper function to get unique PUMS years needed from survey dataframe
  
  get_pums_years_f <- function(input_df, column_name) {
    if (!column_name %in% names(input_df)) {
      stop(paste("The column", column_name, "does not exist in the dataframe."))
    }
    unique_years <- sort(unique(input_df[[column_name]]))
    return(unique_years)
  }
  
  # Get PUMS years 
  
  unique_pums_years <- get_pums_years_f(input_df, "survey_year")
  
  # Load relevant PUMS data for each year
  # Handle 2023 exception
  # Note that this should be updated after getting PUMS 2023 data
  
  for (year in unique_pums_years) {
    year <- if_else(year == "2023", "2022", as.character(year))  # Handle 2023 PUMS case
    year2 <- str_sub(year, -2)
    file_name <- paste0("M:/Data/Census/PUMS/PUMS ", year, "/hbayarea", year2, ".Rdata")
    
    if (file.exists(file_name)) {
      load(file_name)
      df_name <- paste0("hbayarea", year2)
      if (exists(df_name)) {
        temp_df <- get(df_name)
        temp_df <- temp_df %>%
          mutate(adjustment = ADJINC / 1000000,          
                 income = HINCP * adjustment,
                 pums_year = as.numeric(year),
                 PUMA = as.character(PUMA)) %>%
          select(PUMA, HINCP, adjustment, income, pums_year, WGTP)
        assign(paste0("use_",df_name), temp_df, envir = .GlobalEnv)
      } else {
        message(paste("Data frame not found in:", file_name))
      }
    } else {
      message(paste("File not found:", file_name))
    }
  }
  
  # Combine all loaded PUMS datasets from the global environment into a single dataframe
  
  pums_objects <- ls(pattern = "^use_hbayarea", envir = .GlobalEnv)  
  
  # Check if any objects were found
  
  if (length(pums_objects) == 0) {
    stop("No PUMS objects found that match the pattern '^use_hbayarea'.")
  }
  
  # Retrieve and combine data frames into a single one for later use in the imputation
  
  combined_pums <- pums_objects %>%
    map(~get(.x, envir = .GlobalEnv)) %>%  # Specify the global environment for get()
    bind_rows()
  
  # Function to split income range from categorical data
  
  split_income_range_f <- function(input_df) {
    if (!"household_income" %in% names(input_df)) {
      stop("The input dataframe must contain a column named 'household_income'.")
    }

    # Define a regular expression pattern to match the income formats
    
    pattern <- "^\\$(\\d{1,3}(?:,\\d{3})*) to \\$(\\d{1,3}(?:,\\d{3})*)$|^under \\$(\\d{1,3}(?:,\\d{3})*)$|^\\$(\\d{1,3}(?:,\\d{3})*) or higher$"
    
    # Use mutate and map to parse household_income bounds
    
    results <- input_df %>%
      mutate(
        income_split = map(household_income, ~ {
          if (is.na(.x) || .x %in% c("Missing", "refused")) {
            return(tibble(lower_bound = NA, upper_bound = NA))
          }
          
          # Match the income format with the regex, initialize lower and upper bounds
          
          matches <- str_match(.x, pattern)
          lower_bound <- upper_bound <- NA  
          
          # Process matches based on their positions in the regex groups
          # Subtract 1 from upper bound to remove overlap of lower/upper bound values
          
          if (!is.na(matches[1])) {
            if (!is.na(matches[2]) && !is.na(matches[3])) {
              # "$X,XXX to $Y,YYY" format
              lower_bound <- as.numeric(gsub(",", "", matches[2]))
              upper_bound <- as.numeric(gsub(",", "", matches[3])) - 1
            } else if (!is.na(matches[4])) {
              # "under $X,XXX" format
              # Set lower bound to zero, though negative values do exist in the PUMS
              lower_bound <- 0
              upper_bound <- as.numeric(gsub(",", "", matches[4])) - 1
            } else if (!is.na(matches[5])) {
              # "$X,XXX or higher" format
              lower_bound <- as.numeric(gsub(",", "", matches[5]))
              upper_bound <- Inf
            }
          }
          return(tibble(lower_bound, upper_bound))
        })
      ) %>%
      unnest(income_split)
    return(results)
  }
  
  # Apply income range splitting function
  
  input_df <- split_income_range_f(input_df)
  
  # Function to impute income based on bounds and survey year
  # Handle 2023 exception
  
  impute_cat_income_f <- function(lower_bound, upper_bound, survey_year) {
    match_year <- ifelse(survey_year == 2023, 2022, survey_year) # Handle 2023 case
    temp <- combined_pums %>%
      filter(!is.na(income)) %>%
      filter(income >= lower_bound & income <= upper_bound & pums_year == match_year)
    
    if (nrow(temp) == 0) return(NA)
    
    value <- sample(temp$income, size = 1, replace = TRUE, prob = temp$WGTP)
    return(value)
  }
  
  # Impute continuous income values
  
  input_df <- input_df %>%
    rowwise() %>%
    mutate(hh_income_nominal_continuous = ifelse(is.na(lower_bound) | is.na(upper_bound),
                                                 NA,
                                                 impute_cat_income_f(lower_bound, upper_bound, survey_year))) %>%
    ungroup()
  
  # Bring in CPI table from MTC modeling Wiki
  # Keep rows from 2010 and later and rename for local use
  
  url <- "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"
  page <- read_html(url)
  inflation_table <- page %>%
    html_node("table") %>%
    html_table() %>%
    select(CPI_year = Year, CPI_2010_Ref = "Consumer Price Index(2010 Reference)") %>%
    filter(CPI_year >= 2010)
  
  # Keep placeholder CPI value for 2023
  
  CPI_2023_placeholder <- inflation_table %>%
    filter(CPI_year == 2023) %>%
    .$CPI_2010_Ref
  
  # Adjust income to 2023 values using CPI
  # Remove unnecessary variables and keep 2023 continuous income variable
  
  input_df <- input_df %>%
    left_join(inflation_table, by = c("survey_year" = "CPI_year")) %>%
    mutate(CPI_2023 = if_else(is.na(lower_bound) | is.na(upper_bound), NA_real_, CPI_2023_placeholder),
           CPI_ratio = if_else(is.na(lower_bound) | is.na(upper_bound), NA_real_, CPI_2023 / CPI_2010_Ref),
           hh_income_2023_continuous = if_else(is.na(lower_bound) | is.na(upper_bound),
                                               NA_real_,
                                               hh_income_nominal_continuous * CPI_ratio)) %>%
    relocate(household_income, .before = hh_income_2023_continuous) %>%
    relocate(hh_income_nominal_continuous, .before = hh_income_2023_continuous) %>%
    select(-lower_bound, -upper_bound, -hh_income_nominal_continuous, -CPI_2010_Ref, -CPI_2023, -CPI_ratio)
  
  # Assign the modified input_df back to the global environment and return it
  
  assign("input_df", input_df, envir = .GlobalEnv)
  return(input_df)
  
  # Clear up the workspace by removing PUMS data
  rm(list = ls(pattern = "^use_hbayarea"))
}


