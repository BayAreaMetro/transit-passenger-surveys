# Impute_Continuous_Income_Value.R
# Use PUMS data to impute a continuous income value from categorical data
# Use the appropriate PUMS year for each survey (from the "survey_year" field)
# An exception is that we don't yet have 2023 PUMS data for the Snapshot Survey and ACE/Golden Gate
# Instead of 2023, use 2022 for those (but update once 2023 PUMS data is available)

###################################################################################
# Actions required on lines 24, 43
###################################################################################


# Load the relevant libraries
library(tidyverse)
library(rvest)

# Set seed for imputation
# Remove scientific notation from dataframes

set.seed(123)
options(scipen = 999)

# Import data (this part can be overwritten if part of a larger workflow and a dataframe is passed)

input_df <- read_csv("M:/Data/OnBoard/Data and Reports/_data_Standardized/standardized_2024-09-23/survey_combined.csv")

# Loop through each year and download relevant PUMS data

get_pums_years_f <- function(input_df, column_name) {
  # Ensure the column exists in the dataframe
  if (!column_name %in% names(input_df)) {
    stop(paste("The column", column_name, "does not exist in the dataframe."))
  }
  
  # Retrieve unique sorted years
  unique_years <- sort(unique(input_df[[column_name]]))
  
  return(unique_years)
}

unique_pums_years <- get_pums_years_f(input_df,"survey_year")

for (year in unique_pums_years) {
  year <- if_else(year=="2023","2022",as.character(year)) ############### - remove with availability of 2023 PUMS
  year2 <- str_sub(year, -2)  # Extract the last two digits of the year
  file_name <- paste0("M:/Data/Census/PUMS/PUMS ", year, "/hbayarea", year2, ".Rdata")
  
  if (file.exists(file_name)) {
    load(file_name)  # Load the .Rdata file (assumes it loads a data frame)
    
    # After loading, apply mutate and select to the loaded data frame
    # Assuming the data frame inside the .Rdata file is named 'hbayareaXX'
    
    df_name <- paste0("hbayarea", year2)  # Construct the data frame name
    
    # Check if the data frame exists in the environment
    if (exists(df_name)) {
      # Retrieve the data frame
      temp_df <- get(df_name)
      
      # Apply mutate and select actions
      temp_df <- temp_df %>%
        mutate(adjustment = ADJINC / 1000000,          
               income = HINCP * adjustment,
               pums_year=as.numeric(year),
               PUMA=as.character(PUMA)) %>%       
        select(PUMA, HINCP,adjustment,income, pums_year, WGTP)            
      
      # Optionally, you could save this modified data frame back to a new variable or overwrite
      assign(df_name, temp_df, envir = .GlobalEnv)  # Overwrite the data frame in the global environment
    } else {
      message(paste("Data frame not found in:", file_name))
    }
    
    message(paste("Loaded and processed data from:", file_name))
  } else {
    message(paste("File not found:", file_name))
  }
}

# Bind all the PUMS files together

pums_objects <- ls(pattern = "^hbayarea")

combined_pums <- pums_objects %>% 
  map(~get(.)) %>% 
  bind_rows()

# Function to split the income range into lower and upper bounds, handling "under" and "or higher" categories

split_income_range_f <- function(input_df) {
  if (!"household_income" %in% names(input_df)) {
    stop("The input dataframe must contain a column named 'household_income'.")
  }
  
  # Define a regular expression pattern to match the income formats
  pattern <- "^\\$(\\d{1,3}(?:,\\d{3})*) to \\$(\\d{1,3}(?:,\\d{3})*)$|^under \\$(\\d{1,3}(?:,\\d{3})*)$|^\\$(\\d{1,3}(?:,\\d{3})*) or higher$"
  
  # Use mutate and map to parse household_income
  results <- input_df %>%
    mutate(
      income_split = map(household_income, ~ {
        # Handle NA, "Missing", and "refused"
        if (is.na(.x) || .x %in% c("Missing", "refused")) {
          return(tibble(lower_bound = NA, upper_bound = NA))
        }
        
        # Match the income format with the regex
        matches <- str_match(.x, pattern)
        
        # Initialize lower and upper bounds
        lower_bound <- NA
        upper_bound <- NA
        
        # Process matches based on their positions in the regex groups
        # Subtract 1 from upper bound to remove overlap of lower/upper bound values
        
        if (!is.na(matches[1])) {
          if (!is.na(matches[2]) && !is.na(matches[3])) {
            # "$X,XXX to $Y,YYY" format
            lower_bound <- as.numeric(gsub(",", "", matches[2]))
            upper_bound <- as.numeric(gsub(",", "", matches[3]))-1
          } else if (!is.na(matches[4])) {
            # "under $X,XXX" format
            # Set lower bound to zero, though negative values do exist in the PUMS
            lower_bound <- 0
            upper_bound <- as.numeric(gsub(",", "", matches[4]))-1
          } else if (!is.na(matches[5])) {
            # "$X,XXX or higher" format
            lower_bound <- as.numeric(gsub(",", "", matches[5]))
            upper_bound <- Inf
          }
        }
        
        return(tibble(lower_bound, upper_bound))
      })
    ) %>%
    unnest(income_split)  # Unnest the results to create individual columns
  
  return(results)
}


# Call the function with your data
input_df <- split_income_range_f(input_df)

impute_cat_income_f <- function(lower_bound, upper_bound, survey_year) {
  # Determine the appropriate pums_year based on survey_year
  match_year <- ifelse(survey_year == 2023, 2022, survey_year)
  
  temp <- combined_pums %>% 
    filter(!is.na(income)) %>%  # Remove records with no income
    filter(income >= lower_bound & 
             income <= upper_bound & 
             pums_year == match_year)  # Filter by income bounds and the adjusted pums_year
  
  # Check if temp is empty after filtering
  if (nrow(temp) == 0) {
    return(NA)  # or any default value if no records are found
  }
  
  # Sample a value based on the income weights (WGTP)
  value <- sample(temp$income, replace = TRUE, size = 1, prob = temp$WGTP)
  
  return(value)
}

trial <- input_df %>%
  rowwise() %>%
  mutate(hh_income_nominal_continuous = ifelse(is.na(lower_bound) | is.na(upper_bound),
                                    NA,  
                                    impute_cat_income_f(lower_bound, upper_bound, survey_year))) %>%
  ungroup()

# Bring in CPI table from MTC modeling Wiki into a dataframe

# Define the URL
url <- "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"

# Read the webpage
page <- read_html(url)

# Extract the table with the 2010 CPI reference and rename the column; use only 2010 data and beyond
inflation_table <- page %>%
  html_node("table") %>%
  html_table() %>% 
  select(CPI_year=Year,CPI_2010_Ref="Consumer Price Index(2010 Reference)") %>% 
  filter(CPI_year>=2010)

# Get 2023 value for CPI

CPI_2023_placeholder <- inflation_table %>%
  filter(CPI_year==2023) %>%
  .$CPI_2010_Ref

# Add CPI multiplier for each year and calculate 2023 inflated income values

input_df <- trial %>% 
  left_join(., inflation_table, by = c("survey_year" = "CPI_year")) %>% 
  mutate(CPI_2023 = if_else(is.na(lower_bound) | is.na(upper_bound), NA_real_, CPI_2023_placeholder),
         CPI_ratio = if_else(is.na(lower_bound) | is.na(upper_bound), NA_real_, CPI_2023/CPI_2010_Ref),
         hh_income_2023_continuous = if_else(is.na(lower_bound) | is.na(upper_bound), NA_real_, hh_income_nominal_continuous*CPI_ratio)
  ) %>% 
  relocate(household_income,.before = hh_income_2023_continuous) %>% 
  relocate(hh_income_nominal_continuous,.before = hh_income_2023_continuous)%>% 
  select(-lower_bound,-upper_bound,-hh_income_nominal_continuous,-CPI_2010_Ref,-CPI_2023)



