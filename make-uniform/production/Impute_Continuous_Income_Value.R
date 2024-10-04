# Load the relevant libraries
library(tidyverse)
library(rvest)

# Set seed for imputation

set.seed(123)

# Import data (this part can be overwritten if part of a larger workflow)

standardized_2024_09_23 <- read_csv("M:/Data/OnBoard/Data and Reports/_data_Standardized/standardized_2024-09-23/survey_combined.csv")

# Loop through each year and download relevant PUMS data

get_pums_years <- function(data, column_name) {
  # Ensure the column exists in the dataframe
  if (!column_name %in% names(data)) {
    stop(paste("The column", column_name, "does not exist in the dataframe."))
  }
  
  # Retrieve unique sorted years
  unique_years <- sort(unique(data[[column_name]]))
  
  return(unique_years)
}

years <- get_pums_years(standardized_2024_09_23,"survey_year")

for (year in years) {
  year2 <- str_sub(year, -2)  # Extract the last two digits of the year
  file_name <- paste0("M:/Data/Census/PUMS/PUMS ", year, "/hbayarea", year2, ".Rdata")
  
  if (file.exists(file_name)) {
    load(file_name)  # Load the .Rdata file 
    
    message(paste("Loaded data from:", file_name))
  } else {
    message(paste("File not found:", file_name))
  }
}

# Function to split the income range into lower and upper bounds, handling "under" and "or higher" categories
# Function to split the income range into lower and upper bounds

split_income_range <- function(input_df) {
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
result <- split_income_range(standardized_2024_09_23)

# Bring in CPI table from MTC modeling Wiki into a dataframe

# Define the URL
url <- "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"

# Read the webpage
page <- read_html(url)

# Extract the table with the 2010 CPI reference and rename the column; use only 2010 data and beyond
inflation_table <- page %>%
  html_node("table") %>%
  html_table() %>% 
  select(Year,CPI_2010_Ref="Consumer Price Index(2010 Reference)") %>% 
  filter(Year>=2010)


# View the result
print(data)


