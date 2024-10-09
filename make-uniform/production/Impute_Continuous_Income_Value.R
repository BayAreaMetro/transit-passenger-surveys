USAGE = "
 Impute_Continuous_Income_Value.R
 Use PUMS data to impute a continuous income value from categorical data
 Use the appropriate PUMS year for each survey (from the 'survey_year' field)
 An exception is that we don't yet have 2023 PUMS data for surveys performed in 2023 
 (the Regional Snapshot Survey and ACE/Golden Gate)

 Instead of 2023, use 2022 for those (but update once 2023 PUMS data is available)
"

library(tidyverse)
library(rvest) # For web scraping CPI data


# Function to split income range from categorical data
# Given an input dataframe with column, household_income, containing strings such as '$25,000 to $35,000'
# This function will add two columns to the dataframe, income_lower_bound and income_upper_bound.
# For example:
# household_income     income_lower_bound income_upper_bound
# <chr>                             <dbl>              <dbl>
# $25,000 to $35,000                25000              34999
# $75,000 to $100,000               75000              99999
# $50,000 to $75,000                50000              74999
# under $10,000                         0               9999
# $35,000 to $50,000                35000              49999
# $10,000 to $25,000                10000              24999
# $150,000 or higher               150000                Inf
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
          return(tibble(income_lower_bound = NA, income_upper_bound = NA))
        }
        
        # Match the income format with the regex, initialize lower and upper bounds
        
        matches <- str_match(.x, pattern)
        income_lower_bound <- income_upper_bound <- NA  
        
        # Process matches based on their positions in the regex groups
        # Subtract 1 from upper bound to remove overlap of lower/upper bound values
        
        if (!is.na(matches[1])) {
          if (!is.na(matches[2]) && !is.na(matches[3])) {
            # "$X,XXX to $Y,YYY" format
            income_lower_bound <- as.numeric(gsub(",", "", matches[2]))
            income_upper_bound <- as.numeric(gsub(",", "", matches[3])) - 1
          } else if (!is.na(matches[4])) {
            # "under $X,XXX" format
            # Set lower bound to zero, though negative values do exist in the PUMS
            income_lower_bound <- 0
            income_upper_bound <- as.numeric(gsub(",", "", matches[4])) - 1
          } else if (!is.na(matches[5])) {
            # "$X,XXX or higher" format
            income_lower_bound <- as.numeric(gsub(",", "", matches[5]))
            income_upper_bound <- Inf
          }
        }
        return(tibble(income_lower_bound, income_upper_bound))
      })
    ) %>%
    unnest(income_split)
  return(results)
}

# This function requires that the given dataframe includes the columns:
# - survey_year (int), and
# - household_income (string), which is assumed to be in the survey_year dollars.
# The function returns the same dataframe with the following columns added:
# - income_lower_bound: parsed from household_income (see split_income_range_f)
# - income_upper_bound: parsed from household_income (see split_income_range_f)
# - hh_income_nominal_continuous: sampled from the PUMS data for the survey_year using the
#                                 income_lower_bound and income_upper_bound
# - hh_income_2023dollars_continuous: hh_income_nominal_continuous converted to 2023 dollars
impute_continuous_income_f <- function(input_df) {
  print("Running impute_continuous_income_f()")
  
  # Set seed for reproducibility and remove scientific notation
  
  set.seed(123)
  options(scipen = 999)

  
  # Get PUMS years needed (based on survey years in dataset)
  
  unique_pums_years <- sort(unique(input_df[["survey_year"]]))
  print(" unique_pums_years:")
  print(unique_pums_years)
  
  # # Load relevant PUMS data for each year, perform income calculations to adjust within-PUMS inflation values
  
  combined_pums <<- data.frame() # make this global scope
  for (year in unique_pums_years) {
    # TODO: Note that this should be updated after getting PUMS 2023 data
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

        combined_pums <<- bind_rows(combined_pums, temp_df) # note global scope
        print(paste("Added pums from",year,"; combined_pums has",nrow(combined_pums),"rows"))
      } else {
        message(paste("Data frame not found in:", file_name))
      }
    } else {
      message(paste("File not found:", file_name))
    }
  } 

  # Apply income range splitting function on unique values of household income
  household_income_df <- distinct(input_df, household_income)
  household_income_df <- split_income_range_f(household_income_df)
  print("household_income_df:")
  print(household_income_df, n=40)

  # merge these back
  input_df <- left_join(input_df, household_income_df, 
    by="household_income", relationship="many-to-one")
  print(paste("input_df has",nrow(input_df),"rows"))
  print(colnames(input_df))

  # set initial value
  input_df <- mutate(input_df, hh_income_nominal_continuous=NA_real_)

  # Segment by survey_year, income_lower_bound, income_upper_bound
  grouped_df <- input_df %>% group_by(survey_year, income_lower_bound, income_upper_bound)
  # Get the unique combinations of the grouping variables
  group_keys <- grouped_df %>% group_keys()
  # Split the grouped data into a list of data frames
  split_groups <- grouped_df %>% group_split()
  # Iterate over each group using group_keys
  for (i in seq_along(split_groups)) {
    group_data <- split_groups[[i]]
    group_info <- group_keys[i, ]
    print(paste("Group:", paste(group_info, collapse = ", "),"has",nrow(group_data),"rows"))
    # print(group_data)

    # TODO: Remove special 2023 handling
    match_year <- ifelse(group_info$survey_year == 2023, 2022, group_info$survey_year) # Handle 2023 case

    # no bounds -- leave as NA
    if (is.na(group_info$income_lower_bound) & is.na(group_info$income_upper_bound)) { next }

    # filter to relevant pums -- the correct year and with an income
    relevant_pums_df <- combined_pums %>%
      filter(!is.na(income)) %>%
      filter(pums_year == match_year)
    # filter based on income lower bound
    if (!is.na(group_info$income_lower_bound)) {
      relevant_pums_df <- relevant_pums_df %>%
        filter(income >= group_info$income_lower_bound)
    }
    # filter based on income upper bound
    if (!is.na(group_info$income_upper_bound)) {
      relevant_pums_df <- relevant_pums_df %>%
        filter(income <= group_info$income_upper_bound)
    }
    print(paste(" Relevant PUMS rows:",nrow(relevant_pums_df)))
    if (nrow(relevant_pums_df) == 0) { next }

    group_data <- mutate(group_data,
      hh_income_nominal_continuous = sample(relevant_pums_df$income, size=n(), replace=TRUE, prob=relevant_pums_df$WGTP)
    )
    # save the updated group data back to the list
    split_groups[[i]] <- group_data
  }
  # combine groups back together
  input_df <- bind_rows(split_groups)
  
  # Bring in CPI table from MTC modeling Wiki
  # Keep rows from 2010 and later and rename for local use
  url <- "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"
  page <- read_html(url)
  inflation_table <- page %>%
    html_node("table") %>%
    html_table() %>%
    select(CPI_year = Year, CPI_2010_Ref = "Consumer Price Index(2010 Reference)") %>%
    filter(CPI_year >= 2010)
  print("inflation_table:")
  print(inflation_table)
  
  # Keep placeholder CPI value for 2023
  
  CPI_2023_placeholder <- inflation_table %>%
    filter(CPI_year == 2023) %>%
    .$CPI_2010_Ref
  print(paste("CPI_2023_placeholder:",CPI_2023_placeholder))
  
  # Adjust income to 2023 values using CPI
  # Remove unnecessary variables and keep 2023 continuous income variable
  
  input_df <- input_df %>%
    left_join(inflation_table, by = c("survey_year" = "CPI_year")) %>%
    mutate(CPI_2023 = CPI_2023_placeholder,
           CPI_ratio = CPI_2023 / CPI_2010_Ref,
           hh_income_2023dollars_continuous = hh_income_nominal_continuous * CPI_ratio) %>%
    relocate(household_income, .before = hh_income_2023dollars_continuous) %>%
    relocate(hh_income_nominal_continuous, .before = hh_income_2023dollars_continuous) %>%
    select(-CPI_2010_Ref, -CPI_2023, -CPI_ratio)
  
  # Assign return the dataframe with additional columns: hh_income_nominal_continuous, hh_income_2023dollars_continuous
  return(input_df)
}


###################################################################################
# To test as a standalone, call this script with the input csv as an argument
# e.g. RScript --vanilla Impute_Continuous_Income_Value.R -i 
# "M:\Data\OnBoard\Data and Reports\_data_Standardized\standardized_2024-10-03\survey_combined.Rdata"
tryCatch({
  library(argparser) # For command-line testing

  argparser <- arg_parser(USAGE, hide.opts=TRUE)
  argparser <- add_argument(parser=argparser, arg="--input_file",  help="Input csv or rdata file")
  argv <- parse_args(argparser)
  
  if (!is.na(argv$input_file)) {
    print(paste("Reading input_file: ", argv$input_file))
    if (endsWith(argv$input_file,".csv")) {
      survey_combine <- read_csv(argv$input_file)
    }
    if (endsWith(argv$input_file,".Rdata")) {
      load(argv$input_file)
    }
    print(paste("Read", nrow(survey_combine),"rows; colnames:"))
    print(colnames(survey_combine))
    # run it
    print(paste("Starting impute_continuous_income_f at", Sys.time()))
    survey_combine_out_df <- impute_continuous_income_f(survey_combine)
    print(paste("Completed impute_continuous_income_f at", Sys.time()))
    
    print(colnames(survey_combine_out_df))

    # THIS IS JUST FOR TESTING -- don't save these around
    out_file <- str_replace(argv$input_file, ".Rdata", "_with_income_v2.Rdata")
    print(paste("Saving to", out_file))
    save(survey_combine_out_df, file = out_file)
  }
},
error = function(e) { 
  print(paste("ERROR:", e))
  NA
},
warning = function(w) {
  print(paste("WARNING:", w))
  NA
})