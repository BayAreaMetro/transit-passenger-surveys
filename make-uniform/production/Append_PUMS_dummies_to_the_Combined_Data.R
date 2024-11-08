# -------------------------------------------------------------------------
# This script downloads PUMS data to provide context for Transit Passenger Survey data analysis

# It adds labels to the data (as this step seems to be easer in R than in Tableau)
# It exports the data to csv, which can be used for debugging fo rread by Tableau for further visualization
# This is a person table with household income
# -------------------------------------------------------------------------


# Load libraries
library(tidycensus)
library(tidyverse)


# --------------------------------------------------------------------------------
# Specify the path to the survey_combined.Rdata file
# --------------------------------------------------------------------------------
standardized_data_path <- "M:/Data/OnBoard/Data and Reports/_data_Standardized/"
standardized_date      <- "standardized_2024-11-06"
full_path_to_combined_Rdata <- file.path(standardized_data_path, standardized_date, "survey_combined.Rdata")

# --------------------------------------------------------------------------------
# Read survey_combined.Rdata
# this is the output of Build_Standard_Database.R
# full path: https://github.com/BayAreaMetro/transit-passenger-surveys/blob/master/make-uniform/production/Build_Standard_Database.R
# --------------------------------------------------------------------------------
# Load the .Rdata file
load(full_path_to_combined_Rdata)

# save the data frame with a new name to prepare for "bind_rows" later
survey_combined_plusPUMS_df <- survey_combine

# --------------------------------------------------------------------------------
# Download PUMS data
# --------------------------------------------------------------------------------

# could replace this with a local version of 2023 PUMS on M
# but using the API seems easy enough?

# Get PUMAs for the Bay Area
# want to do this for 2019acs5 and 2023acs1
vintages <- c("2019acs5", "2023acs1")

# Loop over each vintage
for (vintage in vintages) {

    # Extract year and type from the vintage string
    survey_year <- as.numeric(substr(vintage, 1, 4))
    acs1_or_acs5 <- substr(vintage, 5, nchar(vintage))

    # Set all_BayAreaPumas based on the survey year
    if (survey_year == 2023) {

        # PUMAs for the Bay Area 2022 onwards
        puma_lists_2022onwards <- list(
            alameda       <- c("00101","00111","00112","00113","00114","00115","00116","00117","00118","00119","00120","00121","00122","00123"),
            contra_costa  <- c("01301","01305","01308","01309","01310","01311","01312","01313","01314"),
            marin         <- c("04103","04104"),
            napa          <- c("05500"),
            san_francisco <- c("07507","07508","07509","07510","07511","07512","07513","07514"),
            san_mateo     <- c("08101","08102","08103","08104","08105","08106"),
            santa_clara   <- c("08505","08506","08507","08508","08510","08511","08512","08515","08516","08517","08518","08519","08520","08521","08522"),
            solano        <- c("09501", "09502", "09503"),
            sonoma        <- c("09702","09704","09705","09706")
        )

        # Combine all PUMAs into one list for get_pums
        all_BayAreaPumas <- unlist(puma_lists_2022onwards)

    } else if (survey_year == 2019) {
        # PUMAs for the Bay Area pre-2022
        puma_lists_pre2022 <- list(
            alameda       <- c("00101", "00102", "00103", "00104", "00105", "00106", "00107", "00108", "00109", "00110"),
            contra_costa  <- c("01301", "01302", "01303", "01304", "01305", "01306", "01307", "01308", "01309"),
            marin         <- c("04101","04102"),
            napa          <- c("05500"),
            san_francisco <- c("07501", "07502", "07503", "07504", "07505", "07506", "07507"),
            san_mateo     <- c("08101","08102", "08103", "08104", "08105", "08106"),
            santa_clara   <- c("08501", "08502", "08503", "08504", "08505","08506", "08507", "08508", "08509", "08510", "08511", "08512", "08513", "08514"),
            solano        <- c("09501", "09502", "09503"),
            sonoma        <- c("09701", "09702", "09703")
        )

        # Combine all PUMAs into one list for get_pums
        all_BayAreaPumas <- unlist(puma_lists_pre2022)

    }


    # Download PUMS data
    pums_df <- get_pums(
      state = "CA",
      puma = all_BayAreaPumas,
      year = survey_year,
      survey = acs1_or_acs5,
      variables = c(
         "HINCP",    # Household income
         "ADJINC",   # Adjustment factor for income and earnings dollar amounts (so it's representative of a standard month)
         "VEH",      # Vehicle owership
         "RAC1P",    # Race
         "HISP"      # Ethnicity
      )
    )

# --------------------------------------------------------------------------------
# Add income labels
#
# 
# Notes on inflation adjustment:
#
# if it's 2023acs1, simple inflation adjustment is needed to bring the values to a standard month in 2023
# if it's 2019acs5, addition inflation adjustment is needed to bring the values to 2023 dollars
#
# as noted in P. 17 of
# https://www2.census.gov/programs-surveys/acs/tech_docs/pums/ACS2015_2019_PUMS_README.pdf
# Note on Income and Earnings Inflation Factor (ADJINC)
# "Divide ADJINC by 1,000,000 to obtain the inflation adjustment factor and multiply it to
# the PUMS variable value to adjust it to 2019 dollars."
#
# this means we need to convert from 2019 to 2023 dollars
# from: https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions
# the Consumer Price Index (2000 Reference) is 1.88 in 2023 and 1.64 in 2019
# --------------------------------------------------------------------------------

# check data type
typeof(pums_df$HINCP)
typeof(pums_df$ADJINC)
typeof(pums_df$VEH)
typeof(pums_df$RAC1P)


pums_df <- pums_df %>%
  # Convert HINCP and ADJINC to numeric; non-numeric values will become NA
  mutate(
    HINCP_numeric = as.numeric(HINCP),
    ADJINC_numeric = as.numeric(ADJINC)
  )
 

# calculate the additional adjustment to bring the values to 2023, for use in the next step
    if (survey_year == 2023) {
       inflate_to_2023 = 1
    } else if (survey_year == 2019) {
       inflate_to_2023 = 1.88/1.64
    }


pums_df <- pums_df %>%
 mutate(income= HINCP_numeric*ADJINC_numeric*inflate_to_2023) 

  # Add labels
# todo: update to snapshot categories
pums_df <- pums_df %>%
  mutate( 
    Income_Label = case_when(
    income < 50000                       ~ "Under $50,000",
    income >= 50000 & income  < 100000   ~ "$50,000-$99,999",
    income >= 100000 & income < 150000   ~ "$100,000-$149,999",
    income >= 150000                     ~ "$150,000 or more"
    ),
  )

# Convert HISP and RACIP to numeric
pums_df <- pums_df %>%
  mutate(
    HISP = as.numeric(HISP),
    RAC1P = as.numeric(RAC1P)
  )

pums_df <- pums_df %>%
  mutate( 
    Ethnicity_Label = case_when(
    HISP==1                              ~"NOT HISPANIC/LATINO OR OF SPANISH ORIGIN",
    HISP>1                               ~"HISPANIC/LATINO OR OF SPANISH ORIGIN",
    TRUE                                 ~"Missing"
    ),
  )


pums_df <- pums_df %>%
  mutate( 
    Race_Label = case_when(
    RAC1P==1                             ~"WHITE",
    RAC1P==2                             ~"BLACK",
    RAC1P==3                             ~"OTHER",
    RAC1P==4                             ~"OTHER",
    RAC1P==5                             ~"OTHER",
    RAC1P==6                             ~"ASIAN",
    RAC1P==7                             ~"OTHER",
    RAC1P==8                             ~"OTHER",
    RAC1P==9                             ~"OTHER",
    TRUE                                 ~"Missing"
    ),
  )

# Add Home_County using functions
# Function to label home county based on PUMA
label_home_county <- function(PUMA) {
  case_when(
    PUMA %in% alameda       ~ "Alameda",
    PUMA %in% contra_costa  ~ "Contra Costa",
    PUMA %in% marin         ~ "Marin",
    PUMA %in% napa          ~ "Napa",
    PUMA %in% san_francisco ~ "San Francisco",
    PUMA %in% san_mateo     ~ "San Mateo",
    PUMA %in% santa_clara   ~ "Santa Clara",
    PUMA %in% solano        ~ "Solano",
    PUMA %in% sonoma        ~ "Sonoma",
    TRUE                    ~ "Should not be included in this extraction"
  )
}
pums_df <- pums_df %>%
  mutate(
    Home_County = label_home_county(PUMA)
  )


# done labeling



# --------------------------------------------------------------------------------
# export the data to csv in case debugging is needed
# --------------------------------------------------------------------------------
# Create the full path for the new subdirectory
new_sub_directory_path <- file.path(standardized_data_path, standardized_date, "pums_data_for_context")

# Create the subdirectory
if (!dir.exists(new_sub_directory_path)) {
    dir.create(new_sub_directory_path)
    print(paste("Created subdirectory:", new_sub_directory_path))
} else {
    print(paste("Subdirectory already exists:", new_sub_directory_path))
}

temp_output_file <- file.path(new_sub_directory_path, paste0("persons_SelectedVars_", vintage, ".csv"))
write.csv(pums_df, file = temp_output_file, row.names = FALSE)

# --------------------------------------------------------------------------------
# Some tabulations...
# --------------------------------------------------------------------------------

# get income distribution by county (for quick verification of total persons e.g. against B01003 on data.census.gov)
Table_IncDistrByCounty <- pums_df %>%
  group_by(Home_County, Income_Label) %>%  
  summarise(weight_sum = sum(PWGTP, na.rm = TRUE))

print(Table_IncDistrByCounty, n = Inf)

# get income distribution for the region
Table_IncDistr_Region <- pums_df %>%
  group_by(Income_Label) %>%  
  summarise(weight_sum = sum(PWGTP, na.rm = TRUE))
print(Table_IncDistr_Region, n = Inf)

# tabulate vehicle ownership
Table_VehOwn <- pums_df %>%
  group_by(VEH) %>%  
  summarise(weight_sum = sum(PWGTP, na.rm = TRUE))

print(Table_VehOwn, n = Inf)

# tabulate race
Table_Race <- pums_df %>%
  group_by(Race_Label) %>%  
  summarise(weight_sum = sum(PWGTP, na.rm = TRUE))

print(Table_Race, n = Inf)

# tabulate ethnicity
Table_Ethnicity <- pums_df %>%
  group_by(Ethnicity_Label) %>%  
  summarise(weight_sum = sum(PWGTP, na.rm = TRUE))

print(Table_Ethnicity, n = Inf)

# --------------------------------------------------------------------------------
# output Table_IncDistr_Region and rename columns as needed
# --------------------------------------------------------------------------------

# Income
# Rename the column to "Weight"
DummyRows_hhinc_df <- Table_IncDistr_Region %>%
  rename(weight           = weight_sum,
         household_income = Income_Label)

# add a column canonical_operator to the df DummyRows_hhinc_df
# each row should be filled in with the name of the ACS vintage
DummyRows_hhinc_df$canonical_operator <- vintage

# Specify the output file path for the .Rdata file
DummyRows_hhinc_rdata <- file.path(standardized_data_path, standardized_date, "pums_data_for_context",
                                    paste0("Table_IncDistr_Region_", vintage, ".Rdata"))

# Save the data frame as .Rdata
save(DummyRows_hhinc_df, file = DummyRows_hhinc_rdata)

# Print a message to confirm saving
print(paste("Saved DummyRows_hhinc_df to", DummyRows_hhinc_rdata))


# Race
# Rename the column to "Weight"
DummyRows_race_df <- Table_Race %>%
  rename(weight           = weight_sum,
         race             = Race_Label)

# add a column canonical_operator to the df DummyRows_race_df
# each row should be filled in with the name of the ACS vintage
DummyRows_race_df$canonical_operator <- vintage

# Specify the output file path for the .Rdata file
DummyRows_race_rdata <- file.path(standardized_data_path, standardized_date, "pums_data_for_context",
                                    paste0("Table_Race", vintage, ".Rdata"))

# Save the data frame as .Rdata
save(DummyRows_race_df, file = DummyRows_race_rdata)

# Print a message to confirm saving
print(paste("Saved DummyRows_race_df to", DummyRows_race_rdata))


# Ethnicity
# Rename the column to "Weight"
DummyRows_ethnicity_df <- Table_Ethnicity %>%
  rename(weight           = weight_sum,
         hispanic         = Ethnicity_Label)

# add a column canonical_operator to the df DummyRows_ethnicity_df
# each row should be filled in with the name of the ACS vintage
DummyRows_ethnicity_df$canonical_operator <- vintage

# Specify the output file path for the .Rdata file
DummyRows_ethnicity_rdata <- file.path(standardized_data_path, standardized_date, "pums_data_for_context",
                                    paste0("Table_Ethnicity", vintage, ".Rdata"))

# Save the data frame as .Rdata
save(DummyRows_ethnicity_df, file = DummyRows_ethnicity_rdata)

# Print a message to confirm saving
print(paste("Saved DummyRows_ethnicity_df to", DummyRows_ethnicity_rdata))


# --------------------------------------------------------------------------------
# append pums data to survey_combined.Rdata
# --------------------------------------------------------------------------------

# Bind rows using dplyr's bind_rows, which will fill missing columns with NA
survey_combined_plusPUMS_df <- bind_rows(survey_combined_plusPUMS_df, DummyRows_hhinc_df)
survey_combined_plusPUMS_df <- bind_rows(survey_combined_plusPUMS_df, DummyRows_race_df)
survey_combined_plusPUMS_df <- bind_rows(survey_combined_plusPUMS_df, DummyRows_ethnicity_df)

survey_combined_plusPUMS_filepath <- file.path(new_sub_directory_path, "survey_combined.Rdata")
save(survey_combined_plusPUMS_df, file = survey_combined_plusPUMS_filepath)

# Print a message to confirm that data is appended
print(paste("Appended dummy rows to survey_combined.Rdata from", vintage))

}  # Closing brace for the `for` loop


