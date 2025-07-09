#
# Summarizes the 2023 Regional Snapshot Transit Passenger Survey for the dashboard,
# including unweighted and weighted responses, and measurers of error.
#
# This script does the following:
# 1. Standardized survey data:
#    a. Reads the most recent standardized survey data (this includes ACS data)
#    b. Filters to survey_year==2023)
# 2. Snapshot-specific survey data
#    a. Reads the snapshot-specific suvey data
# For both datasets, it
# 3. Recodes some variables for summaries
# 4. Summarizes survey data using srvyr package,
#    by survey_tech_group or by operator
#    for a given variable (income, home county, etc)
# 5. Output is written to  TPS_SURVEY_STANDARDIZED_PATH\summarize_snapshot_2023_for_dashboard.Rdata
#    plus a log is save to TPS_SURVEY_STANDARDIZED_PATH\summarize_snapshot_2023_for_dashboard.log
#
library(glue)
library(readxl)
library(rlang)
library(tidyverse)
library(srvyr)

# Handle lonely PSUs by removing them from variance estimation 
# This is needed for the analysis of Q8; otherwise, we'll get the error message: Stratum (Rio Vista Delta Breeze) has only one PSU
options(survey.lonely.psu = "remove")

# Current standardized survey data
TPS_SURVEY_STANDARDIZED_PATH <- "M:\\Data\\OnBoard\\Data and Reports\\_data_Standardized\\standardized_2025-07-02"
# Snapshot Survey file (for special snapshot survey questions)
TPS_SNAPSHOT_FILE <- "M:\\Data\\OnBoard\\Data and Reports\\Snapshot Survey\\mtc snapshot survey_final data file_recoded Dumbarton mode_052725.xlsx"

# Function to summarize survey data by attribute
summarize_for_attr <- function(survey_data, summary_col) {
  summary_col_str <- as_label(enquo(summary_col))
  print(glue("===== Summarizing for {summary_col_str}"))

  return_table <- tibble()

  # first summarize ACS (if it exists)
  ACS_SOURCE <- "2023 pums1"
  acs <- filter(survey_data, (source==ACS_SOURCE) & (!is.na({{ summary_col }})))
  if (nrow(acs) > 0) {
    dplyr::count(acs, {{ summary_col }}, .drop=FALSE) %>% print(n=Inf)

    acs_sumary <- acs %>% group_by({{ summary_col }}) %>% summarize(
      weighted_count = sum(weight, na.rm=TRUE),
      .groups = 'drop'
    )
    total_weighted <- sum(acs_sumary$weighted_count, na.rm=TRUE)
    acs_sumary <- acs_sumary %>% mutate(
      total_weighted = total_weighted,
      weighted_share = weighted_count / total_weighted,
      source = ACS_SOURCE,
    )

    # columns: summary_col, weighted, weighted_share
    return_table <- acs_sumary
  }

  for (filter_weekpart in c("WEEKDAY", "WEEKEND")) {

    # summarize by weekpart, tech group and operator
    for (summary_level in c("all_records", "survey_tech_group", "operator")) {

      print(glue("Summarizing survey data for weekpart={filter_weekpart}, summary_level={summary_level}, summary_col={summary_col_str}"))

      # filter to rows where data exists
      data_to_summarize <- filter(survey_data %>% filter(source == "survey") %>% mutate(all_records=1),
        !is.na(!!summary_level) &
        !is.na({{ summary_col }}) &
        !is.na(weight) &
        !is.na(operator) &  # Need operator for stratification
        !is.na(weekpart) &  # Need weekpart for stratification
        (weight > 0) &      # Exclude dummy records with zero weight
        (weekpart == filter_weekpart)
      )

      # Calculate actual unweighted counts by group and category
      actual_counts <- data_to_summarize %>%
        group_by(across(all_of(c(summary_level, summary_col_str)))) %>%
        summarise(weighted_count_actual = sum(weight), unweighted_count_actual = n(), .groups = "drop")

      # Step 1: Create dummy variables for each level of summary_col
      df_dummy <- data_to_summarize %>%
        mutate(across(all_of(summary_col_str), as.factor)) %>%
        mutate(dummy = 1) %>%
        pivot_wider(
          names_from = all_of(summary_col_str),
          values_from = dummy,
          values_fill = 0,
          names_prefix = "pref_"
        )
        # this adds dummy cols for the summary_col, e.g. pref_Alameda, pref_San Francisco, etc. which are set to 1 or 0
      print("df_dummy:")
      print(df_dummy)

      # Step 2: Create survey design with stratification by operator and weekpart
      srv_design <- df_dummy %>%
        as_survey_design(ids = unique_ID, weights = weight, strata = c(operator, time_period))

      # Step 3: Compute within-group weighted_shares and counts
      srv_results <- srv_design %>%
        group_by(across(all_of(summary_level))) %>%
        summarise(
          across(
            starts_with("pref_"),
            ~ survey_mean(.x, vartype = c("se", "ci", "cv")),
            .names = "{.col}_{.fn}"
          ),
          # Add total counts for each group
          total_weighted = survey_total(vartype = "se"),
          total_unweighted = unweighted(n()),
          .groups = "drop"
        )

      print("1 srv_results:")
      print(srv_results)

      # Reshape to get county as column with weighted_share, SE, and CI as separate columns
      srv_results <- srv_results %>%
        pivot_longer(
          cols = starts_with("pref_"),
          names_pattern = "^pref_(.*)(_1|_1_se|_1_low|_1_upp|_1_cv)$",
          names_to = c(summary_col_str, "stat_type"),
          values_to = "value"
        ) %>%
        mutate(
          stat_type = case_when(
            stat_type == "_1" ~ "weighted_share",
            stat_type == "_1_se" ~ "se",
            stat_type == "_1_low" ~ "ci_lower_95",
            stat_type == "_1_upp" ~ "ci_upper_95",
            stat_type == "_1_cv"  ~ "coeff_of_var"
          ))
      print("2 srv_results:")
      print(srv_results)

      # Pivot the stat_type back to columns
      srv_results <- srv_results %>%
        pivot_wider(
          names_from = stat_type,
          values_from = value
        )
      print("3 srv_results:")
      print(srv_results)

      srv_results <- srv_results %>%
        # Calculate weighted counts and merge actual unweighted counts
        mutate(
          ci_95 = ci_upper_95 - weighted_share,
          weekpart = filter_weekpart,
          summary_level = summary_level,
          summary_col = summary_col_str,
          source = "survey" # add this back
        ) %>%
        # Join with actual unweighted counts
        left_join(actual_counts,
                  by = setNames(c(summary_level, summary_col_str),
                               c(summary_level, summary_col_str))) %>%
        rename(weighted_count = weighted_count_actual, unweighted_count = unweighted_count_actual)
      print("4 srv_results:")
      print(srv_results)

      # References for estimate reliability criteria:
      # - **U.S. Census Bureau**: Quality Standards Metrics Definitions
      #   https://www.census.gov/programs-surveys/acs/methodology/sample-size-and-data-quality/quality-standards-metrics-definitions.html
      # - **Bureau of Labor Statistics**: Handbook of Methods - Survey Design
      #   https://www.bls.gov/opub/hom/ors/design.htm
      # - **Federal Committee on Statistical Methodology**: Data Quality Framework (CV > 30% threshold)
      #   https://nces.ed.gov/fcsm/pdf/FCSM.20.04_A_Framework_for_Data_Quality.pdf
      srv_results <- srv_results %>%
        # Apply criteria for poor estimate reliability
        mutate(
          # Calculate poor estimate reliability flags
          cv_flag = coeff_of_var > 0.30,  # CV > 30%
          sample_size_flag = unweighted_count < 30,  # Minimum sample size
          ci_width_flag = (ci_upper_95 - ci_lower_95) > 0.40,  # CI width > 40pp
          extreme_values_flag = ci_lower_95 < 0 | ci_upper_95 > 1,  # Impossible values

          # Overall poor reliability decision
          suppress = cv_flag | sample_size_flag | ci_width_flag | extreme_values_flag,

          # Create consolidated estimate reliability flag
          estimate_reliability = case_when(
            cv_flag ~ "Poor (High CV >30%)",
            sample_size_flag ~ "Poor (Small sample n<30)",
            ci_width_flag ~ "Poor (Wide CI >40pp)",
            extreme_values_flag ~ "Poor (Invalid range)",
            TRUE ~ "Acceptable"
          ),
        ) %>%
        # Keep relevant columns
        select(all_of(summary_level), all_of(summary_col_str),
               weighted_share, se, ci_95, ci_lower_95, ci_upper_95, coeff_of_var,
               weighted_count, unweighted_count, total_weighted, total_unweighted,
               estimate_reliability,
               weekpart, summary_level, summary_col, source)

      # for summary_level==operator, for each operator, set survey_tech_group to the survey_tech_groups for that operator
      if (summary_level=="operator") {
        operator_modes <- data_to_summarize %>%
          select(operator, survey_tech_group) %>%
          group_by(operator) %>%
          summarise(unique_modes = toString(unique(survey_tech_group)))
        print("operator_modes")
        print(operator_modes)
        # replace survey_tech_group with unique_modes
        srv_results <- left_join(srv_results, operator_modes, by=join_by(operator)) %>%
          mutate(survey_tech_group=unique_modes) %>%
          select(-unique_modes)
      }

      # we'll display this as All Transit Modes
      if (summary_level=="all_records") {
        srv_results <- srv_results %>%
          select(-all_records) %>%
          mutate(
            survey_tech_group="All Transit Modes",
            summary_level="survey_tech_group"
          )
      }

      print("srv_results after reshaping:")
      print(srv_results, n=30)

      # add to return table
      return_table <- bind_rows(return_table, srv_results)
    }
  }

  print("RETURN table:")
  print(return_table, n=30)
  print(glue("===== End of summarizing for {as_label(enquo(summary_col))}"))
  return(return_table)
}

# This function summarizes the standard survey (e.g. survey_combined.Rdata), returning a summary tibble
# with columns:
# -- Metadata --
#   source
#   weekpart
#   operator
#   survey_tech_group
# -- Summary categories --
#   summary_level
#   summary_col
#   household_income_group
#   home_county
#   race_ethnicity
#   trip_purpose_group
# -- Statistics/results --
#   weighted_count
#   total_weighted
#   weighted_share
#   unweighted_count
#   total_unweighted
#   se
#   ci_95
#   ci_lower_95
#   ci_upper_95
#   coeff_of_var
#   estimate_reliability
summarize_standardized_survey <- function() {
  # load the standardized survey data
  source_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "survey_combined.Rdata")
  loaded <- load(source_file)
  print(glue("summarize_standardized_survey(): Loaded {nrow(survey_combine)} rows from {source_file}"))

  # select only 2023 data
  survey_combine <- filter(survey_combine, survey_year == 2023)
  print(glue("Filtered to {nrow(survey_combine)} rows for survey_year==2023"))

  # create Survey Tech (group)
  survey_combine <- survey_combine %>% mutate(
    survey_tech_group = case_when(
      survey_tech %in% c("commuter rail","heavy rail","light rail") ~ "Rail",
      survey_tech %in% c("express bus") ~ "Express Bus",
      survey_tech %in% c("local bus") ~ "Local Bus",
      survey_tech %in% c("ferry") ~ "Ferry",
      TRUE ~ survey_tech
    ))

  # create operator from Canonical Operator without all the odd capitalization
  survey_combine <- survey_combine %>% mutate(
    operator = case_when(
      canonical_operator == "AC TRANSIT"           ~ "AC Transit",
      canonical_operator == "CAPITOL CORRIDOR"     ~ "Capitol Corridor",
      canonical_operator == "COUNTY CONNECTION"    ~ "County Connection",
      canonical_operator == "DUMBARTON"            ~ "Dumbarton Express",
      canonical_operator == "GOLDEN GATE TRANSIT"  ~ "Golden Gate Transit",
      canonical_operator == "MARIN TRANSIT"        ~ "Marin Transit",
      canonical_operator == "NAPA VINE"            ~ "Napa Vine",
      canonical_operator == "PETALUMA TRANSIT"     ~ "Petaluma Transit",
      canonical_operator == "RIO-VISTA"            ~ "Rio Vista Delta Breeze",
      canonical_operator == "SAMTRANS"             ~ "SamTrans",
      canonical_operator == "SF BAY FERRY"         ~ "SF Bay Ferry",
      canonical_operator == "MUNI"                 ~ "SFMTA (Muni)",
      canonical_operator == "SOLTRANS"             ~ "SolTrans",
      canonical_operator == "TRI-DELTA"            ~ "Tri Delta",
      canonical_operator == "UNION CITY"           ~ "Union City Transit",
      canonical_operator == "VACAVILLE CITY COACH" ~ "Vacaville City Coach",
      canonical_operator == "WESTCAT"              ~ "WestCAT",
      TRUE ~ canonical_operator
    ))

  # create household income (group)
  survey_combine <- survey_combine %>% mutate(
    household_income_group = case_when(
      household_income %in% c("under $50,000",
                  "under $15,000", "$15,000 to $25,000", "$25,000 to $35,000", "$35,000 to $50,000",
                  "under $10,000", "$10,000 to $25,000",
                  "$15,000 to $30,000", "$30,000 to $40,000", "$40,000 to $50,000") ~ "< $50,000",
      household_income %in% c("$50,000 to $75,000", "$75,000 to $100,000", "$50,000 to $99,999",
                  "$50,000 to $60,000", "$60,000 to $70,000", "$70,000 to $80,000", "$80,000 to $100,000") ~ "$50,000 - $99,999",
      household_income %in% c("$100,000 to $150,000", "$100,000 to $149,999") ~ "$100,000 - $149,999",
      household_income %in% c("$150,000 to $200,000") ~ "$150,000 - $199,999",
      household_income %in% c("$200,000 or higher") ~ "$200,000+",
      TRUE ~ household_income
    ))

  # create home_county
  survey_combine <- survey_combine %>% mutate(
    home_county = case_when(
      home_county_GEOID == "06001" ~ "Alameda",
      home_county_GEOID == "06013" ~ "Contra Costa",
      home_county_GEOID == "06041" ~ "Marin",
      home_county_GEOID == "06055" ~ "Napa",
      home_county_GEOID == "06075" ~ "San Francisco",
      home_county_GEOID == "06081" ~ "San Mateo",
      home_county_GEOID == "06085" ~ "Santa Clara",
      home_county_GEOID == "06095" ~ "Solano",
      home_county_GEOID == "06097" ~ "Sonoma",
      !is.na(home_county_GEOID) ~ "Outside Bay Area",
      TRUE ~ NA
  ))

  # create race_ethnicy
  # for case_when: "Each case is evaluated sequentially and the first match for each element
  # determines the corresponding value in the output vector."
  survey_combine <- survey_combine %>% mutate(
    race_ethnicity = case_when(
      hispanic == "HISPANIC/LATINO OR OF SPANISH ORIGIN" ~ "Hispanic/Latino (All Races)",
      race     == "WHITE" ~ "White (Non-Hispanic)",
      race     == "ASIAN" ~ "Asian (Non-Hispanic)",
      race     == "BLACK" ~ "Black (Non-Hispanic)",
      race     == "OTHER" ~ "Other (Non-Hispanic)",
      TRUE ~ NA
    ))

  # create trip_purpose by using dest_purp or orig_purp if trip_purp isn't available
  survey_combine <- survey_combine %>% mutate(
    trip_purpose = case_when(
      is.na(trip_purp) & (dest_purp != "missing") & (dest_purp != "home") ~ dest_purp,
      is.na(trip_purp) & (orig_purp != "missing") & (orig_purp != "home") ~ orig_purp,
      TRUE  ~ trip_purp,
    )) %>% mutate(
      trip_purpose_group = case_when(
        trip_purpose %in% c("work") ~ "Work",
        trip_purpose %in% c("school","high school") ~ "School",
        trip_purpose %in% c("social","shopping","social recreation") ~ "Social/Recreation/Shopping",
        trip_purpose %in% c("other", "other discretionary", "other maintenance","hotel","hotels") ~ "Other Purposes",
        TRUE ~ NA
      )
    )

  print("survey_combine:")
  print(survey_combine)
  str(as.data.frame(survey_combine))
  print(colnames(survey_combine))

  ##### Metadata
  print("Survey data by source, survey_name:")
  print(dplyr::count(survey_combine, source, survey_name, .drop=FALSE))

  print("Survey data by survey_tech:")
  print(dplyr::count(survey_combine, source, survey_name, survey_tech_group, survey_tech, .drop=FALSE))

  print("Survey data by operator:")
  print(dplyr::count(survey_combine, source, survey_name, operator, canonical_operator, .drop=FALSE))

  # ACE Survey has NA time_period, so set it to weekpart
  survey_combine <- mutate(survey_combine, time_period = coalesce(time_period, weekpart))

  print("Survey data by time_period, weekpart:")
  print(dplyr::count(survey_combine, source, survey_name, time_period, weekpart, .drop=FALSE))

  ##### Summary variables
  print("Survey data by household_income:")
  dplyr::count(survey_combine, source, survey_name, household_income_group, household_income, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by home_county:")
  dplyr::count(survey_combine, source, survey_name, home_county, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by race_ethnicity:")
  dplyr::count(survey_combine, source, survey_name, race_ethnicity, race, hispanic, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by trip_purpose:")
  dplyr::count(survey_combine, source, survey_name, trip_purpose_group, trip_purpose, trip_purp, orig_purp, dest_purp, .drop=FALSE) %>% print(n=Inf)

  ##### keep only relevant columns
  survey_combine <- select(survey_combine,
   unique_ID, source, survey_name, survey_year, operator, survey_tech, survey_tech_group, time_period, weekpart, weight,
   household_income_group, home_county, race_ethnicity, trip_purpose_group)

  # summarize by household income
  income_summary <- summarize_for_attr(survey_combine, household_income_group)

  # summarize for home county
  homecounty_summary <- summarize_for_attr(survey_combine, home_county)

  # summarize for race_ethnicity
  race_summary <- summarize_for_attr(survey_combine, race_ethnicity)

  # summarize for trip_purpose_group
  purpose_summary <- summarize_for_attr(survey_combine, trip_purpose_group)

  # put it together and save
  full_summary <- bind_rows(
    income_summary,
    homecounty_summary,
    race_summary,
    purpose_summary
  )

  return(full_summary)
}

summarize_snapshot_special_questions <- function() {
  # load the snapshot survey data
  snapshot_df <- read_excel(TPS_SNAPSHOT_FILE, sheet = "data file")
  print(glue("summarize_snapshot_special_questions(): Loaded {nrow(snapshot_df)} rows from '{TPS_SNAPSHOT_FILE}'"))
  print(str(snapshot_df))

  # set unique rowID based on row number in worksheet, which starts at 2
  snapshot_df$unique_ID <- 2:(nrow(snapshot_df)+1)

  # add required standard variables
  snapshot_df <- snapshot_df %>% mutate(
    source = "survey",
    survey_name = "Regional Snapshot",
    survey_year = 2023,
    weight = Weight,
    weekpart = case_when(
      Daytype == "DAY" ~ "WEEKDAY",
		  Daytype == "END" ~ "WEEKEND",
      TRUE ~ NA
    ),
    time_period = case_when(
      Strata == "AM"  ~ "AM PEAK",
		  Strata == "MID" ~ "MIDDAY",
		  Strata == "EVE" ~ "PM PEAK",
  		Strata == "END" ~ "WEEKEND",
      TRUE ~ NA
    ),
    # see SYSCODE_TO_OPERATOR in transit-passenger-surveys\make-uniform\production\preprocess\preprocessing_RegionalSnapshot_2023.py
    operator = case_when(
      Syscode == 1 ~ "AC Transit",
      Syscode == 2 ~ "BART",
      Syscode == 3 ~ "Caltrain",
      Syscode == 4 ~ "County Connection",
      Syscode == 5 ~ "Dumbarton Express",
      Syscode == 6 ~ "FAST",
      Syscode == 7 ~ "LAVTA",
      Syscode == 8 ~ "Marin Transit",
      Syscode == 9 ~ "Napa Vine",
      Syscode == 10 ~ "Petaluma Transit",
      Syscode == 11 ~ "Rio Vista Delta Breeze",
      Syscode == 12 ~ "SamTrans",
      Syscode == 13 ~ "VTA",
      Syscode == 14 ~ "Santa Rosa CityBus",
      Syscode == 15 ~ "SFMTA (Muni)",
      Syscode == 16 ~ "SMART",
      Syscode == 17 ~ "SolTrans",
      Syscode == 18 ~ "Sonoma County Transit",
      Syscode == 19 ~ "Tri Delta",
      Syscode == 20 ~ "Union City Transit",
      Syscode == 21 ~ "Vacaville City Coach",
      Syscode == 22 ~ "WestCAT",
      Syscode == 23 ~ "SF Bay Ferry",
      TRUE ~ "unset"
    ),
    # see TYPE_TO_SURVEY_TECH in transit-passenger-surveys\make-uniform\production\preprocess\preprocessing_RegionalSnapshot_2023.py
    survey_tech_group = case_when(
      Type == 1 ~ "Rail", # Rail
      Type == 2 ~ "Ferry",         # Ferry
      Type == 3 ~ "Local Bus",     # Bus (general)
      Type == 4 ~ "Local Bus",     # Bus - Local (AC Transit, Westcat, and Soltrans only)
      Type == 5 ~ "Express Bus",   # Bus - Express (AC Transit, Westcat, and Soltrans only)
      Type == 6 ~ "Express Bus",   # Bus - Transbay (AC Transit and Westcat only)
      Type == 7 ~ "Rail",          # Light rail (Muni and VTA only)
      Type == 8 ~ "Local Bus",     # Cable car/streetcar (Muni only)
      TRUE ~ "unset"
    )
  )

  # Q7. How often do you use public transit in the Bay Area?
  snapshot_df <- snapshot_df %>% 
    mutate(
      transit_freq = case_when(
        Q7 == "1" ~ "6–7 days/week",
        Q7 == "2" ~ "5 days/week",
        Q7 == "3" ~ "3–4 days/week",
        Q7 == "4" ~ "1–2 days/week",
        Q7 == "5" ~ "1–3 days/month",
        Q7 == "6" ~ "Less than once a month",
        Q7 == "7" ~ "First time riding",
        TRUE ~ NA
      ),
      transit_freq_group = case_when(
        transit_freq %in% c("5 days/week", "6–7 days/week") ~ ">4 days a week",
        transit_freq %in% c("1–2 days/week","3–4 days/week") ~ "1-4 days a week",
        transit_freq %in% c("1–3 days/month") ~ "1-3 days a month",
        transit_freq %in% c("First time riding","Less than once a month") ~ "<1 day a month",
        TRUE ~ NA
      ))

  # Q2 Did you have access to a household vehicle for this trip?
  snapshot_df <- snapshot_df %>% 
    mutate(
      hhveh = case_when(
        Q2 == 1 ~ "Yes",
        Q2 == 2 ~ "No",
        TRUE ~ NA
      ))

  # Q17 Do you have a disability that limits your ability to travel?
  snapshot_df <- snapshot_df %>% 
    mutate(
      disability = case_when(
        Q17 == 1 ~ "Yes",
        Q17 == 2 ~ "No",
        TRUE ~ NA
      ))

  # Q10	How safe do you feel when using public transit in the Bay Area?
  snapshot_df <- snapshot_df %>% 
    mutate(
      feel_safe = case_when(
        Q10 == 1 ~ "1 - Very Unsafe",
        Q10 == 2 ~ "2",
        Q10 == 3 ~ "3",
        Q10 == 4 ~ "4",
        Q10 == 5 ~ "5 - Very Safe",
        TRUE ~ NA
      ))
  # Q8_1 and Q8_2: Top two desired improvements – convert to long format for weighting both choices
  q8_long_df <- snapshot_df %>%
  select(unique_ID, source, survey_name, survey_year, weight, weekpart, time_period, operator, survey_tech_group, Q8_1, Q8_2, Q8_3) %>%
  mutate(all_records=1) %>%
  filter(is.na(Q8_3)) %>% #drop respondents who have picked more than two choices
  select(-Q8_3) %>%
  mutate(
    Q8_1 = as.character(Q8_1),
    Q8_2 = as.character(Q8_2)
  ) %>%
  pivot_longer(
    cols = c(Q8_1, Q8_2),
    names_to = "q8_top2",
    values_to = "q8_response"
  ) %>%
  filter(!is.na(q8_response)) %>%
  mutate(
    desired_improvement = case_when(
      q8_response == 1  ~ "Frequency",
      q8_response == 2  ~ "Reliability",
      q8_response == 3  ~ "Service hours",
      q8_response == 4  ~ "Transferring",
      q8_response == 5  ~ "Travel time",
      q8_response == 6  ~ "Lower fares",
      q8_response == 7  ~ "Cleanliness",
      q8_response == 8  ~ "Transit reach",
      # note original wording in CCG's deliverable; they reviewed the free text and added additional categories 
      # q8_response == 1  ~ "1 - More frequent service",
      # q8_response == 2  ~ "2 - More reliable service",
      # q8_response == 3  ~ "3 - Expanded hours when public transit operates",
      # q8_response == 4  ~ "4 - Shorter transfer times/Improved connections with other transit/longer effective time for transfers",
      # q8_response == 5  ~ "5 - Shorter travel time",
      # q8_response == 6  ~ "6 - Lower fares",
      # q8_response == 7  ~ "7 - Cleaner vehicles/stations",
      # q8_response == 8  ~ "8 - For transit to pick me up where I am/take me where I need to go",
      # q8_response == 9  ~ "9 - Other (Unspecified)",
      # q8_response == 10 ~ "10 - NOT USED",
      # q8_response == 11 ~ "11 - Updated/more modern/comfortable trains/buses",
      # q8_response == 12 ~ "12 - Better integration with online travel apps/Clear addresses for stops/better real time tracking",
      # q8_response == 13 ~ "13 - More/Better security",
      # q8_response == 14 ~ "14 - Fewer homeless",
      # q8_response == 15 ~ "15 - Stop fare evaders/drug use onboard/Better rule enforcement onboard",
      # q8_response == 16 ~ "16 - System needs to be more bike friendly (more space, storage, loading ramps, etc.)",
      # q8_response == 17 ~ "17 - Less crowding",
      # q8_response == 18 ~ "18 - Alternative payment options (online, contactless, multiple media, etc)",
      # q8_response == 19 ~ "19 - Improved/Upgraded/Better maintained stops/stations",
      # q8_response == 20 ~ "20 - More/better onboard amenities (desks, wi-fi, water, food, etc.)",
      # q8_response == 21 ~ "21 - More/Better/Safer parking",
      # q8_response == 22 ~ "22 - Better/More delay information",
      # q8_response == 23 ~ "23 - Rule changes (allow pets, food, etc.)",
      # q8_response == 24 ~ "24 - Easier to carry luggage/tools/equipment",
      # q8_response == 25 ~ "25 - More professional/helpful/friendly drivers/staff/easier to reach customer service",
      # q8_response == 26 ~ "26 - More/Improved/Clearer signage",
      # q8_response == 27 ~ "27 - On demand service",
      # q8_response == 28 ~ "28 - Better disabled access",
      # q8_response == 29 ~ "29 - Continued/More Covid mitigation measures (masking, distancing, etc.)",
      # q8_response == 30 ~ "30 - Easier to read schedules/provide in different formats/make more available/improved digital signage",
      TRUE ~ NA
    )
  ) %>%
  filter(!is.na(desired_improvement))

  print("Survey data by operator:")
  dplyr::count(snapshot_df, source, survey_name, operator, System, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by survey_tech_group:")
  dplyr::count(snapshot_df, source, survey_name, survey_tech_group, operator, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by transit_freq_group:")
  dplyr::count(snapshot_df, source, survey_name, transit_freq_group, transit_freq, Q7, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by hhveh:")
  dplyr::count(snapshot_df, source, survey_name, hhveh, Q2, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by disability:")
  dplyr::count(snapshot_df, source, survey_name, disability, Q17, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by disability:")
  dplyr::count(snapshot_df, source, survey_name, feel_safe, Q10, .drop=FALSE) %>% print(n=Inf)

  ##### keep only relevant columns
  snapshot_df <- select(snapshot_df,
   unique_ID, source, survey_name, survey_year, operator, survey_tech_group, time_period, weekpart, weight,
   transit_freq_group, hhveh, disability, feel_safe)

  # summarize by transit use frequency
  freq_summary <- summarize_for_attr(snapshot_df, transit_freq_group)

  # summarize by household vehicle available for this trip
  hhveh_summary <- summarize_for_attr(snapshot_df, hhveh)
  
  # summarize by disability limiting travel
  disability_summary <- summarize_for_attr(snapshot_df, disability)

  # summarize by feeling safe
  safety_summary <- summarize_for_attr(snapshot_df, feel_safe)

  # summarize by desired improvement
  desiredImprov_summary <- summarize_for_attr(q8_long_df, desired_improvement)

  special_summary <- bind_rows(
    freq_summary, 
    hhveh_summary,
    disability_summary,
    safety_summary,
    desiredImprov_summary
  )
  return(special_summary)
}

main <- function() {
  options(width = 10000)
  options(dplyr.width = 10000)
  options(datatable.print.nrows = 1000)
  # options(warn=2) # error on warning

  run_log <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "summarize_snapshot_2023_for_dashboard.log")
  print(glue("Writing log to {run_log}"))
  sink(run_log, append=FALSE, type = c('output', 'message'))

  # summarize Standardized survey data
  standardized_summary <- summarize_standardized_survey()
  print(str(standardized_summary))

  # summarize Special Questions
  special_summary <- summarize_snapshot_special_questions()

  # put them together
  full_summary <- bind_rows(standardized_summary, special_summary)

  # save N=1,234
  full_summary <- full_summary %>% mutate(
    total_unweighted_str = case_when(
      source == "survey" ~ paste0("N=",prettyNum(total_unweighted, big.mark = ",", scientific = FALSE)),
      TRUE ~ NA_character_
  ))

  # Generate estimate reliability summary report
  print("=== ESTIMATE RELIABILITY SUMMARY ===")
  reliability_summary <- full_summary %>%
    filter(source == "survey") %>%
    summarise(
      total_estimates = n(),
      poor_estimates = sum(str_starts(estimate_reliability, "Poor"), na.rm = TRUE),
      suppression_rate = round(100 * sum(str_starts(estimate_reliability, "Poor"), na.rm = TRUE) / n(), 1),
      .groups = "drop"
    )
  print(glue("Total estimates: {reliability_summary$total_estimates}"))
  print(glue("Poor quality estimates: {reliability_summary$poor_estimates} ({reliability_summary$suppression_rate}%)"))

  # Estimate reliability distribution
  reliability_breakdown <- full_summary %>%
    filter(source == "survey") %>%
    count(estimate_reliability, sort = TRUE)
  print("Estimate reliability distribution:")
  print(reliability_breakdown)

  output_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "summarize_snapshot_2023_for_dashboard.Rdata")
  save(full_summary, file = file.path(output_file))
  print(glue("Wrote {nrow(full_summary)} to {output_file}"))
  message(glue("Wrote {nrow(full_summary)} to {output_file}"))
}

main()