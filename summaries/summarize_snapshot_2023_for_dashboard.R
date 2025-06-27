#
# Summarizes the 2023 Regional Snapshot Transit Passenger Survey for the dashboard,
# including unweighted and weighted responses, and measurers of error.
#
# This script does the following:
# 1. Reads the most recent standardized survey data (this includes ACS data)
# 2. Filters to survey_year==2023
# 3. Recodes some variables for summaries
# 4. Summarizes survey data using srvyr package,
#    by survey_tech_group or by operator
#    for a given variable (income, home county, etc)
#
#         Output is written to TPS_SURVEY_STANDARDIZED_PATH\summarize_snapshot_2023_for_dashboard.Rdata
# and a log file is written to TPS_SURVEY_STANDARDIZED_PATH\summarize_snapshot_2023_for_dashboard.log
#
library(glue)
library(rlang)
library(tidyverse)
library(srvyr)

# Read the standardized survey data
TPS_SURVEY_STANDARDIZED_PATH <- "M:\\Data\\OnBoard\\Data and Reports\\_data_Standardized\\standardized_2025-05-16"

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
        as_survey_design(weights = weight, strata = c(operator, weekpart))      

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
        rename(weighted_count = weighted_count_actual, unweighted_count = unweighted_count_actual) %>%
        # Keep relevant columns
        select(all_of(summary_level), all_of(summary_col_str), weighted_share, se, ci_95, ci_lower_95, ci_upper_95, coeff_of_var,
               weighted_count, unweighted_count, total_weighted, total_unweighted,
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

main <- function() {
  options(width = 10000)
  options(dplyr.width = 10000)
  options(datatable.print.nrows = 1000)
  # options(warn=2) # error on warning

  run_log <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "summarize_snapshot_2023_for_dashboard.log")
  print(glue("Writing log to {run_log}"))
  sink(run_log, append=FALSE, type = c('output', 'message'))

  # load the standardized survey data
  source_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "survey_combined.Rdata")
  loaded <- load(source_file)
  print(glue("Loaded {nrow(survey_combine)} rows from {source_file}"))

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

  print("survey_combine:")
  print(survey_combine)
  str(as.data.frame(survey_combine))
  print(colnames(survey_combine))

  print("Survey data by source, survey_name:")
  print(dplyr::count(survey_combine, source, survey_name, .drop=FALSE))

  print("Survey data by survey_tech:")
  print(dplyr::count(survey_combine, source, survey_name, survey_tech_group, survey_tech, .drop=FALSE))

  print("Survey data by operator:")
  print(dplyr::count(survey_combine, source, survey_name, operator, canonical_operator, .drop=FALSE))

  print("Survey data by weekpart:")
  print(dplyr::count(survey_combine, source, survey_name, weekpart, .drop=FALSE))

  print("Survey data by household_income:")
  dplyr::count(survey_combine, source, survey_name, household_income_group, household_income, .drop=FALSE) %>% print(n=Inf)

  print("Survey data by home_county:")
  dplyr::count(survey_combine, source, survey_name, home_county, .drop=FALSE) %>% print(n=Inf)

  select(filter(survey_combine, (home_county == "Napa") & (weekpart == "WEEKDAY")), 
    ID, weekpart, survey_name, survey_year, canonical_operator, survey_tech, home_county, home_county_GEOID) %>% print(n=Inf)

  # keep only relevant columns
  survey_combine <- select(survey_combine,
   unique_ID, source, survey_name, survey_year, operator, survey_tech, survey_tech_group, weekpart, weight,
   household_income_group, home_county)

  # summarize by household income
  income_summary <- summarize_for_attr(survey_combine, household_income_group)

  # summarize for home county
  homecounty_summary <- summarize_for_attr(survey_combine, home_county)

  # put it together and save
  full_summary <- bind_rows(income_summary,homecounty_summary)
  # save N=1,234
  full_summary <- full_summary %>% mutate(
    total_unweighted_str = case_when(
      source == "survey" ~ paste0("N=",prettyNum(total_unweighted, big.mark = ",", scientific = FALSE)),
      TRUE ~ NA_character_
  ))

  output_file <- file.path(TPS_SURVEY_STANDARDIZED_PATH, "summarize_snapshot_2023_for_dashboard.Rdata")
  save(full_summary, file = file.path(output_file))
  print(glue("Wrote {nrow(full_summary)} to {output_file}"))
  message(glue("Wrote {nrow(full_summary)} to {output_file}"))
}

main()