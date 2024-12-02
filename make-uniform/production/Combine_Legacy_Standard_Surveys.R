#### Purpose
# Combines legacy data (see `Extract Variables from Legacy Surveys` and `Build Legacy Database`)
# with standard data (see `Build Standard Database`).

combine_data <- function(data_standard,
                         data_legacy){
  
  # taps are deprecated -- remove from legacy
  data_legacy <- data_legacy %>% select(!ends_with("_tap"))
  # auto_suff is auto-biased; use the more clear autos_vs_workers
  data_legacy <- data_legacy %>%
    mutate(autos_vs_workers = case_when(
      auto_suff == "zero autos"       ~ "zero autos",
      auto_suff == "auto negotiating" ~ "workers > autos",
      auto_suff == "auto sufficient"  ~ "workers <= autos",
      # "Missing" doesn't need to be coded as such; leaving unset is more standard
    )) %>% 
    select(-auto_suff)

  # modify legacy data field names to be consistent with standard data field names
  data_legacy <- data_legacy %>%
    rename('unique_ID'         = 'Unique_ID',
           'survey_name'       = 'operator',
           'dest_tm2_maz'      = 'dest_maz',
           'dest_tm1_taz'      = 'dest_taz',
           'home_tm2_maz'      = 'home_maz',
           'home_tm1_taz'      = 'home_taz',
           'orig_tm2_maz'      = 'orig_maz',
           'orig_tm1_taz'      = 'orig_taz',
           'school_tm2_maz'    = 'school_maz',
           'school_tm1_taz'    = 'school_taz',
           'workplace_tm2_maz' = 'workplace_maz',
           'workplace_tm1_taz' = 'workplace_taz')

  # survey_name -> canonical_operator
  print('Initial tabulation on survey_name vs survey_tech for legacy surveys:')
  print(table(data_legacy$survey_name, data_legacy$survey_tech, useNA = 'ifany'))
  # code canonical_operator to be consistent with standard
  data_legacy <- data_legacy %>%
    mutate(
      canonical_operator = case_match(
        survey_name,
          "AC Transit"                  ~ "AC TRANSIT",
          "BART PRE-TEST"               ~ "BART",
          "County Connection"           ~ "COUNTY CONNECTION",
          "Golden Gate Transit (bus)"   ~ "GOLDEN GATE TRANSIT",
          "Golden Gate Transit (ferry)" ~ "GOLDEN GATE TRANSIT",
          "Petaluma"                    ~ "PETALUMA TRANSIT",
          "SamTrans"                    ~ "SAMTRANS",
          "SF Bay Ferry"                ~ "SF BAY FERRY",
          "Sonoma County"               ~ "Sonoma County Transit",
          "Tri-Delta"                   ~ "TRI-DELTA",
          "Union City"                  ~ "UNION CITY",
          .default = survey_name
      ),
      # and survey_name to be consistent with standard
      survey_name = case_match(
        survey_name,
          "Golden Gate Transit (bus)"   ~ "Golden Gate Transit",
          "Golden Gate Transit (ferry)" ~ "Golden Gate Transit",
          "Petaluma"                    ~ "Petaluma Transit",
          "Sonoma County"               ~ "Sonoma County Transit",
          "Tri-Delta"                   ~ "TriDelta",
          "Union City"                  ~ "Union City Transit",
          .default = survey_name
      ),
      # convert AC Transit Route DB and DB1 to canonical_operator == DUMBARTON
      canonical_operator = 
        ifelse((canonical_operator=="AC TRANSIT") & ((route=="DB") | (route == "DB1")), 
          "DUMBARTON", canonical_operator)
    )

  print('Updated tabulation on survey_name vs survey_tech for legacy surveys:')
  print(table(data_legacy$survey_name, data_legacy$survey_tech, useNA = 'ifany'))
  print('Updated tabulation on canonical_operator vs survey_tech for legacy surveys:')
  print(table(data_legacy$canonical_operator, data_legacy$survey_tech, useNA = 'ifany'))

  # DEBUG
  legacy_names <- colnames(data_legacy)
  std_names <- colnames(data_standard)
  
  print('variables in standard_database but not in legacy_database:')
  for (std_name in std_names) {
    if(!(std_name %in% legacy_names)) {
      print(paste(" ",std_name))
    }
  }
  
  print('variables in legacy_database but not in standard_database:')
  for (legacy_name in legacy_names) {
    if(!(legacy_name %in% std_names)) {
      print(paste(" ",legacy_name))
    }
  }

  # check types
  std_class <- sapply(data_standard,class)
  legacy_class <- sapply(data_legacy,class)
  for (col_name in std_names) {
    if(col_name %in% legacy_names) {
      if (std_class[col_name] != legacy_class[col_name]) {
        print(paste("Mismatched classes for",col_name,
        "; standard:",std_class[col_name],
        "; legacy:",legacy_class[col_name]))
        # convert to standard
        if (std_class[col_name] == "numeric") {
          # don't warn on NAs introduced by coercion
          suppressWarnings(
            data_legacy <- data_legacy %>% mutate(!!col_name := as.numeric(!!col_name))
          )
        }
        else if (std_class[col_name] == "character") {
          # don't warn on NAs introduced by coercion
          suppressWarnings(
            data_legacy <- data_legacy %>% mutate(!!col_name := as.character(!!col_name))
          )
        }
      }
    }
  }
  # end DEBUG
  
  data_legacy['survey_batch'] = 'legacy'
  data_standard['survey_batch'] = 'standard'
  
  # this fills missing columns with NAs
  data_combine <- dplyr::bind_rows(data_standard, data_legacy)
  
  # Remove the BART pre-test data
  data_combine <- data_combine %>%
    filter(survey_name != "BART PRE-TEST")
  
  # Make survey_name name consistent
  data_combine <- data_combine %>%
    # revise survey_name names in legacy data to be consistent with standard data;
    # also abbreviate 'Sonoma-Marin Area Rail Transit' to 'SMART' 
    mutate(survey_name = recode(survey_name,
                             'Golden Gate Transit (ferry)'    = 'Golden Gate Transit',
                             'Golden Gate Transit (bus)'      = 'Golden Gate Transit',
                             'Tri-Delta'                      = 'TriDelta',
                             'Union City'                     = 'Union City Transit',
                             'Sonoma County'                  = 'Sonoma County Transit',
                             'Sonoma-Marin Area Rail Transit' = 'SMART',
                             'Petaluma'                       = 'Petaluma Transit',
                             'SF Bay Ferry'                   = 'SF Bay Ferry/WETA',
                             'WETA'                           = 'SF Bay Ferry/WETA'))
  
  return (data_combine)
}

