#### Purpose
# Combines legacy data (see `Extract Variables from Legacy Surveys` and `Build Legacy Database`)
# with standard data (see `Build Standard Database`).

combine_data <- function(data_standard,
                         data_legacy){
  
  # modify legacy data field names to be consistent with standard data field names
  data_legacy <- data_legacy %>%
    rename('unique_ID'         = 'Unique_ID',
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
  
  # DEBUG
  legacy_names <- colnames(data_legacy)
  std_names <- colnames(data_standard)
  
  print('variables in standard_database but not in legacy_database:')
  for (std_name in std_names) {
    if(!(std_name %in% legacy_names)) {
      print(std_name)
      # New column in standardized data. Add NA column in legacy so rbind will succeed.
      data_legacy[,std_name] <- NA
      if (typeof(data_standard[,std_name])=="character") {
        data_legacy[,std_name] <- as.character(data_legacy[,std_name])
      } else if (typeof(data_standard[,std_name])=="double") {
        data_legacy[,std_name] <- as.numeric(data_legacy[,std_name])
      }
    }
  }
  
  print('variables in legacy_database but not in standard_database:')
  for (legacy_name in legacy_names) {
    if(!(legacy_name %in% std_names)) {
      print(legacy_name)
      # Columns in legacy data but not in standardized data. Add NA column in legacy so rbind will succeed.
      data_standard[,legacy_name] <- NA
      if (typeof(data_legacy[,legacy_name])=="character") {
        data_standard[,legacy_name] <- as.character(data_standard[,legacy_name])
      } else if (typeof(data_legacy[,legacy_name])=="double") {
        data_standard[,legacy_name] <- as.numeric(data_standard[,legacy_name])
      }
    }
  }
  # end DEBUG
  
  data_legacy['survey_batch'] = 'legacy'
  data_standard['survey_batch'] = 'standard'
  
  data_combine <- rbind(data_standard, data_legacy)
  
  # Remove the BART pre-test data
  data_combine <- data_combine %>%
    filter(operator != "BART PRE-TEST")
  
  # Make operator name consistent
  data_combine <- data_combine %>%
    # revise operator names in legacy data to be consistent with standard data;
    # also abbreviate 'Sonoma-Marin Area Rail Transit' to 'SMART' 
    mutate(operator = recode(operator,
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

