#### Purpose
# Combines legacy data (see `Extract Variables from Legacy Surveys` and `Build Legacy Database`)
# with standard data (see `Build Standard Database`) and then extracts `CSV` files for use in Tableau summaries.
# Script also writes out combined data sets to disk as `CSV`.


######## Preparation ########
#############################
# library(tidyverse)
library(dplyr)
library(stringr)

# The working directory is set as the location of the script. All other paths will be relative.
wd <- paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")
setwd(wd)

# Input
F_INPUT_LEGACY_RDATA = 'M:/Data/OnBoard/Data and Reports/_data Standardized/survey_legacy.RData'
F_INPUT_STANDARD_CSV = 'M:/Data/OnBoard/Data and Reports/_data Standardized/survey_standard_2021-05-20.csv'
F_STD_DICTIONARY_CSV = paste0('C:/Users/',
                              Sys.getenv("USERNAME"),
                              '/Documents/GitHub/onboard-surveys/util/standard_variable_dict.csv')

F_TM1_TAZ_CSV = 'M:/Data/GIS layers/TM1_taz/bayarea_rtaz1454_rev1_WGS84.csv'
F_SD_NAME_CSV = paste0('C:/Users/',
                       Sys.getenv("USERNAME"),
                       '/Documents/GitHub/bayarea_urbansim/data/superdistricts.csv')

F_DEMO_TM1_TAZ_CSV = paste0('C:/Users/',
                            Sys.getenv("USERNAME"),
                            '/Documents/GitHub/petrale/applications/travel_model_lu_inputs/2015/TAZ1454_Ethnicity.csv')

F_PUMS_H_RDATA <- 'M:/Data/Census/PUMS/PUMS 2015-19/hbayarea1519.Rdata'
F_PUMS_P_RDATA <- 'M:/Data/Census/PUMS/PUMS 2015-19/pbayarea1519.Rdata'

# Output
F_COMBINED_CSV = 'M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-05-20.csv'
F_COMBINED_RDATA = 'M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-05-20.RData'

D_OUTPUT_TABLEAU = "M:/Data/OnBoard/Data and Reports/_data Standardized/tableau"
F_TABLEAU_CSV  = paste0(D_OUTPUT_TABLEAU, '/for_tableau_all_survey_by_passenger.csv')
F_TAZ_CSV      = paste0(D_OUTPUT_TABLEAU, '/for_tableau_all_survey_by_TM1_TAZ.csv')
F_TAZ_DEMO_CSV = paste0(D_OUTPUT_TABLEAU, '/for_tableau_ACS_by_TM1_TAZ.csv')
F_PUMS_HH_DEMO_CSV  = paste0(D_OUTPUT_TABLEAU, '/for_tableau_PUMS.csv')

# Setup the log file
today = Sys.Date()
run_log <- file(sprintf("%s/for_tableau_summaryLog_%s.log", D_OUTPUT_TABLEAU, today))
sink(run_log, append=TRUE, type = 'output')
sink(run_log, append=TRUE, type = "message")


######## Combine Legacy and Standard data ########
##################################################

load(F_INPUT_LEGACY_RDATA)
sprintf('Read %d rows of legacy data', nrow(survey.legacy))
survey_standard <- read.csv(file = F_INPUT_STANDARD_CSV, header = TRUE)
sprintf('Read %d rows of standard data', nrow(survey_standard))

# revise legacy data field names to be consistent with standard data field names
survey.legacy <- survey.legacy %>%
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
         'workplace_tm1_taz' = 'workplace_taz' 
  )

# DEBUG
legacy_names <- colnames(survey.legacy)
std_names <- colnames(survey_standard)

print('variables in standard_database but not in legacy_database:')
for (std_name in std_names) {
  if(!(std_name %in% legacy_names)) {
    print(std_name)
    # New column in standardized data. Add NA column in legacy so rbind will succeed.
    survey.legacy[,std_name] <- NA
    if (typeof(survey_standard[,std_name])=="character") {
      survey.legacy[,std_name] <- as.character(survey.legacy[,std_name])
    } else if (typeof(survey_standard[,std_name])=="double") {
      survey.legacy[,std_name] <- as.numeric(survey.legacy[,std_name])
    }
  }
}

print('variables in legacy_database but not in standard_database:')
for (legacy_name in legacy_names) {
  if(!(legacy_name %in% std_names)) {
    print(legacy_name)
    # Columns in legacy data but not in standardized data. Add NA column in legacy so rbind will succeed.
    survey_standard[,legacy_name] <- NA
    if (typeof(survey.legacy[,legacy_name])=="character") {
      survey_standard[,legacy_name] <- as.character(survey_standard[,legacy_name])
    } else if (typeof(survey.legacy[,legacy_name])=="double") {
      survey_standard[,legacy_name] <- as.numeric(survey_standard[,legacy_name])
    }
  }
}
# end DEBUG

survey.legacy['survey_batch'] = 'legacy'
survey_standard['survey_batch'] = 'standard'

data.ready <- rbind(survey_standard, survey.legacy)

# Remove the BART pre-test data
data.ready <- data.ready %>%
  filter(operator != "BART PRE-TEST")

# Make operator name consistent
data.ready <- data.ready %>%
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

# export combined data
sprintf('Export %d rows and %d columns of legacy-standard combined data to %s and %s',
        nrow(data.ready),
        ncol(data.ready),
        F_COMBINED_CSV,
        F_COMBINED_RDATA)
write.csv(data.ready, F_COMBINED_CSV, row.names = FALSE)
save(data.ready, file = F_COMBINED_RDATA)


######## Prepare Data for Tableau: Surveyed-passenger-level ########
####################################################################

df <- data.ready

## remove Capitol Corridor survey from the data since it is not part of MTC's Transit Passenger Survey
df <- df[which(df$operator != 'Capitol Corridor'),]

## create a field to represent operator + survey_year
df$operator_survey_year <- paste0(df$operator, ' - ', df$survey_year)

sprintf('Read %d rows including the following surveys:', nrow(df))
print(count(df, operator_survey_year))

## create a field to represent survey versions
df$survey_version <- 'new'
df$survey_version[(df$operator_survey_year == 'AC Transit - 2012') | (
                   df$operator_survey_year == 'ACE - 2014') | (
                   df$operator_survey_year == 'County Connection - 2012') | (
                   df$operator_survey_year == 'Golden Gate Transit - 2013') | (
                   df$operator_survey_year == 'LAVTA - 2013') | (
                   df$operator_survey_year == 'Napa Vine - 2014') | (
                   df$operator_survey_year == 'Petaluma Transit - 2012') | (
                   df$operator_survey_year == 'Santa Rosa CityBus - 2012') | (
                   df$operator_survey_year == 'Sonoma County Transit - 2012') | (
                   df$operator_survey_year == 'TriDelta - 2014') | (
                   df$operator_survey_year == 'Union City Transit - 2013') | (
                   df$operator_survey_year == 'SF Bay Ferry/WETA - 2013')] <- 'old'

print('Double-check the survey version:')
print(table(df$operator_survey_year, df$survey_version))

## summarize fare medium and fare category into fewer categories
# use standard value dictionary
dict_df = read.csv(file = F_STD_DICTIONARY_CSV, header = TRUE)[ ,c('generic_variable',
                                                                   'valid_values_for_categoric_variables',
                                                                   'standard_values_for_categoric_variables')]

names(dict_df)[1] <- 'variable_name'
names(dict_df)[2] <- 'value_details'
names(dict_df)[3] <- 'value_summary'

# trim white spaces in 'fare_medium' and 'fare_category'
trim_func <- function(x) str_trim(x)
df <- cbind(df[, -which(names(df) %in% c('fare_medium', 'fare_category'))],
            lapply(df[c('fare_medium', 'fare_category')], trim_func))

# merge with variable dictionary to get summarized categories
fare_medium_dict <- dict_df[which(dict_df$variable_name == 'fare_medium'),]
fare_category_dict <- dict_df[which(dict_df$variable_name == 'fare_category'),]
df <- df %>%
  left_join(fare_medium_dict, by = c('fare_medium' = 'value_details')) %>%
  rename('fare_medium_summary' = 'value_summary') %>%
  left_join(fare_category_dict, by = c('fare_category' = 'value_details')) %>%
  rename('fare_category_summary' = 'value_summary')

print('Stats of fare_medium_summary:')
print(count(df, fare_medium_summary))
print('Stats of fare_category_summary:')
print(count(df, fare_category_summary))


## consistently label 'missing' value for fields that should have a value
tot_cnt = nrow(df)
  
for (colname in c('race', 'hispanic', 'household_income', 'approximate_age',
                'work_status', 'student_status', 'auto_suff',
                'access_mode', 'egress_mode', 'boardings', 'depart_hour', 'return_hour',
                'tour_purp', 'weekpart', 'day_part', 'fare_medium_summary', 'fare_category_summary',
                'eng_proficient', 'persons', 'gender')){
  df[colname][(df[colname] == 'MISSING') | (
               df[colname] == 'Missing') | (
               df[colname] == 'UNKNOWN') | (
               df[colname] == 'PREFER NOT TO ANSWER') | (
               df[colname] == 'SKIP - PAPER SURVEY') | (
               df[colname] == 'do not know') | (
               df[colname] == '.') | (
               df[colname] == '') | (
               df[colname] == 'refused') | (
               df[colname] == 'Missing - Dummy Record' ) | (
               df[colname] == 'Missing - Question Not Asked') | (
               df[colname] == 'Unknown') | is.na(df[colname])] <- 'missing'
  missing_cnt = nrow(df[which(df[colname] == 'missing'),])
  info <- sprintf('%s missing data in %d rows, %.2f of total', colname, missing_cnt, missing_cnt/tot_cnt)
  print(eval(info))
  print(table(df[colname]))
  }

# variables only in heavy rail/commuter rail surveys
for (colname in c('onoff_enter_station', 'onoff_exit_station')){
  df[colname][
    ((df$survey_tech == 'commuter rail') | (df$survey_tech == 'heavy rail')) & is.na(df[colname])] <- 'missing'
  missing_cnt = nrow(df[which(df[colname] == 'missing'),])
  rail_cnt = nrow(df[which((df$survey_tech == 'commuter rail') | (df$survey_tech == 'heavy rail')),])
  info <- sprintf('%s missing data in %d rows, %.2f of total', colname, missing_cnt, missing_cnt/rail_cnt)
  print(eval(info))
}

# fix station names: fix encoding issue, unify station names in different versions of survey
for (colname in c('onoff_enter_station', 'onoff_exit_station')){
  df[, colname] <- str_trim(df[, colname])
}

# fix station names like 'College�Park'
print('onoff_enter_station before fixing:')
print(table(df$onoff_enter_station))
df$onoff_enter_station[(
  startsWith(df$onoff_enter_station, 'College')) & (endsWith(df$onoff_enter_station, 'Park'))] <- 'College Park'
df$onoff_enter_station[(
  startsWith(df$onoff_enter_station, 'Mountain')) & (endsWith(df$onoff_enter_station, 'View'))] <- 'Mountain View'
df$onoff_enter_station[(
  startsWith(df$onoff_enter_station, 'San')) & (endsWith(df$onoff_enter_station, 'Antonio'))] <- 'San Antonio'
df$onoff_enter_station[(
  startsWith(df$onoff_enter_station, 'Santa')) & (endsWith(df$onoff_enter_station, 'Clara'))] <- 'Santa Clara'
print('onoff_enter_station after fixing:')
print(table(df$onoff_enter_station))

print('onoff_exit_station before fixing:')
print(table(df$onoff_exit_station))
df$onoff_exit_station[(
  startsWith(df$onoff_exit_station, 'College')) & (endsWith(df$onoff_exit_station, 'Park'))] <- 'College Park'
df$onoff_exit_station[(
  startsWith(df$onoff_exit_station, 'Mountain')) & (endsWith(df$onoff_exit_station, 'View'))] <- 'Mountain View'
df$onoff_exit_station[(
  startsWith(df$onoff_exit_station, 'San')) & (endsWith(df$onoff_exit_station, 'Antonio'))] <- 'San Antonio'
df$onoff_exit_station[(
  startsWith(df$onoff_exit_station, 'Santa')) & (endsWith(df$onoff_exit_station, 'Clara'))] <- 'Santa Clara'
print('onoff_exit_station after fixing:')
print(table(df$onoff_exit_station))


df <- df %>%
  mutate(onoff_enter_station = recode(onoff_enter_station,
                                      'FREMONT STATION' = 'Fremont Station',
                                      'GREAT AMERICA STATION' = 'Great America Station',
                                      'LATHROP-MANTECA STATION' = 'Lathrop/Manteca Station',
                                      'LIVERMORE STATION' = 'Livermore Station',
                                      'PLEASANTON STATION' = 'Pleasanton Station',
                                      'SAN JOSE STATION' = 'San Jose Station',
                                      'SANTA CLARA STATION' = 'Santa Clara University Station',
                                      'STOCKTON STATION' = 'Stockton Station',
                                      'TRACY STATION' = 'Tracy Station',
                                      'VASCO STATION' = 'Vasco Station')) %>%
  mutate(onoff_exit_station = recode(onoff_exit_station,
                                     'FREMONT STATION' = 'Fremont Station',
                                     'GREAT AMERICA STATION' = 'Great America Station',
                                     'LATHROP-MANTECA STATION' = 'Lathrop/Manteca Station',
                                     'LIVERMORE STATION' = 'Livermore Station',
                                     'PLEASANTON STATION' = 'Pleasanton Station',
                                     'SAN JOSE STATION' = 'San Jose Station',
                                     'SANTA CLARA STATION' = 'Santa Clara University Station',
                                     'STOCKTON STATION' = 'Stockton Station',
                                     'TRACY STATION' = 'Tracy Station',
                                     'VASCO STATION' = 'Vasco Station'))


# creat a field to represent operator + on/off_station for heavy rail/commuter rail surveys
df$board_station <- paste0(df$operator, ' - ', df$onoff_enter_station)
df$alight_station <- paste0(df$operator, ' - ', df$onoff_exit_station)
df$board_station[(df$survey_tech != 'commuter rail') & (df$survey_tech != 'heavy rail')] <- NA
df$alight_station[(df$survey_tech != 'commuter rail') & (df$survey_tech != 'heavy rail')] <- NA


## create variable 'immediate_access_mode' and 'immediate_egress_mode' to represent
# the connection directly before and after the surveyed route
df <- df %>%
  mutate(immediate_access_mode = access_mode) %>%
  mutate(immediate_egress_mode = egress_mode) %>%
  
  # in standard survey,
  # before/after technology == 'Missing' indicating no transfer; before/after technology is NA indicating legacy survey
  # also, only need to check first_before and first_after technology
  mutate(immediate_access_mode = ifelse((first_before_technology != 'Missing') & (!is.na(first_before_operator_detail)),
                                        'transit', immediate_access_mode)) %>%
  mutate(immediate_egress_mode = ifelse((first_after_technology != 'Missing') & (!is.na(first_after_operator_detail)),
                                        'transit', immediate_egress_mode)) %>%
  
  # legacy survey data doesn't contain before/after transfer technology
  mutate(immediate_access_mode = ifelse(is.na(first_before_technology), 'missing', immediate_access_mode)) %>%
  mutate(immediate_egress_mode = ifelse(is.na(first_after_technology), 'missing', immediate_egress_mode))

# create access_egress_diff for QA/QC. 
# It contains records where access_mode != immediate_access_mode, or egress_mode != immediate_egress_mode
access_egress_diff <- df[which((df$immediate_access_mode != df$access_mode) | (df$immediate_egress_mode != df$egress_mode)),
                         c('operator_survey_year',
                           'access_mode', 'immediate_access_mode', 'first_board_tech',
                           'first_before_technology', 'second_before_technology', 'third_before_technology',
                           'egress_mode', 'immediate_egress_mode', 'last_alight_tech',
                           'first_after_technology', 'second_after_technology', 'third_after_technology')]

print('Stats of access_mode and immediate_access_mode:')
print(count(df, access_modes))
print(count(df, immediate_access_modes))
print('Stats of egress_mode and immediate_egress_mode:')
print(count(df, egress_mode))
print(count(df, immediate_egress_mode))


## create 'age_group' field
df$approximate_age <- as.numeric(df$approximate_age)

df$age_group <- 'missing'
df$age_group[(df$approximate_age > 0)  & (df$approximate_age < 16)] <- 'Below 16'
df$age_group[(df$approximate_age > 15) & (df$approximate_age < 23)] <- '16 to 22'
df$age_group[(df$approximate_age > 22) & (df$approximate_age < 30)] <- '23 to 29'
df$age_group[(df$approximate_age > 29) & (df$approximate_age < 40)] <- '30 to 39'
df$age_group[(df$approximate_age > 39) & (df$approximate_age < 50)] <- '40 to 49'
df$age_group[(df$approximate_age > 49) & (df$approximate_age < 60)] <- '50 to 59'
df$age_group[(df$approximate_age > 59) & (df$approximate_age < 70)] <- '60 to 69'
df$age_group[df$approximate_age > 69] <- 'Above 69'

print('Stats of age_group:')
print(count(df, age_group))


## rename 'auto sufficient' field name and values
df <- df %>% 
  rename('hh_auto_ownership' = 'auto_suff') %>%
  mutate(hh_auto_ownership = recode(hh_auto_ownership,
                                    'auto sufficient' = 'autos >= workers',
                                    'zero autos' = 'zero autos', 
                                    'auto negotiating' = 'autos < workers'))
print('Stats of household vehicle ownership:')
print(count(df, hh_auto_ownership))


## recode race/ethnicity categories
df['race_ethnicity'] = paste0(df$hispanic, '__', df$race)

df <- df %>%
  mutate(race_ethnicity = recode(race_ethnicity,
                                 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN__BLACK' = 'Black Non-hispanic',
                                 'HISPANIC/LATINO OR OF SPANISH ORIGIN__OTHER' = 'Hispanic',
                                 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN__ASIAN' = 'Asian Non-hispanic',
                                 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN__WHITE' = 'White Non-hispanic',
                                 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN__missing' = 'missing',
                                 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN__OTHER' = 'Other Non-hispanic',
                                 'HISPANIC/LATINO OR OF SPANISH ORIGIN__WHITE' = 'Hispanic',
                                 'HISPANIC/LATINO OR OF SPANISH ORIGIN__ASIAN' = 'Hispanic', 
                                 'missing__missing' = 'missing',
                                 'HISPANIC/LATINO OR OF SPANISH ORIGIN__BLACK' = 'Hispanic',
                                 'HISPANIC/LATINO OR OF SPANISH ORIGIN__missing' = 'Hispanic', 
                                 'missing__ASIAN' = 'Asian Non-hispanic',
                                 'missing__OTHER' = 'Other Non-hispanic',
                                 'missing__WHITE' = 'White Non-hispanic',
                                 'missing__BLACK' = 'Black Non-hispanic'
    
  ))
print('Stats of race_ethnicity:')
print(count(df, race_ethnicity))

## combine 'evening' (standard data) with 'night' (legacy data) for variable 'day_part'
df <- df %>%
  mutate(day_part = recode(day_part,
                           'NIGHT' = 'EVENING'))

## tour_purp 'work-related' or 'business apt' should be 'other maintenance'
df <- df %>%
  mutate(tour_purp = recode(tour_purp,
                            'work-related' = 'other maintenance'))

## recode 'persons'
df <- df %>%
  mutate(persons = recode(persons,
                          'eight' = 'six or more',
                          'eleven' = 'six or more',
                          'nine'  = 'six or more',
                          'seven' = 'six or more',
                          'six' = 'six or more',
                          'ten' = 'six or more',
                          'ten or more' = 'six or more',
                          'twenty-seven' = 'six or more'))

## recategorize household income
df <- df %>%
  mutate(household_income = recode(household_income,
                                   'under $10,000' = 'under $25,000',
                                   '$10,000 to $25,000' = 'under $25,000',
                                   '$25,000 to $35,000' = '$25,000 to $50,000',
                                   '$35,000 to $50,000' = '$25,000 to $50,000',
                                   '$50,000 to $75,000' = '$50,000 to $100,000',
                                   '$75,000 to $100,000' = '$50,000 to $100,000'))


## export needed fields for Tableau
basic_info = c('survey_version', 'survey_batch', 'operator_survey_year', 'operator', 'survey_year',
               'survey_tech', 'weekpart', 'day_part', 'trip_weight', 'weight')

trip_info = c('board_station', 'alight_station', 'first_board_tech', 'last_alight_tech',
              'access_mode', 'egress_mode', 'immediate_access_mode', 'immediate_egress_mode', 'boardings',
              'orig_purp', 'dest_purp', 'tour_purp', 'fare_category_summary', 'fare_medium_summary',
              'commuter_rail_present', 'heavy_rail_present', 'ferry_present',
              'light_rail_present', 'express_bus_present')

demo_info = c('persons', 'work_status', 'student_status', 'age_group', 'gender', 'race_ethnicity',
              'eng_proficient', 'household_income', 'hh_auto_ownership')

spatial_info = c(
                 # taz/maz info will be exported to another summary table
                 # 'orig_tm1_taz', 'dest_tm1_taz', 'home_tm1_taz', 'workplace_tm1_taz', 'school_tm1_taz',
                 # 'orig_tm2_taz', 'dest_tm2_taz', 'home_tm2_taz', 'workplace_tm2_taz', 'school_tm2_taz',
                 # 'orig_tm2_maz', 'dest_tm2_maz', 'home_tm2_maz', 'workplace_tm2_maz', 'school_tm2_maz',
                 'first_board_lat', 'first_board_lon', 'last_alight_lat', 'last_alight_lon',
                 'dest_lat', 'dest_lon', 'orig_lat', 'orig_lon', 'home_lat', 'home_lon',
                 'school_lat', 'school_lon', 'workplace_lat', 'workplace_lon')

# export
export <- df[c(basic_info, trip_info, demo_info, spatial_info)]
sprintf('Export %d rows and %d columns of passenger-level data for Tableau to %s',
        nrow(export),
        ncol(export),
        F_TABLEAU_CSV)
write.csv(export, F_TABLEAU_CSV, row.names = FALSE)


######## Prepare Data for Tableau: TM1-TAZ-level ########
#########################################################

## Origin/Destination/Home/Workplace/School TM1 TAZ summaries

# TM1 TAZ - superdistrict crosswalk
TM1_taz_sd <- read.csv(file = F_TM1_TAZ_CSV, header = TRUE)[, c('SUPERD', 'TAZ1454')]
TM1_taz_sd <- TM1_taz_sd[!duplicated(TM1_taz_sd[,]), ]

sprintf('TM1 TAZ has %d zones in %d superdictricts',
        length(unique(TM1_taz_sd$TAZ1454)),
        length(unique(TM1_taz_sd$SUPERD)))

# add super district names
sd_name <- read.csv(file = F_SD_NAME_CSV, header = TRUE)[, c('number', 'name')]

TM1_taz_sd <- TM1_taz_sd %>%
  left_join(sd_name, by = c('SUPERD' = 'number')) %>%
  rename('SD_id' = 'SUPERD',
         'SD_name' = 'name')%>%
  select(TAZ1454, SD_id, SD_name)


# summarize weight and trip_weight of each TM1_TAZ
# first, fill NaN in tm1_taz fields
for (colname in c('orig_tm1_taz', 'dest_tm1_taz', 'home_tm1_taz',
                  'workplace_tm1_taz', 'school_tm1_taz')) {
  print(colname)
  print(nrow(df[which(is.na(df[colname])),]))
  df[colname][is.na(df[colname])] <- 0
  print(nrow(df[which(is.na(df[colname])),]))
}

df_groupby_orig <- df %>%
  dplyr::group_by(survey_version, operator_survey_year, operator, survey_year,
                  survey_tech, weekpart, day_part, access_egress_modes,
                  board_station, alight_station, 
                  tour_purp, boardings, race_ethnicity, household_income,
                  hh_auto_ownership, orig_tm1_taz) %>%
  dplyr::summarize(weight = sum(weight), trip_weight = sum(trip_weight), survey_cnt = n()) %>%
  rename('TM1_TAZ' = 'orig_tm1_taz',
         'weight_by_orig_taz' = 'weight',
         'trip_weight_by_orig_taz' = 'trip_weight',
         'num_surveyed_by_orig_taz' = 'survey_cnt')

sprintf('The survey data contains %d unique %s, representing %.3f of all TM1 TAZs',
        length(unique(df_groupby_orig$TM1_TAZ))-1,
        'orig_tm1_taz',
        (length(unique(df_groupby_orig$TM1_TAZ))-1)/length(unique(TM1_taz_sd$TAZ1454)))

df_groupby_dest <- df %>%
  dplyr::group_by(survey_version, operator_survey_year, operator, survey_year,
                  survey_tech, weekpart, day_part, access_egress_modes,
                  board_station, alight_station,
                  tour_purp, boardings, race_ethnicity, household_income,
                  hh_auto_ownership, dest_tm1_taz) %>%
  dplyr::summarize(weight = sum(weight), trip_weight = sum(trip_weight), survey_cnt = n()) %>%
  rename('TM1_TAZ' = 'dest_tm1_taz',
         'weight_by_dest_taz' = 'weight',
         'trip_weight_by_dest_taz' = 'trip_weight',
         'num_surveyed_by_dest_taz' = 'survey_cnt')

sprintf('The survey data contains %d unique %s, representing %.3f of all TM1 TAZs',
        length(unique(df_groupby_dest$TM1_TAZ))-1,
        'dest_tm1_taz',
        (length(unique(df_groupby_dest$TM1_TAZ))-1)/length(unique(TM1_taz_sd$TAZ1454)))

df_groupby_home <- df %>%
  dplyr::group_by(survey_version, operator_survey_year, operator, survey_year,
                  survey_tech, weekpart, day_part, access_egress_modes,
                  board_station, alight_station,
                  tour_purp, boardings, race_ethnicity, household_income,
                  hh_auto_ownership, home_tm1_taz) %>%
  dplyr::summarize(weight = sum(weight), trip_weight = sum(trip_weight), survey_cnt = n()) %>%
  rename('TM1_TAZ' = 'home_tm1_taz',
         'weight_by_home_taz' = 'weight',
         'trip_weight_by_home_taz' = 'trip_weight',
         'num_surveyed_by_home_taz' = 'survey_cnt')

sprintf('The survey data contains %d unique %s, representing %.3f of all TM1 TAZs',
        length(unique(df_groupby_home$TM1_TAZ))-1,
        'home_tm1_taz',
        (length(unique(df_groupby_home$TM1_TAZ))-1)/length(unique(TM1_taz_sd$TAZ1454)))

df_groupby_workplace <- df %>%
  dplyr::group_by(survey_version, operator_survey_year, operator, survey_year,
                  survey_tech, weekpart, day_part, access_egress_modes,
                  board_station, alight_station,
                  tour_purp, boardings, race_ethnicity, household_income,
                  hh_auto_ownership, workplace_tm1_taz) %>%
  dplyr::summarize(weight = sum(weight), trip_weight = sum(trip_weight), survey_cnt = n()) %>%
  rename('TM1_TAZ' = 'workplace_tm1_taz',
         'weight_by_work_taz' = 'weight',
         'trip_weight_by_work_taz' = 'trip_weight',
         'num_surveyed_by_work_taz' = 'survey_cnt')

sprintf('The survey data contains %d unique %s, representing %.3f of all TM1 TAZs',
        length(unique(df_groupby_workplace$TM1_TAZ))-1,
        'workplace_tm1_taz',
        (length(unique(df_groupby_workplace$TM1_TAZ))-1)/length(unique(TM1_taz_sd$TAZ1454)))

df_groupby_school <- df %>%
  dplyr::group_by(survey_version, operator_survey_year, operator, survey_year,
                  survey_tech, weekpart, day_part, access_egress_modes,
                  board_station, alight_station,
                  tour_purp, boardings, race_ethnicity, household_income,
                  hh_auto_ownership, school_tm1_taz) %>%
  dplyr::summarize(weight = sum(weight), trip_weight = sum(trip_weight), survey_cnt = n()) %>%
  rename('TM1_TAZ' = 'school_tm1_taz',
         'weight_by_school_taz' = 'weight',
         'trip_weight_by_school_taz' = 'trip_weight',
         'num_surveyed_by_school_taz' = 'survey_cnt')

sprintf('The survey data contains %d unique %s, representing %.3f of all TM1 TAZs',
        length(unique(df_groupby_school$TM1_TAZ))-1,
        'school_tm1_taz',
        (length(unique(df_groupby_school$TM1_TAZ))-1)/length(unique(TM1_taz_sd$TAZ1454)))


# merge all group_by dataframes
all_tm1_taz <- as.data.frame(df_groupby_orig) %>% 
  full_join(as.data.frame(df_groupby_dest),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'board_station', 'alight_station',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_home),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'board_station', 'alight_station',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_workplace),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'board_station', 'alight_station',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_school),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'board_station', 'alight_station',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ'))

# add TAZ-County crosswalk from ACS demographic data
demo_tm1_taz = read.csv(file = F_DEMO_TM1_TAZ_CSV, header = TRUE)
sprintf('Read race/ethnicity data on TM1 TAZ for %d TAZs and %d counties',
        length(unique(demo_tm1_taz$TAZ1454)),
        length(unique(demo_tm1_taz$County_Name)))

TM1_taz_county = demo_tm1_taz[,c('TAZ1454', 'COUNTY', 'County_Name')]
all_tm1_taz <- all_tm1_taz %>%
  left_join(TM1_taz_county,
            by = c('TM1_TAZ' = 'TAZ1454'))

# add superdistrict ID and name to the TAZ/MAZ summary
all_tm1_taz <- all_tm1_taz %>%
  left_join(TM1_taz_sd,
            by = c('TM1_TAZ' = 'TAZ1454'))

# fill NaN in taz, SD_ID, COUNTY with 0
for (colname in c('TM1_TAZ', 'SD_id', 'COUNTY')) {
  print(colname)
  print('Num of rows with NaN:')
  print(nrow(all_tm1_taz[which(is.na(all_tm1_taz[colname])),]))
  all_tm1_taz[colname][is.na(all_tm1_taz[colname])] <- 0
  print('After filling NaN, num of rows with NaN:')
  print(nrow(all_tm1_taz[which(is.na(all_tm1_taz[colname])),]))
}

# fill NaN in SD_name and County_Name with 'missing'
for (colname in c('SD_name', 'County_Name')){
  print(colname)
  print('Num of rows with NaN:')
  print(nrow(all_tm1_taz[which(is.na(all_tm1_taz[colname])),]))
  all_tm1_taz[colname][is.na(all_tm1_taz[colname])] <- 'missing'
  print('After filling NaN, num of rows with NaN:')
  print(nrow(all_tm1_taz[which(is.na(all_tm1_taz[colname])),]))}

# finally, make sure all 'trip_weight' and 'weight' fields are float,
# and survey response counts are integers
for (colname in c('weight_by_orig_taz', 'trip_weight_by_orig_taz',
                  'weight_by_dest_taz', 'trip_weight_by_dest_taz',
                  'weight_by_home_taz', 'trip_weight_by_home_taz',
                  'weight_by_work_taz', 'trip_weight_by_work_taz',
                  'weight_by_school_taz', 'trip_weight_by_school_taz',
                  'num_surveyed_by_orig_taz',
                  'num_surveyed_by_dest_taz',
                  'num_surveyed_by_home_taz',
                  'num_surveyed_by_work_taz',
                  'num_surveyed_by_school_taz')) {
  all_tm1_taz[,colname] <- as.numeric(all_tm1_taz[,colname])
  all_tm1_taz[,colname][is.na(all_tm1_taz[,colname])] <- 0
}

# check datatypes
str(all_tm1_taz)

# export
sprintf('Export %d rows and %d columns of TM1-TAZ-level data for Tableau to %s',
        nrow(all_tm1_taz),
        ncol(all_tm1_taz),
        F_TAZ_CSV)
write.csv(all_tm1_taz, F_TAZ_CSV, row.names = FALSE)


## Configure ACS race/ethnicity data for comparison
demo_tm1_taz <- demo_tm1_taz %>%
  rename('his' = 'hispanic') %>%
  transform(hispanic_pct_ACS = his/TOTPOP,
            white_nonh_pct_ACS = white_nonh/TOTPOP,
            black_nonh_pct_ACS = black_nonh/TOTPOP,
            asian_nonh_pct_ACS = asian_nonh/TOTPOP,
            other_nonh_pct_ACS = other_nonh/TOTPOP)

# add superdistrict ID and name to the TAZ/MAZ summary
demo_tm1_taz <- demo_tm1_taz %>%
  left_join(TM1_taz_sd,
            by = c('TAZ1454' = 'TAZ1454'))

# export
sprintf('Export %d rows and %d columns of TM1-TAZ-level ACS data for Tableau to %s',
        nrow(demo_tm1_taz),
        ncol(demo_tm1_taz),
        F_TAZ_DEMO_CSV)
write.csv(demo_tm1_taz, F_TAZ_DEMO_CSV, row.names = FALSE)


######## Prepare Data for Tableau: PUMS data ########
#####################################################

# load the data and keep only the needed fields
load(F_PUMS_H_RDATA)
load(F_PUMS_P_RDATA)

h_df <- hbayarea1519[, c('SERIALNO', 'HINCP', 'NP', 'VEH')]
p_df <- pbayarea1519[, c('SERIALNO', 'SPORDER', 'ESR', 'PWGTP', 'County_Name')]


## recide HH income, vehicle ownership, and employment status

# recode HH income into categories
# household income 'HINCP' (past 12 months, use ADJINC to adjust HINCP to constant dollars)
# zero income and negative income are included in 'under $25,000
h_df <- h_df %>%
  mutate(household_income = ifelse(is.na(HINCP), 'missing',
                                   ifelse(HINCP < 25000, 'under $25,000',
                                          ifelse(HINCP < 50000, '$25,000 to $50,000',
                                                 ifelse(HINCP < 100000, '$50,000 to $100,000',
                                                        ifelse(HINCP < 150000, '$100,000 to $150,000',
                                                               ifelse(HINCP >= 150000, '$150,000 or higher', 'other')))))))

# vehicle ownership 'VEH' (Vehicles (1 ton or less) available)
  # b .N/A (GQ/vacant)
  # 0 .No vehicles
  # 1 .1 vehicle
  # 2 .2 vehicles
  # 3 .3 vehicles
  # 4 .4 vehicles
  # 5 .5 vehicles
  # 6 .6 or more vehicles
  # NA is considered no vehicle (0)
h_df <- h_df %>%
  mutate(VEH = ifelse(is.na(VEH), 0, VEH))


# work status 'ESR' (Employment status recode) 
  # b .N/A (less than 16 years old)
  # 1 .Civilian employed, at work
  # 2 .Civilian employed, with a job but not at work
  # 3 .Unemployed
  # 4 .Armed forces, at work
  # 5 .Armed forces, with a job but not at work
  # 6 .Not in labor force


# recode ESR into a new binary variable 'work_status': 1 full-time/part-time workers, 0 non-worker
# NA (less than 16 years old) is coded as non-worker
p_df <- p_df %>%
  mutate('work_status' = ifelse(is.na(ESR), 0, 
                                ifelse(ESR %in% c(1, 2, 4, 5), 1, 0)))


## calculate 'auto-sufficiency' by household

# group by household SERIALNO and calculate number of workers and workers by household
p_groupby <- p_df %>% 
  dplyr::group_by(SERIALNO) %>%
  dplyr::summarize(num_workers = sum(work_status), num_persons = n())

# merge household and person data and calculate workers per household
pums_h_df <- h_df %>%
  full_join(p_groupby, by = c('SERIALNO' = 'SERIALNO'))

# QA/QC groupby and join
# first, there should be same number of NA in num_persons and num_workers
stopifnot(
  nrow(pums_h_df[which(is.na(pums_h_df$num_workers)),]) == nrow(pums_h_df[which(is.na(pums_h_df$num_persons)),]))
# second, there should be no row where num_persons != NP, with NP as an interger >= o
  # representing household size
pums_h_df[, 'num_persons'][is.na(pums_h_df[, 'num_persons'])] <- 0
pums_h_df['chk'] = pums_h_df['num_persons'] - pums_h_df['NP']
stopifnot(nrow(pums_h_df[which(pums_h_df$chk != 0),]) == 0)

# calculate household vehicle ownership categories
pums_h_df[, 'num_workers'][is.na(pums_h_df[, 'num_workers'])] <- 0

pums_h_df <- pums_h_df %>%
  mutate(hh_auto_ownership = ifelse(VEH == 0, 'zero autos',
                                    ifelse((VEH > 0) & (
                                      num_workers > 0) & (
                                        num_workers > VEH), 'autos < workers',
                                      ifelse((VEH > 0) & (
                                        num_workers >= 0) & (
                                          num_workers <= VEH), 'autos >= workers', 'other'))))


## finally, join household-level HH income and vehicle-ownership data back to
   # persons data to get person-level data with person weights
pums_p_df <- pums_h_df %>%
  right_join(p_df, by = c('SERIALNO' = 'SERIALNO')) %>%
  # create an unique ID for each person for potential QA/QC need
  mutate(p_id = paste(SERIALNO, SPORDER, sep='-')) %>%
  rename('person_weight' = 'PWGTP') %>%
  select(p_id, household_income, hh_auto_ownership, person_weight, County_Name)

# export
sprintf('Export %d rows and %d columns of PUMS person-level data for Tableau to %s',
        nrow(pums_p_df),
        ncol(pums_p_df),
        F_PUMS_HH_DEMO_CSV)
write.csv(pums_p_df, F_PUMS_HH_DEMO_CSV, row.names = FALSE)
