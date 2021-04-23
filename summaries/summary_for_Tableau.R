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
F_INPUT_STANDARD_CSV = 'M:/Data/OnBoard/Data and Reports/_data Standardized/survey_standard_2021-04-22.csv'
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

# Output
F_COMBINED_CSV = 'M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-04-22.csv'

D_OUTPUT_TABLEAU = "M:/Data/OnBoard/Data and Reports/_data Standardized/tableau"
F_TABLEAU_CSV  = paste0(D_OUTPUT_TABLEAU, '/for_tableau_all_survey_by_passenger.csv')
F_TAZ_CSV      = paste0(D_OUTPUT_TABLEAU, '/for_tableau_all_survey_by_TM1_TAZ.csv')
F_TAZ_DEMO_CSV = paste0(D_OUTPUT_TABLEAU, '/for_tableau_ACS_by_TM1_TAZ.csv')

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
                           'Petaluma'                       = 'Petaluma Transit'))

# export combined data
sprintf('Export %d rows and %d columns of legacy-standard combined data to %s',
        nrow(data.ready),
        ncol(data.ready),
        F_COMBINED_CSV)
write.csv(data.ready, F_COMBINED_CSV, row.names = FALSE)


######## Prepare Data for Tableau: Surveyed-passenger-level ########
####################################################################

df <- data.ready

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
                   df$operator_survey_year == 'Union City Transit - 2013') ] <- 'old'

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
               df[colname] == 'refused')| is.na(df[colname])] <- 'missing'
  missing_cnt = nrow(df[which(df[colname] == 'missing'),])
  info <- sprintf('%s missing data in %d rows, %.2f of total', colname, missing_cnt, missing_cnt/tot_cnt)
  print(eval(info))
  print(table(df[colname]))
  }


## summarize access and egress modes
df$access_egress_modes <- paste0(df$access_mode, '-', df$egress_mode)

df <- df %>%
  mutate(access_egress_modes = recode(access_egress_modes,
                                      'walk-walk'       = 'walk at both ends',
                                      'bike-bike'       = 'bike at both ends',
                                      'pnr-pnr'         = 'pnr at both ends',
                                      'knr-knr'         = 'knr at both ends',
                                      'tnc-tnc'         = 'tnc at both ends',
                                      
                                      'bike-walk'       = 'bike at one end',
                                      'walk-bike'       = 'bike at one end',
                                      'pnr-walk'        = 'pnr at one end',
                                      'walk-pnr'        = 'pnr at one end',
                                      'knr-walk'        = 'knr at one end',
                                      'walk-knr'        = 'knr at one end',
                                      'tnc-walk'        = 'tnc at one end',
                                      'walk-tnc'        = 'tnc at one end',
                                      'bike-pnr'        = 'pnr at one end',
                                      'pnr-bike'        = 'pnr at one end',
                                      'bike-knr'        = 'knr at one end',
                                      'knr-bike'        = 'knr at one end',
                                      'tnc-bike'        = 'tnc at one end',
                                      'bike-tnc'        = 'tnc at one end',
                                      
                                      'knr-other'       = 'knr at one end',
                                      'other-pnr'       = 'pnr at one end',
                                      'other-knr'       = 'knr at one end',
                                      'other-bike'      = 'bike at one end',
                                      
                                      'knr-pnr'         = 'pnr and knr',
                                      'pnr-knr'         = 'pnr and knr',
                                      'pnr-tnc'         = 'pnr and tnc',
                                      'tnc-pnr'         = 'pnr and tnc',
                                      'knr-tnc'         = 'knr and tnc',
                                      'tnc-knr'         = 'knr and tnc',
                                      
                                      'other-walk'      = 'other',
                                      'walk-other'      = 'other',
                                      'other-other'     = 'other',
                                      
                                      'missing-missing' = 'missing at least one end',
                                      'walk-missing'    = 'missing at least one end',
                                      'missing-walk'    = 'missing at least one end',
                                      'bike-missing'    = 'missing at least one end',
                                      'pnr-missing'     = 'missing at least one end',
                                      'missing-pnr'     = 'missing at least one end',
                                      'knr-missing'     = 'missing at least one end',
                                      'missing-knr'     = 'missing at least one end',
                                      'tnc-missing'     = 'missing at least one end',
                                      'missing-tnc'     = 'missing at least one end',
                                      'missing-other'   = 'missing at least one end'))

print('Stats of aggregated access_egress_modes:')
print(count(df, access_egress_modes))


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

# combine 'evening' (standard data) with 'night' (legacy data) for variable 'day_part'
df <- df %>%
  mutate(day_part = recode(day_part,
                           'NIGHT' = 'EVENING'))


## export needed fields for Tableau
basic_info = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
               'survey_tech', 'weekpart', 'day_part', 'trip_weight', 'weight')

trip_info = c('access_egress_modes', 'access_mode', 'egress_mode', 'tour_purp', 'boardings',
              'fare_category_summary', 'fare_medium_summary',
              'commuter_rail_present', 'heavy_rail_present', 'ferry_present',
              'light_rail_present', 'express_bus_present')

demo_info = c('persons', 'work_status', 'student_status', 'age_group', 'gender', 'race_ethnicity',
              'eng_proficient', 'household_income', 'hh_auto_ownership')

spatial_info = c(
                 # taz/maz info will be exported to another summary table
                 # 'orig_tm1_taz', 'dest_tm1_taz', 'home_tm1_taz', 'workplace_tm1_taz', 'school_tm1_taz',
                 # 'orig_tm2_taz', 'dest_tm2_taz', 'home_tm2_taz', 'workplace_tm2_taz', 'school_tm2_taz',
                 # 'orig_tm2_maz', 'dest_tm2_maz', 'home_tm2_maz', 'workplace_tm2_maz', 'school_tm2_maz',
                 'first_board_lat', 'first_board_lon', 'last_alight_lat', 'last_alight_lon')

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
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_home),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_workplace),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
                   'tour_purp', 'boardings', 'race_ethnicity', 'household_income',
                   'hh_auto_ownership', 'TM1_TAZ')) %>%
  full_join(as.data.frame(df_groupby_school),
            by = c('survey_version', 'operator_survey_year', 'operator', 'survey_year',
                   'survey_tech', 'weekpart', 'day_part', 'access_egress_modes',
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
}

# check datatypes
str(all_tm1_taz)

# export
sprintf('Export %d rows and %d columns of TM1-TAZ-level data for Tableau to %s',
        nrow(all_tm1_taz),
        ncol(all_tm1_taz),
        F_TAZ_CSV)
write.csv(all_tm1_taz, F_TAZ_CSV, row.names = FALSE)


## Configure ACS demographic data for comparison
demo_tm1_taz <- demo_tm1_taz %>%
  transform(hispanic_pct_ACS = hispanic/TOTPOP,
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
