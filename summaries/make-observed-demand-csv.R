#-------------------------------------------------------------------------------
#title: "Make Observed Demand Flatfile"
#-------------------------------------------------------------------------------

# Overhead ---------------------------------------------------------------------
packages_vector <- c("tidyverse")

need_to_install <- packages_vector[!(packages_vector %in% installed.packages()[,"Package"])]

if (length(need_to_install)) install.packages(need_to_install)

for (package in packages_vector) {
  library(package, character.only = TRUE)
}

# Remote I-O -------------------------------------------------------------------
box_dir <- "~/Box Sync/" 
github_dir <- "../../"
survey_filename <- paste0(box_dir, "Survey_Database_090221/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata")
output_filename <- paste0(
  box_dir, 
  "Survey_Database_090221/observed-demand-year-2015.csv"
)
output_01_filename <- paste0(box_dir, "Survey_Database_090221/observed-demand-year-2015-emme-taz-by-path.csv")
output_02_filename <- paste0(box_dir, "Survey_Database_090221/observed-demand-am-year-2015-emme-taz-by-path.csv")
output_03_filename <- paste0(box_dir, "Survey_Database_090221/observed-demand-ea-year-2015-emme-taz-by-path.csv")

# TODO: need to update to more stable path
tm2_zone_system_filename <- paste0(github_dir, "tm2py/examples/temp_acceptance/inputs/landuse/taz_data.csv") 
maz_filename <- paste0(github_dir, "tm2py/examples/temp_acceptance/inputs/landuse/maz_data.csv") 


# Parameters -------------------------------------------------------------------
time_period_dict_df <- tibble(
  day_part = c("EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "NIGHT"),
  model_time = c("ea", "am", "md", "pm", "ev", "ev")
)

# Methods ----------------------------------------------------------------------

# Data Reads -------------------------------------------------------------------
load(survey_filename) 

taz_df <- read_csv(tm2_zone_system_filename, col_types = cols(
  TAZ = col_integer(),
  index = col_double(),
  TAZ_ORIGINAL = col_integer(),
  AVGTTS = col_double(),
  DIST = col_double(),
  PCTDETOUR = col_double(),
  TERMINALTIME = col_double()
))

maz_df <- read_csv(maz_filename, col_types = cols(
  MAZ_ORIGINAL = col_integer(),
  TAZ_ORIGINAL = col_integer(),
  DistID = col_double(),
  DistName = col_character(),
  CountyID = col_double(),
  CountyName = col_character(),
  ACRES = col_double(),
  HH = col_double(),
  POP = col_double(),
  ag = col_double(),
  art_rec = col_double(),
  constr = col_double(),
  eat = col_double(),
  ed_high = col_double(),
  ed_k12 = col_double(),
  ed_oth = col_double(),
  fire = col_double(),
  gov = col_double(),
  health = col_double(),
  hotel = col_double(),
  info = col_double(),
  lease = col_double(),
  logis = col_double(),
  man_bio = col_double(),
  man_lgt = col_double(),
  man_hvy = col_double(),
  man_tech = col_double(),
  natres = col_double(),
  prof = col_double(),
  ret_loc = col_double(),
  ret_reg = col_double(),
  serv_bus = col_double(),
  serv_pers = col_double(),
  serv_soc = col_double(),
  transp = col_double(),
  util = col_double(),
  emp_total = col_double(),
  publicEnrollGradeKto8 = col_double(),
  privateEnrollGradeKto8 = col_double(),
  publicEnrollGrade9to12 = col_double(),
  privateEnrollGrade9to12 = col_double(),
  comm_coll_enroll = col_double(),
  EnrollGradeKto8 = col_double(),
  EnrollGrade9to12 = col_double(),
  collegeEnroll = col_double(),
  otherCollegeEnroll = col_double(),
  AdultSchEnrl = col_double(),
  hstallsoth = col_double(),
  hstallssam = col_double(),
  dstallsoth = col_double(),
  dstallssam = col_double(),
  mstallsoth = col_double(),
  mstallssam = col_double(),
  park_area = col_double(),
  hparkcost = col_double(),
  numfreehrs = col_double(),
  dparkcost = col_double(),
  mparkcost = col_double(),
  ech_dist = col_double(),
  hch_dist = col_double(),
  parkarea = col_double(),
  TERMINAL = col_double()
))

# Reductions -------------------------------------------------------------------

# SamTrans records are missing TAZ codes, use MAZ and update
output_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  select(-orig_tm2_taz, -dest_tm2_taz) %>%
  left_join(., select(maz_df, orig_tm2_taz = TAZ_ORIGINAL, orig_tm2_maz = MAZ_ORIGINAL), by = c("orig_tm2_maz")) %>%
  left_join(., select(maz_df, dest_tm2_taz = TAZ_ORIGINAL, dest_tm2_maz = MAZ_ORIGINAL), by = c("dest_tm2_maz")) %>%
  filter(!is.na(orig_tm2_taz)) %>%
  filter(!is.na(dest_tm2_taz)) %>%
  filter(!is.na(model_time)) %>%
  group_by(model_time, access_mode_model, egress_mode_model, orig_tm2_taz, dest_tm2_taz) %>%
  summarise(trips = sum(final_tripWeight_2015), .groups = "drop")

sum(output_df$trips)

# Write ------------------------------------------------------------------------
write_csv(output_df, output_filename)


# Variations -------------------------------------------------------------------
taz_dict_df <- select(taz_df, external_taz = TAZ_ORIGINAL, emme_taz = TAZ)

## Variation 1: Emme TAZ numbers, aggregate over TAZs by path and time-of-day --
working_df <- output_df %>%
  left_join(., 
            select(taz_dict_df, orig_tm2_taz = external_taz, emme_taz), 
            by = c("orig_tm2_taz")) %>%
  rename(orig_emme_taz = emme_taz) %>%
  left_join(., 
            select(taz_dict_df, dest_tm2_taz = external_taz, emme_taz), 
            by = c("dest_tm2_taz")) %>%
  rename(dest_emme_taz = emme_taz) %>%
  select(model_time, 
         access_mode_model, 
         egress_mode_model, 
         orig_emme_taz, 
         dest_emme_taz, 
         trips) %>%
  mutate(path_type = if_else(access_mode_model == "walk" & egress_mode_model == "walk", "wlk_trn_wlk", "drive-to-drive")) %>%
  mutate(path_type = if_else(access_mode_model == "pnr" & egress_mode_model == "walk", "pnr_trn_wlk", path_type)) %>%
  mutate(path_type = if_else(access_mode_model == "knr" & egress_mode_model == "walk", "knr_trn_wlk", path_type)) %>%
  mutate(path_type = if_else(access_mode_model == "tnc" & egress_mode_model == "walk", "knr_trn_wlk", path_type)) %>%
  mutate(path_type = if_else(access_mode_model == "walk" & egress_mode_model == "pnr", "wlk_trn_pnr", path_type)) %>%
  mutate(path_type = if_else(access_mode_model == "walk" & egress_mode_model == "knr", "wlk_trn_knr", path_type)) %>%
  mutate(path_type = if_else(access_mode_model == "walk" & egress_mode_model == "tnc", "wlk_trn_knr", path_type)) %>%
  filter(path_type != "drive-to-drive")

output_01_df <- working_df %>%
  group_by(model_time, path_type, orig_emme_taz, dest_emme_taz) %>%
  summarise(trips = sum(trips), .groups = "drop")

sum(output_01_df$trips)

write_csv(output_01_df, output_01_filename)

## Variation 2: Same as 1 for AM period
output_02_df <- output_01_df %>%
  filter(model_time == "am")

sum(output_02_df$trips)

sum(filter(output_02_df, orig_emme_taz < 637 & dest_emme_taz < 637)$trips)

write_csv(output_02_df, output_02_filename)

## Variation 3: Get transfer rate
transfer_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  filter(!is.na(orig_tm2_taz)) %>%
  filter(!is.na(dest_tm2_taz)) %>%
  group_by(model_time, access_mode_model, egress_mode_model, orig_tm2_taz, dest_tm2_taz) %>%
  summarise(trips = sum(final_tripWeight_2015),
            boardings = sum(final_boardWeight_2015),
            .groups = "drop") %>%
  filter(model_time == "am")

sum(transfer_df$boardings) / sum(transfer_df$trips)

## Variation 4: Same as 1 for EA period
output_03_df <- output_01_df %>%
  filter(model_time == "ea")

sum(output_03_df$trips)

write_csv(output_03_df, output_03_filename)

## Debug: Operator Distribution for Marin-SF or SF-Marin Movements
debug_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  filter(!is.na(orig_tm2_taz)) %>%
  filter(!is.na(dest_tm2_taz)) %>%
  mutate(orig_in_sf = orig_tm2_taz < 100000) %>%
  mutate(orig_in_marin = orig_tm2_taz > 800000) %>%
  mutate(dest_in_sf = dest_tm2_taz < 100000) %>%
  mutate(dest_in_marin = dest_tm2_taz > 800000) %>%
  filter((orig_in_sf & dest_in_marin) | (orig_in_marin & dest_in_sf)) %>%
  filter(model_time == "am") %>%
  group_by(operator) %>%
  summarise(trips = sum(final_tripWeight_2015), .groups = "drop")

debug_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  filter(!is.na(orig_tm2_taz)) %>%
  filter(!is.na(dest_tm2_taz)) %>%
  filter(model_time == "am") %>%
  group_by(operator, survey_year) %>%
  summarise(trips = sum(final_tripWeight_2015), .groups = "drop")




