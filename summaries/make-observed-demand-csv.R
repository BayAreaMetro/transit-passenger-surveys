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

# TODO: need to update to more stable path
tm2_zone_system_filename <- paste0(github_dir, "tm2py/examples/temp_acceptance/inputs/landuse/taz_data.csv") 


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

# Reductions -------------------------------------------------------------------
output_df <- TPS %>%
  filter(weekpart != "WEEKEND") %>%
  left_join(., time_period_dict_df, by = c("day_part")) %>%
  filter(!is.na(orig_tm2_taz)) %>%
  filter(!is.na(dest_tm2_taz)) %>%
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
