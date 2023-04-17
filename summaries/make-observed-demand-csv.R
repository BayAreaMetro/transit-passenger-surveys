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
survey_filename <- paste0(box_dir, "Survey_Database_090221/TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata")
output_filename <- paste0(
  box_dir, 
  "Survey_Database_090221/observed-demand-year-2015.csv"
)


# Parameters -------------------------------------------------------------------
time_period_dict_df <- tibble(
  day_part = c("EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "NIGHT"),
  model_time = c("ea", "am", "md", "pm", "ev", "ev")
)

# Methods ----------------------------------------------------------------------

# Data Reads -------------------------------------------------------------------
load(survey_filename)

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

