# Visualize Select Link for Bay Bridge Transit Applications.R

# Import Library

suppressMessages(library(tidyverse))

# Input survey file

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-06-09.RData"
OUTPUT = "M:/Data/Requests/Lisa Zorn/TPS Bay Bridge Income and Race/"
load (TPS_SURVEY_IN)

# Bring in select link files and concatenate all combinations with vol_pax > 0

directory <- "M:/Application/Model One/RTP2021/IncrementalProgress/2015_TM152_IPA_16/OUTPUT/BayBridge_and_transit/"

EA_West <- paste0(directory,"loadEA_selectlink_2783-6972_ODs_v2.csv")  # Early AM, Westbound
AM_West <- paste0(directory,"loadAM_selectlink_2783-6972_ODs_v2.csv")
MD_West <- paste0(directory,"loadMD_selectlink_2783-6972_ODs_v2.csv")
PM_West <- paste0(directory,"loadPM_selectlink_2783-6972_ODs_v2.csv")
EV_West <- paste0(directory,"loadEV_selectlink_2783-6972_ODs_v2.csv")
  
EA_East <- paste0(directory,"loadEA_selectlink_6973-2784_ODs_v2.csv")
AM_East <- paste0(directory,"loadAM_selectlink_6973-2784_ODs_v2.csv")
MD_East <- paste0(directory,"loadMD_selectlink_6973-2784_ODs_v2.csv")
PM_East <- paste0(directory,"loadPM_selectlink_6973-2784_ODs_v2.csv")
EV_East <- paste0(directory,"loadEV_selectlink_6973-2784_ODs_v2.csv")

EA_WB <- read.csv(EA_West,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Early AM", direction="Westbound")  # Early AM, Westbound
AM_WB <- read.csv(AM_West,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="AM Peak", direction="Westbound")
MD_WB <- read.csv(MD_West,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Midday", direction="Westbound")
PM_WB <- read.csv(PM_West,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="PM Peak", direction="Westbound")
EV_WB <- read.csv(EV_West,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Evening", direction="Westbound")

EA_EB <- read.csv(EA_East,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Early AM", direction="Eastbound")
AM_EB <- read.csv(AM_East,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="AM Peak", direction="Eastbound")
MD_EB <- read.csv(MD_East,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Midday", direction="Eastbound")
PM_EB <- read.csv(PM_East,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="PM Peak", direction="Eastbound")
EV_EB <- read.csv(EV_East,header = TRUE) %>% select(OTAZ,DTAZ,vol_pax) %>% mutate (period="Evening", direction="Eastbound")

all_travel <- bind_rows(EA_WB,AM_WB,MD_WB,PM_WB,EV_WB,EA_EB,AM_EB,MD_EB,PM_EB,EV_EB) %>% 
  filter(vol_pax>0) %>% 
  arrange(direction, period,OTAZ,DTAZ)

write.csv(all_travel, paste0(OUTPUT, "All Select Link Files Concatenated.csv"), row.names = FALSE, quote = T)


 