# Code Person Type.R

# Import Libraries

suppressMessages(library(tidyverse))
library(stats)

# Input survey file

userprofile   <- Sys.getenv("USERPROFILE")
BOX_TM        <- file.path(userprofile,"Box","Modeling and Surveys","Share Data","Protected Data","Joel Freedman")
SURVEY_IN <- file.path(BOX_TM,"TPS_Model_Version_PopulationSim_Weights2021-07-27.Rdata")
load (SURVEY_IN)

# Code Person Type
# Change age variable from character to numeric
# Recoded missing age as 30, putting it in the largest category (18-64)
# Assumed missing for workers meant non-work

TPS <- TPS %>% 
  mutate(age = as.numeric(approximate_age),
         age = if_else(is.na(age),30,age),
         age = if_else(age==-9,30,age)) %>%         
  
  mutate(ptype=case_when(
           student_status=="full- or part-time" & (age>=18)                ~ "3",    # College student
           work_status=="full- or part-time" & (age>=18)                   ~ "1_2",  # Full or part-time worker
           work_status %in% c("non-worker", "missing", "Missing") &
             (age>=18 & age<=64)                                           ~ "4",    # Non-working adult
           work_status %in% c("non-worker", "missing", "Missing") &
             age >= 65                                                     ~ "5",    # Non-working adult
           age %in% c(16,17)                                               ~ "6",    # Driving age student
           age >= 6 & age <= 15                                            ~ "7",    # Non-driving student
           age <= 5                                                        ~ "8",    # Pre-school
           TRUE                                                            ~ "0"     # Check that all ptypes were assigned
         ))

sum_2015_old <- old_survey %>% 
  group_by(access_concat) %>% 
  summarize(old_2015_targets_trips=sum(trip_weight2015), old_2015_targets_boardings=sum(board_weight2015))

sum_2015_new <- new_survey %>% 
  group_by(access_concat) %>% 
  summarize(new_2015_targets_trips=sum(final_tripWeight_2015), new_2015_targets_boardings=sum(final_boardWeight_2015)) 
  


