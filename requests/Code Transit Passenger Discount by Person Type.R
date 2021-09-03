# Code Transit Passenger Discount by Person Type.R
# First export files grouped by fare category and fare medium (and by "FARECATEGORYSUMMARY" for BART)
# We're looking for unique combinations of these variables to code respective discounts
# Join the values back to onboard survey file, code a person type variable
# Summarize transit fare discount by person type
# Location of spreadsheet for manual assignment of discounts is here: 
# "M:\Data\OnBoard\Bespoke\Fare Discount Model\Operator Discounts for 2015.xlsx"

# Import Libraries

suppressMessages(library(tidyverse))
library(stats)
library(readxl)

# Input standardized survey file

userprofile   <- Sys.getenv("USERPROFILE")
BOX_TM        <- file.path(userprofile,"Box","Modeling and Surveys","Share Data","Protected Data","Joel Freedman")
SURVEY_IN <- file.path(BOX_TM,"TPS_Model_Version_PopulationSim_Weights2021-09-02.Rdata")
load (SURVEY_IN)

# Set working directory for file output

wd <- "M:/Data/OnBoard/Bespoke/Fare Discount Model"
setwd(wd)

# -----------------------------------------------------------------------------------
# Begin work to create fare discount equivalency coding
# Summarize (non-BART) operators by fare_category and fare_medium for unique fare combinations
# Process BART separately because it includes an extra variable (FARECATEGORYSUMMARY) for high-value tickets
# Export BART to CSV for manual assignment of discounts

# Summarize all operators except BART and Capitol Corridor (the former done next step and the latter lacking data to code person type)
# Recode NA values to "z_unknown" so that there will be a value there for later rejoining (the "z" prefix puts it in the last category position)

TPS <- TPS %>% 
  mutate_at(.,vars(fare_category,fare_medium),
            ~if_else(is.na(.),"z_unknown",.)) %>% 
  filter(operator!="Capitol Corridor")


all_no_bart_summary <- TPS %>% 
  filter(!(operator %in% c("BART"))) %>% 
  group_by(operator, fare_category,fare_medium) %>% 
  summarize(total=n()) %>% 
  ungroup()

write.csv(all_no_bart_summary,file = "all_no_BART_fare_categories.csv",row.names = F)

# Now do the same with BART, utilizing the extra variable

dir_path           <- "M:/Data/OnBoard/Data and Reports/"

f_bart_survey_path <- paste0(dir_path,
                             "BART/As CSV/BART_Final_Database_Mar18_SUBMITTED_with_station_xy_with_first_board_last_alight_fixColname_modifyTransfer_NO POUND OR SINGLE QUOTE.csv")

BART_raw <- read.csv(f_bart_survey_path,header = T) %>% mutate(operator="BART") %>% select(
  operator,ID,FARECATEGORYSUMMARY) %>% 
  mutate(ID=as.character(ID))

# Join new variable to TPS dataset
# Summarize with the extra variable

TPS <- TPS %>% 
  left_join(.,BART_raw,by=c("operator","ID")) 
  

bart_summary <- TPS %>% filter(operator=="BART") %>% 
  group_by(operator, fare_category,fare_medium,FARECATEGORYSUMMARY) %>% 
  summarize(total=n()) %>% 
  ungroup()

# Write out to do calculations
write.csv(bart_summary,file="BART_2015_fare_categories.csv",row.names = F)

# End of discount coding text
# -----------------------------------------------------------------------------------

# Join discount data, code person type, and summarize discounts by person type
# Read in processed discount files

BART_discount       <- read_excel(file.path(wd,"Operator Discounts for 2015.xlsx"),sheet = "BART_discount_rate")
all_other_discount  <- read_excel(file.path(wd,"Operator Discounts for 2015.xlsx"),sheet = "all_other_discount_rate")

# Join discounts to main discount TPS file
# Create new variable to merge BART and all other discounts
# Select out older discount variables for final data file

TPS <- TPS %>% 
  left_join(.,BART_discount,by=c("operator","fare_category","fare_medium","FARECATEGORYSUMMARY")) %>% 
  left_join(.,all_other_discount,by=c("operator","fare_category","fare_medium")) %>% mutate(
    discount_rate=if_else(operator=="BART",BART_discount_rate,all_other_discount_rate)) %>% 
  select(-BART_discount_rate,-all_other_discount_rate) %>% 

# Hand code a few that didn't merge for some reason
  
  mutate(discount_rate=case_when(
    operator=="ACE" & ID=="1300"                      ~ 0.712195121951219,
    operator=="Vacaville City Coach" & ID=="1371"     ~ 0.1,
    operator=="FAST [LOCAL]" & ID=="1579"             ~ 0,
    operator=="Soltrans [LOCAL]" & ID=="1344"         ~ 0,
    operator=="Soltrans [LOCAL]" & ID=="1356"         ~ 0, 
    operator=="Soltrans [LOCAL]" & ID=="1365"         ~ 0.485714285714286,
    TRUE                                              ~ discount_rate))

# Code Person Type
# Change age variable from character to numeric
# Recoded missing age as 30, putting it in the largest category (18-64)
# Assumed missing for workers meant non-work

TPS <- TPS %>% 
  mutate(age = as.numeric(approximate_age),
         age = if_else(is.na(age),30,age),
         age = if_else(age==-9,30,age)) %>%    
  
# Now code person type
  
  mutate(ptype=case_when(
           student_status=="full- or part-time" & (age>=18)                ~ "3",    # College student
           work_status=="full- or part-time" & (age>=18)                   ~ "1_2",  # Full or part-time worker
           work_status %in% c("non-worker", "missing", "Missing") &
             (age>=18 & age<=64)                                           ~ "4",    # Non-working adult
           work_status %in% c("non-worker", "missing", "Missing") &
             age >= 65                                                     ~ "5",    # Non-working senior
           age %in% c(16,17)                                               ~ "6",    # Driving age student
           age >= 6 & age <= 15                                            ~ "7",    # Non-driving student
           age <= 5                                                        ~ "8",    # Pre-school
           TRUE                                                            ~ "0"     # Check that all ptypes were assigned
         )) 

# Summarize weighted average discount by person type, using boarding weight

final <- TPS %>% 
  group_by(operator,ptype) %>% 
  summarize(mean_discount=weighted.mean(discount_rate,final_boardWeight_2015)) %>% 
  ungroup()

write.csv(final,file = "Weighted Transit Fare Discount by Operator and Person Type.csv",row.names = F)
