# Snapshot_Survey_Recode_Transit_Ride_More.r
# Recodes Snapshot Survey question about using transit more
# Uses results from the first two columns only (directions stated to pick your top two choices)
# People who picked more than two (2,089 records) were purged from this question because they picked too many choices
# Creates dummy records (0,1) for the values in Q8_1 and Q8_2 in variables (q8_rc_1-30)

# Set options to get rid of scientific notation

options(scipen = 999)

# Bring in libraries

suppressMessages(library(tidyverse))
library(readxl)
library(writexl)

# Set file directories for input and output

USERPROFILE    <- gsub("////","/", Sys.getenv("USERPROFILE"))
BOX_dir1       <- file.path(USERPROFILE, "Box", "Modeling and Surveys","Surveys","Transit Passenger Surveys")
Box_dir2       <- file.path(BOX_dir1,"Snapshot Survey","Data")
input_data     <- file.path(Box_dir2,"mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024.xlsx")
output_data    <- "M:/Data/OnBoard/Data and Reports/Snapshot Survey"

# Bring in data file

snapshot_data <- read_excel(input_data,sheet="data file")

# Filter cases where people followed directions and only indicated one or, at most, two choices

snapshot_data_filtered <- snapshot_data %>% 
  filter(is.na(Q8_3))

# Create the dummy variables for var_1 to var_30 (rc for "recode")
# The value for 10 was unused, so skip

for (i in 1:30) {
  if(i==10){next}
  var_name <- paste0("q8_rc_", i)  # Create the column name (e.g., q8_rc_1, q8_rc_2 etc.)
  
  # Add the new column with 1 if the value is in Q8_1 or Q8_2, otherwise 0
  snapshot_data_filtered[[var_name]] <- ifelse(snapshot_data_filtered$Q8_1 == i | snapshot_data_filtered$Q8_2 == i, 1, 0)
}

# Keep ccgid and new variables to append back to the main file
# Apply 0 values for NAs

joiner <- snapshot_data_filtered %>% 
  select(CCGID,71:99)
  
final <- snapshot_data %>% 
  left_join(.,joiner,by="CCGID") %>% 
  mutate(across(starts_with("q8_rc_"), ~ ifelse(is.na(.), 0, .)))

# Output file to Excel, updating name to "_recode"

write_xlsx(final,path=file.path(output_data,"mtc snapshot survey_final data file_for regional MTC only_REVISED 28 August 2024_recode.xlsx"))

# Output file to pivot_long for transit improvement variables

final2 <- final %>% 
  select(System,Strata,Daytype,Type,Q7,grep("q8_rc_",names(.)),Weight) %>% 
  pivot_longer(.,grep("q8_rc_",names(.)),names_to = "improvement_type",values_to = "dummy_value")

write_xlsx(final2,path=file.path(output_data,"mtc snapshot survey_final data pivot_long improvements.xlsx"))







