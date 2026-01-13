# Summarize ACS Vehicle Availability for 2021.R

# Import Libraries

suppressMessages(library(tidyverse))
library(tidycensus)
library(dplyr)

# Load ACS tables for inspection, set up directories, install census key, set ACS year

ACS_table <- load_variables(year=2019, dataset="acs1", cache=TRUE)

output               <- "M:\Data\OnBoard\Data and Reports\_data Standardized\share_data\public_version"


censuskey            <- readLines("M:/Data/Census/API/api-key.txt")
census_api_key(censuskey, install = TRUE, overwrite = TRUE)
baycounties          <- c("01","13","41","55","75","81","85","95","97")
ACS_table            <- c(Total_         ="B08201_001",
                          Zero_Vehicle_  ="B08201_002",
                          One_Vehicle_   ="B08201_003",
                          Two_Vehicle_   ="B08201_004",
                          Three_Vehicle_ ="B08201_005",
                          Four_p_Vehicle_="B08201_006")

ACS_year            <- 2019
state_code          <- "06"

# Make ACS call, remove margins of error, rename variables, sort by county

acs_cars <- get_acs(geography = "county", variables = ACS_table,
                      state = state_code, county=baycounties,
                      year=ACS_year,
                      output="wide",
                      survey = "acs1",
                      key = censuskey) %>% 
  mutate(County=str_replace(NAME," County, California","")) %>%
  select(County, Total_E, Zero_Vehicle_E, One_Vehicle_E, Two_Vehicle_E,Three_Vehicle_E,Four_p_Vehicle_E)  %>% 
  summarize(County = "Bay Area Car Total", Total_E = sum(Total_E), Zero_Vehicles = sum(Zero_Vehicle_E), 
            One_Vehicle = sum(One_Vehicle_E), Two_Vehicles = sum(Two_Vehicle_E), Three_Vehicles = sum(Three_Vehicle_E),
            Four_Vehicles = sum(Four_p_Vehicle_E))

# Output file

write.csv(acs_cars,file.path(output,"ACS 2019 Vehicle Availability.csv"),row.names = F)

###income section

# import income table
income_table <- get_acs(
  geography = "county",
  state = state_code, county=baycounties,
  table = c("B19001"),
  survey = "acs1",
  year = 2019,
  output = "wide"
)

#combining columns to our income groups
income_table <- income_table %>%
  mutate(Under_25K = B19001_002E + B19001_003E + B19001_004E + B19001_005E,
         Under_50K = B19001_006E + B19001_007E + B19001_008E + B19001_009E + B19001_010E,
         Under_75K = B19001_011E + B19001_012E,
         Under_100K = B19001_013E,
         Under_150K = B19001_014E + B19001_015E,
         Over_150K = B19001_016E + B19001_017E) %>% 
  select(GEOID, NAME, Under_25K, Under_50K, Under_75K, Under_100K, Under_150K, Over_150K) %>% 
  summarise(NAME = "Bay Area Income Total", Under_25K = sum(Under_25K), Under_50K = sum(Under_50K), Under_75K = sum(Under_75K), 
            Under_100K = sum(Under_100K), Under_150K = sum(Under_150K), Over_150K = sum(Over_150K))

#resulting table will be a single line
# Output file

write.csv(income_table,file.path(output,"ACS 2019 Income Distribution.csv"),row.names = F)

###gender section
# import gender table
gender_table <- get_acs(
  geography = "county",
  state = state_code, county=baycounties,
  table = c("C05003"),
  survey = "acs1",
  year = 2019,
  output = "wide"
)

#combining columns to our gender groups
gender_table <- gender_table %>%
  mutate(Total = C05003_001E,
         Male = C05003_002E,
         Female = C05003_009E) %>% 
  select(GEOID, NAME, Total, Male, Female) %>% 
  summarise(NAME = "Bay Area Gender Total", Total = sum(Total), Male = sum(Male), Female = sum(Female))

#resulting table will be a single line
# Output file

write.csv(gender_table,file.path(output,"ACS 2019 Gender Distribution.csv"),row.names = F)

### race

# import race table
race_table <- get_acs(
  geography = "county",
  state = state_code, county=baycounties,
  table = c("C03002"),
  survey = "acs1",
  year = 2019,
  output = "wide"
)

#combining columns to our racial groups
race_table <- race_table %>%
  mutate(Hispanic = C03002_012E,
         White = C03002_003E,
         Black = C03002_004E,
         Asian = C03002_006E,
         Other = C03002_005E + C03002_007E + C03002_008E + C03002_009E) %>% 
  select(GEOID, NAME, Hispanic, White, Black, Asian, Other) %>% 
  summarise(NAME = "Bay Area Race Total", Hispanic = sum(Hispanic), White = sum(White), Black = sum(Black), 
            Asian = sum(Asian), Other = sum(Other)) %>%
  pivot_longer(names_to = "Race / Ethnicity", values_to = "Weight")

# Output file

write.csv(race_table,file.path(output,"ACS 2019 Race Distribution.csv"),row.names = F)

