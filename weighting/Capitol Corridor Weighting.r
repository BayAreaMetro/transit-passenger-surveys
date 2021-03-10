# Capitol Corridor Weighting.r
# SI

'#-------------------------------------------

This script creates weights for Capitol Corridor data collected by others (Corey, Canapary & Galanis).
The data file includes both weekend and weekday data, but the weekend data are set to zero and not weighted here.
We generally do not work with weighted weekend data, so there is likely no need to set a precedent here.

1. Average weekday ridership data from FY 2019 (~5,762 average daily passengers).
2. The 2019 annual distribution for station-to-station boardings comes from a FY 2019 matrix (both weekday/weekend) coming from Capital Corridor.
3. The ratio of weekday/yearly boardings is applied to #2 above to get average daily (proxy for average weekday) station-to-station riders.
4. The average daily ridership is divided by the number of surveys in each boarding-to-alighting pair to calculate the weight for that station-to station combination.
5. Station boardings with an unknown station within Oakland are assigned to Jack London and those unknown in Santa Clara were assigned to Great America (both based on higher likelihood).
6. A weighted average weight was applied to survey records missing a boarding and/or alighting station location.
7. All survey records weights are summed and compared to the average weekday ridership from #1, above. A correction factor is then applied to result in the precise total.

'#-------------------------------------------


# Set working directory

wd <- "M:/Data/OnBoard/Data and Reports/Capitol Corridor/OD Survey 2019"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set up input and output directories

CAPCO_data_in   <- file.path(wd,"As CSV","CAPCO19 Data-For MTC_NO POUND OR SINGLE QUOTE.csv")
ridership_in    <- file.path(wd,"Weighting","MTC_2019_Summary_Capitol_Corridor_Station_Summary.xlsx")
write_directory <- file.path(wd,"Weighting","Capitol_Corridor_Weights.csv")

# Bring in data and shunt to data frames

capco             <- read.csv (file=CAPCO_data_in,stringsAsFactors = FALSE) %>% 
  mutate_at(.,vars(BOARD,ALIGHT),~str_trim(.))                            # Remove outside whitespace for later joining
avg_ridership     <- read_excel (ridership_in,sheet="Average_Ridership")
station_ridership <- read_excel (ridership_in,sheet="Station_to_Station") 

# Make NA values 0, convert matrix format to list, rename variables, and recode station names into those that match the survey

station_ridership_rc <- station_ridership %>% 
  gather(.,alighting,ridership,-Origin,na.rm = FALSE) %>% 
  rename(.,boarding_station=Origin,alighting_station=alighting) %>% 
  mutate(ridership=if_else(is.na(ridership),0,ridership)) %>%
  mutate_at(.,vars(boarding_station,alighting_station),~recode(.,
          "ARN"=                         "AUBURN",
          "BKY"=                         "BERKELEY",
          "DAV"=                         "DAVIS",
          "EMY"=                         "EMERYVILLE",
          "FFV"=                         "FAIRFIELD-VACAVILLE",
          "FMT"=                         "FREMONT-CENTERVILLE",
          "GAC"=                         "SANTA CLARA-GREAT AMERICA",
          "HAY"=                         "HAYWARD",
          "MTZ"=                         "MARTINEZ",
          "OAC"=                         "OAKLAND COLISEUM",
          "OKJ"=                         "OAKLAND-JLS",
          "RIC"=                         "RICHMOND",
          "RLN"=                         "ROCKLIN",
          "RSV"=                         "ROSEVILLE",
          "SAC"=                         "SACRAMENTO",
          "SCC"=                         "SANTA CLARA-NIVERSITY",    #Intentionally misspelled to match joining dataset
          "SJC"=                         "SAN JOSE",
          "SUI"=                         "FAIRFIELD-SUISUN"))


          
# Get average weekday total and yearly sum into vectors, calculate adjustment factor

average_weekday <- avg_ridership %>% 
  filter(Fiscal_Year==2019) %>% 
  .$Weekday

yearly_sum=sum(station_ridership_rc$ridership)

adj_factor=average_weekday/yearly_sum

# Now apply adjustment factor to station-to-station pairs

station_ridership_rc <- station_ridership_rc %>% 
  mutate(avg_weekday=ridership*adj_factor)

# Sum up station combinations in survey after renaming variables, recoding unknown Oakland station to Jack London and unknown Santa Clara station to Great America
# These stations have the highest ridership in their respective cities

station_sum <- capco %>% 
  rename(boarding_station=BOARD,alighting_station=ALIGHT) %>% 
  mutate(boarding_station=recode(boarding_station,"OAKLAND (UNSPECIFIED)"="OAKLAND-JLS","SANTA CLARA (UNSPECIFIED)"="SANTA CLARA-GREAT AMERICA")) %>% 
  filter(PERIOD==1) %>%                              # Weekday only
  group_by(boarding_station,alighting_station) %>% 
  summarize(num_surveys=n())

# Now join ridership to summed survey file to divide ridership by number of survey records

joined <- left_join(station_sum,station_ridership_rc,by=c("boarding_station","alighting_station")) %>% 
  mutate(weight=avg_weekday/num_surveys) 

# Calculate a weighted mean for of the weights to apply to surveys missing a boarding station, alighting station, or both

weighted_mean <- weighted.mean(joined$weight,joined$num_surveys, na.rm = TRUE)

interim_final_weights <- joined %>% 
  mutate(weight=if_else(is.na(weight),weighted_mean,weight)) %>% 
  select(boarding_station,alighting_station,weight)

interim_final_capco <- capco %>% 
  mutate(boarding_station=BOARD,
         alighting_station=ALIGHT,
         boarding_station=recode(BOARD,"OAKLAND (UNSPECIFIED)"="OAKLAND-JLS","SANTA CLARA (UNSPECIFIED)"="SANTA CLARA-GREAT AMERICA")) # Create new variables for joining
                                                                                                                                       # Impute specific Oakland and SC stations where missing
temp <- left_join(interim_final_capco,interim_final_weights,by=c("boarding_station","alighting_station")) %>% 
  mutate(weight=if_else(PERIOD==2,0,weight)) %>% 
  select(-boarding_station,-alighting_station)

# Final scaling to bring dataset up to average weekday ridership, apply to final dataset

scalar <- average_weekday/sum(temp$weight)

final <- temp %>% 
  mutate(weight=weight*scalar) %>% 
  select(ID,weight)

write.csv(final,write_directory,row.names = FALSE)
