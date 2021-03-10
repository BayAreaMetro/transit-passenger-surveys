# Capitol Corridor Weighting.r
# SI

# Set working directory

wd <- "M:/Data/OnBoard/Data and Reports/Capitol Corridor/OD Survey 2019"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))
library(readxl)

# Set up input and output directories

CAPCO_data_in <- file.path(wd,"As CSV","CAPCO19 Data-For MTC_NO POUND OR SINGLE QUOTE.csv")
ridership_in  <- file.path(wd,"Weighting","MTC_Summary_Capitol_Corridor_Station_Summary.xlsx")

# Bring in data and shunt to data frames

capco             <- read.csv (file=CAPCO_data_in,stringsAsFactors = FALSE)
avg_ridership     <- read_excel (ridership_in,sheet="Average_Ridership")
station_ridership <- read_excel (ridership_in,sheet="Station_to_Station") 

# Make NA values 0, convert matrix format to list, and recode station names into those that match the survey

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
  mutate(ridership_adj=ridership*adj_factor)

# Sum up station combinations in survey

station_sum <- capco %>% 
  filter(PERIOD==1) %>% 
  group_by(BOARD,ALIGHT) %>% 
  summarize(count=n())
          
          ))
  
  mutate_at(.,vars(boarding_station,alighting_station),~case_when(
    .=="ARN"                         ~ "AUBURN",
    .=="BKY"                         ~ "BERKELEY",
    .=="DAV"                         ~ "DAVIS",
    .=="EMY"                         ~ "EMERYVILLE",
    .=="FFV"                         ~ "FAIRFIELD-VACAVILLE",
    .=="FMT"                         ~ "FREMONT-CENTERVILLE",
    .=="GAC"                         ~ "SANTA CLARA-GREAT AMERICA",
    .=="HAY"                         ~ "HAYWARD",
    .=="MTZ"                         ~ "MARTINEZ",
    .=="OAC"                         ~ "OAKLAND COLISEUM",
    .=="OKJ"                         ~ "OAKLAND-JLS",
    .=="RIC"                         ~ "RICHMOND",
    .=="RLN"                         ~ "ROCKLIN",
    .=="SAC"                         ~ "SACRAMENTO",
    .=="SCC"                         ~ "SANTA CLARA-NIVERSITY",    #Intentionally misspelled to match joining dataset
    .=="SJC"                         ~ "",
    .=="SUI"                         ~ "",
    TRUE                             ~ .
  ))


# Write out final CSV files

write.csv(final,"Big7_Auto_Sufficiency 052920.csv",row.names = FALSE)


# From http://stackoverflow.com/questions/1181060
stocks <- tibble(
  time = as.Date('2009-01-01') + 0:9,
  X = rnorm(10, 0, 1),
  Y = rnorm(10, 0, 2),
  Z = rnorm(10, 0, 4)
)

gather(stocks, "stock", "price", -time)
stocks %>% gather("stock", "price", -time)





