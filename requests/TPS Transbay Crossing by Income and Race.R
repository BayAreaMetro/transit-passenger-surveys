# TPS Transbay Crossing by Income and Race.R
# Analyze BART and Transbay bus riders (Bay Bridge) for income and race

# Import Library

suppressMessages(library(tidyverse))

# Input survey file

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/model_version/TPS_Model_Version_PopulationSim_Weights2021-06-09.Rdata"
OUTPUT = "M:/Data/Requests/Lisa Zorn/"
load (TPS_SURVEY_IN)

# Subset transbay operators as a first pass

BB_Operators <- TPS %>% filter(operator %in% c("AC Transit [EXPRESS]", "BART", "SF Bay Ferry/WETA", 
                                               "WestCAT [EXPRESS]"))

East_Bay_BART <- c("12th St. Oakland City Center", "19th St. Oakland", 
                   "Ashby", "Bay Fair", "Castro Valley", 
                   "Coliseum", "Concord","Downtown Berkeley", 
                   "Dublin/Pleasanton", "El Cerrito del Norte", 
                   "El Cerrito Plaza", "Fremont", "Fruitvale", 
                   "Hayward", "Lafayette", "Lake Merritt", "MacArthur", 
                   "North Berkeley", "North Concord/Martinez", 
                   "Oakland International Airport", "Orinda", "Pittsburg/Bay Point", 
                   "Pleasant Hill/Contra Costa Centre",  "Richmond", 
                   "Rockridge", "San Leandro","South Hayward", 
                   "Union City", "Walnut Creek", "West Dublin/Pleasanton", 
                   "West Oakland")

West_Bay_BART <- c("16th St. Mission","24th St. Mission", "Balboa Park",
                   "Civic Center/UN Plaza",  "Colma",  "Daly City","Embarcadero",
                   "Glen Park", "Millbrae", "Montgomery St.", "Powell St.", "San Bruno", 
                   "San Francisco Intl Airport", "South San Francisco")

Transbay_Routes <- c("AC TRANSIT___B Lakeshore Ave Oakland", "AC TRANSIT___C Highland Ave Piedmont", 
                     "AC TRANSIT___CB Warren Freeway and Broadway Terr Oakland", "AC TRANSIT___E Caldecott Ln Oakland", 
                     "AC TRANSIT___F UC Campus Berkeley", "AC TRANSIT___FS Solano Ave Berkeley", 
                     "AC TRANSIT___G Richmond St & Potrero St El Cerrito", "AC TRANSIT___H Barrett Ave & San Pablo Ave El Cerrito", 
                     "AC TRANSIT___J Sacramento St and University Ave Berkeley", "AC TRANSIT___L San Pablo Dam Rd", 
                     "AC TRANSIT___LA Hilltop Dr Park & Ride", "AC TRANSIT___NL Eastmont Transit Center Oakland", 
                     "AC TRANSIT___NX Seminary Ave & MacArthur Blvd", "AC TRANSIT___NX1 Fruitvale Ave & MacArthur Blvd", 
                     "AC TRANSIT___NX2 High St & MacArthur Blvd", "AC TRANSIT___NX3 Marlow Dr & Foothill Way Oakland", 
                     "AC TRANSIT___NX4 Castro Valley Park & Ride", "AC TRANSIT___O Park Ave & Encinal Ave Alameda", 
                     "AC TRANSIT___OX San Francisco Alameda Bay Farm Is","AC TRANSIT___P Highland Ave & Highland Way Piedmont", 
                     "AC TRANSIT___S Eden Shores Hayward", "AC TRANSIT___SB Cedar Blvd & Stevenson Blvd Newark", 
                     "AC TRANSIT___V Broadway and Broadway Terr Oakland", "AC TRANSIT___W Broadway & Blanding Ave Alameda",
                     "ALAMEDA/OAKLAND", "HARBOR BAY","VALLEJO/MARE ISLAND", "WESTCAT___LYNX Transbay Hercules to San Francisco Transbay Terminal") 
                     
# Below routes were removed as they either utilize different bridges or don't travel into SF:
                     #"AC TRANSIT___M Hayward BART to Oracle" 
                     #"AC TRANSIT___U Fremont BART to Stanford University"
                     #"DUMBARTON___DB Dumbarton Express", "DUMBARTON___DB1 Dumbarton Express",
                     #"WESTCAT___JPX Express Del Norte BART to Hercules Transit Center", 
                     #"WESTCAT___JR Express Del Norte BART Richmond Parkway Transit Center", 
                     #"WESTCAT___JX Express Del Norte BART to Hercules Transit Center",
                     #"WESTCAT___JL Express Del Norte BART Hilltop Shopping Center"

data.summary <- TPS %>% 
  filter(!(is.na(fare_category))) %>% 
  filter(fare_category!="") %>% 
  group_by(operator,fare_category) %>% 
  summarize(total=sum(final_boardWeight_2015))

write.csv(data.summary, paste0(OUTPUT, "TPS by operator and fare category.csv"), row.names = FALSE, quote = T)


 