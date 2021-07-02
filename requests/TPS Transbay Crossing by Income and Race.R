# TPS Transbay Crossing by Income and Race.R
# Analyze BART and Transbay bus riders (Bay Bridge) for income and race

# Import Library

suppressMessages(library(tidyverse))

# Input survey file

TPS_SURVEY_IN = "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/survey_combined_2021-06-09.RData"
OUTPUT = "M:/Data/Requests/Lisa Zorn/TPS Bay Bridge Income and Race/"
load (TPS_SURVEY_IN)

# Bring in select link files and concatenate all combinations with sum of vol > 0

directory <- "M:/Application/Model One/RTP2021/IncrementalProgress/2015_TM152_IPA_16/OUTPUT/BayBridge_and_transit/"

EA_West <- paste0(directory,"loadEA_selectlink_2783-6972_ODs_v2.csv")  # Early AM, westbound, etc.
AM_West <- paste0(directory,"loadAM_selectlink_2783-6972_ODs_v2.csv")
MD_West <- paste0(directory,"loadMD_selectlink_2783-6972_ODs_v2.csv")
PM_West <- paste0(directory,"loadPM_selectlink_2783-6972_ODs_v2.csv")
EV_West <- paste0(directory,"loadEV_selectlink_2783-6972_ODs_v2.csv")

EA_East <- paste0(directory,"loadEA_selectlink_6973-2784_ODs_v2.csv")   # Early AM, eastbound, etc.
AM_East <- paste0(directory,"loadAM_selectlink_6973-2784_ODs_v2.csv")
MD_East <- paste0(directory,"loadMD_selectlink_6973-2784_ODs_v2.csv")
PM_East <- paste0(directory,"loadPM_selectlink_6973-2784_ODs_v2.csv")
EV_East <- paste0(directory,"loadEV_selectlink_6973-2784_ODs_v2.csv")

# Import files, append period and direction variables

EA_WB <- read.csv(EA_West,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Early AM", direction="Westbound")  # Early AM, westbound, etc.
AM_WB <- read.csv(AM_West,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="AM Peak", direction="Westbound")
MD_WB <- read.csv(MD_West,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Midday", direction="Westbound")
PM_WB <- read.csv(PM_West,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="PM Peak", direction="Westbound")
EV_WB <- read.csv(EV_West,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Evening", direction="Westbound")

EA_EB <- read.csv(EA_East,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Early AM", direction="Eastbound")  # Early AM, eastbound, etc.
AM_EB <- read.csv(AM_East,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="AM Peak", direction="Eastbound")
MD_EB <- read.csv(MD_East,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Midday", direction="Eastbound")
PM_EB <- read.csv(PM_East,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="PM Peak", direction="Eastbound")
EV_EB <- read.csv(EV_East,header = TRUE) %>% select(OTAZ,DTAZ,vol) %>% mutate (period="Evening", direction="Eastbound")

all_Westbound <- bind_rows(EA_WB,AM_WB,MD_WB,PM_WB,EV_WB)
all_eastbound <- bind_rows(EA_EB,AM_EB,MD_EB,PM_EB,EV_EB)

west_sum <- all_Westbound %>% 
  group_by(OTAZ,DTAZ) %>% 
  summarize(west_vol=sum(vol)) %>% 
  filter(west_vol>0) 

east_sum <- all_eastbound %>% 
  group_by(OTAZ,DTAZ) %>% 
  summarize(east_vol=sum(vol)) %>% 
  filter(east_vol>0) 
  

# Subset transbay operators as a first pass - remove dummy records, pick right year of survey, weekday only

BB_Operators <- data.ready %>% filter((operator == "AC Transit" & survey_tech=="express bus") |
                                        (operator == "WestCAT" & survey_tech=="express bus") |
                                        operator %in% c("BART","SF Bay Ferry/WETA"))
BB_Operators <- BB_Operators %>% filter(survey_year>=2015 & weekpart=="WEEKDAY" & 
                                          access_mode != "Missing - Dummy Record") %>% 
  mutate(westbound_transit=0,eastbound_transit=0)
                                      

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

BB_Operators <- BB_Operators %>% 
  filter(operator=="BART" | route %in% Transbay_Routes) %>% 
  left_join(.,west_sum, by=c("orig_tm1_taz"="OTAZ","dest_tm1_taz"="DTAZ")) %>% 
  left_join(.,east_sum, by=c("orig_tm1_taz"="OTAZ","dest_tm1_taz"="DTAZ")) %>% 
  mutate(westbound_transit=if_else(operator=="BART" & 
                                     onoff_enter_station %in% East_Bay_BART &
                                     onoff_exit_station %in% West_Bay_BART,1,westbound_transit),
         eastbound_transit=if_else(operator=="BART" &
                                     onoff_enter_station %in% West_Bay_BART &
                                     onoff_exit_station %in% East_Bay_BART,1,eastbound_transit))%>% 
  mutate(westbound_transit=if_else(west_vol>0 & !is.na(west_vol),1,westbound_transit),
         eastbound_transit=if_else(east_vol>0 & !is.na(east_vol),1,eastbound_transit))

# Summarize transit by operator, income, and race ethnicity

# Summarize income
BB_income <- BB_Operators %>% 
  mutate(
    income_rc=case_when(
      household_income=="under $10,000"        ~"1_less than 25k",
      household_income=="$10,000 to $25,000"   ~"1_less than 25k",
      household_income=="$25,000 to $35,000"   ~"2_25-50k",
      household_income=="$35,000 to $50,000"   ~"2_25-50k",
      household_income=="$50,000 to $75,000"   ~"3_50-75k",
      household_income=="$75,000 to $100,000"  ~"4_75-100k",
      household_income=="$100,000 to $150,000" ~"5_100-150k",
      household_income=="$150,000 or higher"   ~"6_150k+",
      TRUE                           ~"Missing or NA")
    ) 

west_income <- BB_income %>% 
  filter(westbound_transit==1) %>%  
  group_by(operator,income_rc) %>% 
  summarize(total=sum(weight)) %>% 
  spread(income_rc,total, fill=0)

east_income <- BB_income %>% 
  filter(eastbound_transit==1) %>%  
  group_by(operator,income_rc) %>% 
  summarize(total=sum(weight)) %>% 
  spread(income_rc,total,fill=0)

# Summarize race/ethnicity
BB_race <- BB_Operators %>% 
  mutate(race_general=case_when(
    hispanic=="HISPANIC/LATINO OR OF SPANISH ORIGIN"                           ~ "hispanic",
    hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" & race=="WHITE"       ~ "white",
    hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" & race=="BLACK"       ~ "black",
    hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" & race=="ASIAN"       ~ "asian",
    hispanic=="NOT HISPANIC/LATINO OR OF SPANISH ORIGIN" & race=="OTHER"       ~ "other",
    TRUE                                                                       ~ "NA or missing")
  ) 

west_race <- BB_race %>% 
  filter(westbound_transit==1) %>%  
  group_by(operator,race_general) %>% 
  summarize(total=sum(weight)) %>% 
  spread(race_general,total, fill = 0)

east_race <- BB_race %>% 
  filter(eastbound_transit==1) %>%  
  group_by(operator,race_general) %>% 
  summarize(total=sum(weight)) %>% 
  spread(race_general,total, fill = 0)


write.csv(west_income, paste0(OUTPUT, "TPS Westbound Bay Bridge Income by Operator.csv"), row.names = FALSE, quote = T)
write.csv(east_income, paste0(OUTPUT, "TPS Eastbound Bay Bridge Income by Operator.csv"), row.names = FALSE, quote = T)
write.csv(west_race, paste0(OUTPUT, "TPS Westbound Bay Bridge Race by Operator.csv"), row.names = FALSE, quote = T)
write.csv(east_race, paste0(OUTPUT, "TPS Eastbound Bay Bridge Race by Operator.csv"), row.names = FALSE, quote = T)
