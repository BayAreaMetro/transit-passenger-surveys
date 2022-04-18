# SMART Race and Income Summaries.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Bill Bacon/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories
Onboard       <- "M:/Data/OnBoard/Data and Reports/"
SMART_in      <- paste0(Onboard,"SMART/As CSV/SMART Standardized Final Data NO POUND NO SINGLE QUOTE.csv")

SMART <- read.csv(SMART_in, stringsAsFactors = FALSE) 

# Summarize income
SMART_income <- SMART %>% 
  filter(income!="UNKNOWN") %>% mutate(
    income_rc=case_when(
      income=="UNDER $10,000"        ~"1_less than 25k",
      income=="$10,000 to $25,000"   ~"1_less than 25k",
      income=="$25,000 to $35,000"   ~"2_25-50k",
      income=="$35,000 to $50,000"   ~"2_25-50k",
      income=="$50,000 to $75,000"   ~"3_50-75k",
      income=="$75,000 to $100,000"  ~"4_75-100k",
      income=="$100,000 to $150,000" ~"5_100-150k",
      income=="$150,000 OR HIGHER"   ~"6_150k+"
    )
  ) %>% 
  group_by(income_rc) %>% 
  summarize(total=sum(WEIGHT)) %>% 
  spread(income_rc,total)

# Summarize race/ethnicity
SMART_race <- SMART %>% 
  select(hisp,race_dmy_ind,race_dmy_hwi,race_dmy_blk,race_dmy_wht,race_dmy_asn,race_other,race_6_other,WEIGHT) %>% 
  filter(race_6_other != "REFUSED") %>% mutate(
    race_sum=race_dmy_ind+race_dmy_hwi+race_dmy_blk+race_dmy_wht+race_dmy_asn+race_other,
    race_general=case_when(
      hisp=="HISPANIC/LATINO OR OF SPANISH ORIGIN" | race_6_other=="HISPANIC"  ~ "5_Hispanic",
      race_sum>=2                                                              ~ "4_Other, Not Hispanic",
      race_dmy_wht==1                                                          ~ "1_White, Not Hispanic",
      race_dmy_blk==1                                                          ~ "2_Black, Not Hispanic",
      race_dmy_asn==1                                                          ~ "3_Asian, Not Hispanic",
      race_dmy_ind==1                                                          ~ "4_Other, Not Hispanic",
      race_dmy_hwi==1                                                          ~ "4_Other, Not Hispanic",
      race_other==1                                                            ~ "4_Other, Not Hispanic",
      TRUE                                                                     ~ "Uncoded")
  ) %>% 
  group_by(race_general) %>% 
  summarize(total=sum(WEIGHT)) %>% 
  spread(race_general,total)

# Write out final CSV files

write.csv(SMART_income,"SMART_income.csv",row.names = FALSE)
write.csv(SMART_race,"SMART_race.csv",row.names = FALSE)




