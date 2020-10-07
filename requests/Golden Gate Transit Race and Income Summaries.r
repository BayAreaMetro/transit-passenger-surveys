# Golden Gate Transit Race and Income Summaries.r
# SI


# Set working directory

wd <- "M:/Data/Requests/Adam Crenshaw/TIP 2020/"
setwd(wd)

# Import libraries

suppressMessages(library(tidyverse))

# Set up input and output directories
Onboard       <- "M:/Data/OnBoard/Data and Reports/"
GGT_in      <- paste0(Onboard,"Golden Gate Transit/2018/As CSV/20180907_OD_GoldenGate_WEEKDAY_Submitted NO POUND NO SINGLE QUOTE.csv")

GGT <- read.csv(GGT_in, stringsAsFactors = FALSE) 

# Summarize income
GGT_income <- GGT %>% 
  filter(!(household_income %in% c("REFUSED","Skip - Paper Survey"))) %>% mutate( # Filter out refusals
    income_rc=case_when(
      household_income=="Below $10,000"        ~"1_less than 25k",
      household_income=="$10,000-$24,999"      ~"1_less than 25k",
      household_income=="$25,000-$34,999"      ~"2_25-50k",
      household_income=="$35,000-$49,999"      ~"2_25-50k",
      household_income=="$50,000 - $74,999"    ~"3_50-75k",
      household_income=="$75,000 - $99,999"    ~"4_75-100k",
      household_income=="$100,000 - $149,999"  ~"5_100-150k",
      household_income=="$150,000 or more"     ~"6_150k+",
      TRUE                           ~"Missing Recode"
    )
  ) %>% 
  group_by(income_rc) %>% 
  summarize(total=sum(unlinked_weight_factor)) %>% 
  spread(income_rc,total)

# Summarize race/ethnicity
GGT_race <- GGT %>% 
  select(race_dmy_ltn,race_dmy_blk,race_dmy_asn,race_dmy_amcn_ind,race_dmy_hwi,race_dmy_whi,race_other_string,unlinked_weight_factor) %>%
  mutate(
    race_other=if_else(race_other_string=="",0,1),
    race_ltn=if_else(race_dmy_ltn=="Yes",1,0),
    race_blk=if_else(race_dmy_blk=="Yes",1,0),
    race_asn=if_else(race_dmy_asn=="Yes",1,0),
    race_amcn_ind=if_else(race_dmy_amcn_ind=="Yes",1,0),
    race_hwi=if_else(race_dmy_hwi=="Yes",1,0),
    race_whi=if_else(race_dmy_whi=="Yes",1,0),
    race_sum=race_ltn+race_blk+race_asn+race_amcn_ind+race_hwi+race_whi+race_other,

    race_general=case_when(
      race_ltn==1                                                              ~ "5_Hispanic",
      race_sum>=2                                                              ~ "4_Other, Not Hispanic",
      race_whi==1                                                              ~ "1_White, Not Hispanic",
      race_blk==1                                                              ~ "2_Black, Not Hispanic",
      race_asn==1                                                              ~ "3_Asian, Not Hispanic",
      race_amcn_ind==1                                                         ~ "4_Other, Not Hispanic",
      race_hwi==1                                                              ~ "4_Other, Not Hispanic",
      race_other==1                                                            ~ "4_Other, Not Hispanic",
      TRUE                                                                     ~ "Uncoded")) %>% 
  filter(race_general!="Uncoded") %>%                                     #Filter out non-response records for race
  group_by(race_general) %>% 
  summarize(total=sum(unlinked_weight_factor)) %>% 
  spread(race_general,total)

# Write out final CSV files

write.csv(GGT_income,"GGT_income.csv",row.names = FALSE)
write.csv(GGT_income,"GGT_race.csv",row.names = FALSE)




