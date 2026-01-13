# ACS PUMS 2019 People by Income.R

suppressMessages(library(tidyverse))
output <- "M:/Data/OnBoard/Data and Reports/_data Standardized/share_data/public_version"  # work directory

# Input person census files

PERSON_RDATA = "M:/Data/Census/PUMS/PUMS 2019/pbayarea19.Rdata"
HH_RDATA     = "M:/Data/Census/PUMS/PUMS 2019/hbayarea19.Rdata"

load (PERSON_RDATA)
load(HH_RDATA) 

household <- hbayarea19 %>%  
  select(SERIALNO,HINCP,ADJINC,VEH)

# Extract bike commuters and recode relevant variables, join with household file for income values

final <- pbayarea19 %>%
  filter(RELSHIPP<=36) %>% 
  select(-ADJINC) %>%                                   # Remove this variable and use joined version
  left_join(.,household,by="SERIALNO")

income <- final %>% 
  mutate(
    adjustedinc=HINCP*(ADJINC/1000000),
    Household_Income=case_when(
      adjustedinc <25000                         ~"<$25K",
      adjustedinc >=25000 & adjustedinc <50000   ~"$25K - $50K",
      adjustedinc >=50000 & adjustedinc <75000   ~"$50K - $75K",
      adjustedinc >=75000 & adjustedinc <100000  ~"$75K - $100K",
      adjustedinc >= 100000 & adjustedinc <150000  ~"$100K - $150K",
      adjustedinc >= 150000              ~"150k+",
      TRUE                                       ~"Uncoded, group quarters")) %>% 
  group_by(Household_Income) %>% 
  summarize(Weight=sum(PWGTP)) %>% 
  #mutate(share=total/sum(total)) %>% 
  ungroup()

vehicles <- final %>% 
  mutate(
    Vehicles=case_when(
      VEH ==0  ~"Zero",
      VEH ==1   ~"One",
      VEH ==2  ~"Two",
      VEH ==3  ~"Three",
      VEH >=4  ~"Four+",
      TRUE                                       ~"Uncoded, group quarters")) %>% 
  group_by(Vehicles) %>% 
  summarize(Weight=sum(PWGTP)) %>% 
  #mutate(share=total/sum(total)) %>% 
  ungroup()

write.csv(vehicles, file.path(output,"PUMS2019 Household Persons by Vehicles.csv"),row.names = F, quote = T)
write.csv(income, file.path(output,"PUMS2019 Household Persons by Income.csv"),row.names = F, quote = T)

#Previous incorrect way
#write.csv(income, "PUMS2019 Household Persons by Income.csv", row.names = FALSE, quote = T)
#write.csv(vehicles, "PUMS2019 Household Persons by Vehicle Availability.csv", row.names = FALSE, quote = T)
