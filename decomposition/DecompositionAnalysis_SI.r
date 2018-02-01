##################################################################################################
### Script to implement decomposition analysis on MTC survey data
### Author: Shimon Israel, February 2018, based on Binny M Paul, binny.paul@rsginc.com, April 2016
##################################################################################################

suppressMessages(library(dplyr))

# User Inputs
OBS_Dir <- "M:/Data/OnBoard/Data and Reports/_data Standardized/decomposition"
Legacy <- "M:/Data/OnBoard/Data and Reports/_data Standardized/survey_legacy.RData"
load(Legacy)

setwd(OBS_Dir)

# Read Files
load("survey_decomposition.RData")

# Rename operators so everything matches and filter weekday records only

OBS <- survey.decomposition %>% mutate(
  operator=ifelse(operator=="SF Muni","MUNI",operator),
  operator=ifelse(operator=="Caltrain","CALTRAIN",operator),
  operator=ifelse(operator=="Napa Vine","NAPA VINE",operator),
  operator=ifelse(operator=="Napa Vine","NAPA VINE",operator)) %>%
  filter(weekpart=="WEEKDAY") # Weekday records only 

#Fix missing values
#OBS$TRANSFERS_FROM_CODE[is.na(OBS$TRANSFERS_FROM_CODE)] <- 0 #missing for dummy records
#OBS$TRANSFERS_TO_CODE[is.na(OBS$TRANSFERS_TO_CODE)] <- 0 #missing for dummy records

# Decomposition Analysis
DA_Table <- data.frame(unique(OBS$operator))
colnames(DA_Table) <- c("operator")
DA_Table$SURVEYED_RESP <- 0
DA_Table$TRNSF_FROM_RESP <- 0
DA_Table$TRNSF_TO_RESP <- 0

DA_Table$T1 <- 0
DA_Table$T2 <- 0
DA_Table$T3 <- 0
DA_Table$T4 <- 0

DA_Table$F1 <- 0
DA_Table$F2 <- 0
DA_Table$F3 <- 0
DA_Table$F4 <- 0


temp <- aggregate(trip_weight~operator, data = OBS, FUN = sum)
DA_Table$SURVEYED_RESP <- temp$trip_weight[match(DA_Table$operator, temp$operator)]

temp <- aggregate(trip_weight~first_before_operator, data = OBS[OBS$first_before_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$F1 <- temp$trip_weight[match(DA_Table$operator, temp$first_before_operator)]

temp <- aggregate(trip_weight~second_before_operator, data = OBS[OBS$second_before_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$F2 <- temp$trip_weight[match(DA_Table$operator, temp$second_before_operator)]

temp <- aggregate(trip_weight~third_before_operator, data = OBS[OBS$third_before_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$F3 <- temp$trip_weight[match(DA_Table$operator, temp$third_before_operator)]

#temp <- aggregate(trip_weight~fourth_before_operator, data = OBS[OBS$fourth_before_operator %in% DA_Table$operator,], FUN = sum)
#DA_Table$F4 <- temp$trip_weight[match(DA_Table$operator, temp$fourth_before_operator)]

temp <- aggregate(trip_weight~first_after_operator, data = OBS[OBS$first_after_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$T1 <- temp$trip_weight[match(DA_Table$operator, temp$first_after_operator)]

temp <- aggregate(trip_weight~second_after_operator, data = OBS[OBS$second_after_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$T2 <- temp$trip_weight[match(DA_Table$operator, temp$second_after_operator)]

temp <- aggregate(trip_weight~third_after_operator, data = OBS[OBS$third_after_operator %in% DA_Table$operator,], FUN = sum)
DA_Table$T3 <- temp$trip_weight[match(DA_Table$operator, temp$third_after_operator)]

#temp <- aggregate(trip_weight~fourth_after_operator, data = OBS[OBS$fourth_after_operator %in% DA_Table$operator,], FUN = sum)
#DA_Table$T4 <- temp$trip_weight[match(DA_Table$operator, temp$fourth_after_operator)]

DA_Table[is.na(DA_Table)] <- 0

DA_Table$TRNSF_FROM_RESP <- DA_Table$F1 + DA_Table$F2 + DA_Table$F3 + DA_Table$F4

DA_Table$TRNSF_TO_RESP <- DA_Table$T1 + DA_Table$T2 + DA_Table$T3 + DA_Table$T4

DA_Table <- DA_Table[,c("operator", "SURVEYED_RESP", "TRNSF_FROM_RESP", "TRNSF_TO_RESP")]

DA_Table$operator <- as.character(DA_Table$operator)


# Process unlinked boardings

Boardings <- aggregate(weight~operator, data = OBS, FUN = sum)

DA_Table$OBSERVED <- Boardings$weight[match(DA_Table$operator, Boardings$operator)]

DA_Table <- DA_Table[order(DA_Table$operator),]
write.csv(DA_Table, paste(OBS_Dir, "DecompositionAnalysis.csv", sep = "/"), row.names = FALSE)


#View(OBS[,c("ID","FROM1", "FROM2", "FROM3", "FROM4", "operator", 
#            "TO1", "TO2", "TO3", "TO4", "TRANSFERS_FROM_CODE", "TRANSFERS_TO_CODE", 
#            "UNLINKED_WEIGHT_FACTOR", "trip_weight")])

