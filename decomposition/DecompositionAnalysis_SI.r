#######################################################
### Script to implement decomposition analysis on SANDAG survey data
### Author: Binny M Paul, binny.paul@rsginc.com, April 2016
#######################################################

# User Inputs
OBS_Dir <- "M:/Data/OnBoard/Data and Reports/_data Standardized/decomposition"
Legacy <- "M:/Data/OnBoard/Data and Reports/_data Standardized/survey_legacy.RData"
load(Legacy)

setwd(OBS_Dir)

# Read Files
load("survey_decomposition.RData")
OBS <- survey.decomposition

#Fix missing values
#OBS$TRANSFERS_FROM_CODE[is.na(OBS$TRANSFERS_FROM_CODE)] <- 0 #missing for dummy records
#OBS$TRANSFERS_TO_CODE[is.na(OBS$TRANSFERS_TO_CODE)] <- 0 #missing for dummy records

# Decomposition Analysis
DA_Table <- data.frame(unique(OBS$operator))
colnames(DA_Table) <- c("ROUTENUM")
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


temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~ROUTENUM, data = OBS, FUN = sum)
DA_Table$SURVEYED_RESP <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$ROUTENUM)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~FROM1, data = OBS[OBS$FROM1 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$F1 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$FROM1)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~FROM2, data = OBS[OBS$FROM2 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$F2 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$FROM2)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~FROM3, data = OBS[OBS$FROM3 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$F3 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$FROM3)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~FROM4, data = OBS[OBS$FROM4 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$F4 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$FROM4)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~TO1, data = OBS[OBS$TO1 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$T1 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$TO1)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~TO2, data = OBS[OBS$TO2 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$T2 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$TO2)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~TO3, data = OBS[OBS$TO3 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$T3 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$TO3)]

temp <- aggregate(FACTOR_TO_EXPAND_TO_LINKED_TRIPS~TO4, data = OBS[OBS$TO4 %in% DA_Table$ROUTENUM,], FUN = sum)
DA_Table$T4 <- temp$FACTOR_TO_EXPAND_TO_LINKED_TRIPS[match(DA_Table$ROUTENUM, temp$TO4)]

DA_Table[is.na(DA_Table)] <- 0

DA_Table$TRNSF_FROM_RESP <- DA_Table$F1 + DA_Table$F2 + DA_Table$F3 + DA_Table$F4

DA_Table$TRNSF_TO_RESP <- DA_Table$T1 + DA_Table$T2 + DA_Table$T3 + DA_Table$T4

DA_Table <- DA_Table[,c("ROUTENUM", "SURVEYED_RESP", "TRNSF_FROM_RESP", "TRNSF_TO_RESP")]

DA_Table$ROUTENUM <- as.character(DA_Table$ROUTENUM)


# Process observed boardings
#---------------------------
# Get route numbers in OBS
Obs_Board$ROUTENUM <- sapply(as.character(Obs_Board$Row.Labels), function(x) {returnRoute(x)})
Obs_Board$ROUTENUM[is.na(Obs_Board$ROUTENUM)] <- ""

Obs_Board$ROUTENUM[Obs_Board$ROUTENUM=="Blue"] <- 510
Obs_Board$ROUTENUM[Obs_Board$ROUTENUM=="Orange"] <- 520
Obs_Board$ROUTENUM[Obs_Board$ROUTENUM=="Green"] <- 530
Obs_Board$ROUTENUM[Obs_Board$ROUTENUM=="COASTER" | Obs_Board$ROUTENUM=="Coaster"] <- 398
Obs_Board$ROUTENUM[Obs_Board$ROUTENUM=="SPRINTER" | Obs_Board$ROUTENUM=="Sprinter"] <- 399

Boardings <- aggregate(Grand.Total~ROUTENUM, data = Obs_Board, FUN = sum)

DA_Table$OBSERVED <- Boardings$Grand.Total[match(DA_Table$ROUTENUM, Boardings$ROUTENUM)]

DA_Table$ROUTENUM[DA_Table$ROUTENUM=="510"] <- "Blue"
DA_Table$ROUTENUM[DA_Table$ROUTENUM=="520"] <- "Orange"
DA_Table$ROUTENUM[DA_Table$ROUTENUM=="530"] <- "Green"
DA_Table$ROUTENUM[DA_Table$ROUTENUM=="398"] <- "Coaster"
DA_Table$ROUTENUM[DA_Table$ROUTENUM=="399"] <- "Sprinter"

DA_Table <- DA_Table[order(DA_Table$ROUTENUM),]
write.csv(DA_Table, paste(DA_Dir, "DecompositionAnalysis.csv", sep = "//"), row.names = FALSE)


#View(OBS[,c("ID","FROM1", "FROM2", "FROM3", "FROM4", "ROUTENUM", 
#            "TO1", "TO2", "TO3", "TO4", "TRANSFERS_FROM_CODE", "TRANSFERS_TO_CODE", 
#            "UNLINKED_WEIGHT_FACTOR", "FACTOR_TO_EXPAND_TO_LINKED_TRIPS")])

