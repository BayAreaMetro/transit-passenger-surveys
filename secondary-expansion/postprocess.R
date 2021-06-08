###########################################################################################################################
### Script to process MTC TPS Database and produce inputs for weighting using PopualtionSim
###
### Author: Binny M Paul, July 2019
###########################################################################################################################

#=========================================================================================================================
# READ POPSIM INPUTS & OUTPUTS
#=========================================================================================================================

# Read TPS seed sample
TPS <- read.csv(file.path(POPSIM_Dir, "data", "TPS_processed.csv"), header = TRUE, stringsAsFactors = FALSE)
seed_households <- read.csv(file.path(POPSIM_Dir, "data", "seed_households.csv"), header = TRUE, stringsAsFactors = FALSE)

# Read PopSim weights
popSim_weights <- read.csv(file.path(POPSIM_Dir, "output", "GEOID_weights.csv"), stringsAsFactors = FALSE)

# Read in target boardings for 2015
boarding_targets <- read.csv(file.path(TARGETS_Dir, "transitRidershipTargets2015.csv"), header = TRUE, stringsAsFactors = FALSE)


#=========================================================================================================================
# WRITE OUT SUMMARIES
#=========================================================================================================================

# filter out zero weight records
TPS <- TPS[TPS$tripWeight_2015>0,]

# copy new weights from PopSim outputs
TPS$hh_id <- seed_households$HHNUM[match(TPS$Unique_ID, seed_households$UNIQUE_ID)]
TPS$final_tripWeight_2015 <- popSim_weights$balanced_weight[match(TPS$hh_id, popSim_weights$hh_id)]
TPS$final_boardWeight_2015 <- TPS$final_tripWeight_2015 * TPS$boardings
TPS$final_expansionFactor <- TPS$final_boardWeight_2015/TPS$boardWeight_2015

linkedtrips_best_mode_xfer <- TPS %>%
  group_by(SURVEY_MODE) %>%
  summarise(LB_CR = sum(LB_CR * final_boardWeight_2015), 
            LB_HR = sum(LB_HR * final_boardWeight_2015), 
            LB_LR = sum(LB_LR * final_boardWeight_2015), 
            LB_FR = sum(LB_FR * final_boardWeight_2015), 
            LB_EB = sum(LB_EB * final_boardWeight_2015), 
            LB_LB = sum(LB_LB * final_boardWeight_2015),
            EB_CR = sum(EB_CR * final_boardWeight_2015), 
            EB_HR = sum(EB_HR * final_boardWeight_2015), 
            EB_LR = sum(EB_LR * final_boardWeight_2015), 
            EB_FR = sum(EB_FR * final_boardWeight_2015), 
            EB_EB = sum(EB_EB * final_boardWeight_2015), 
            FR_CR = sum(FR_CR * final_boardWeight_2015), 
            FR_HR = sum(FR_HR * final_boardWeight_2015), 
            FR_LR = sum(FR_LR * final_boardWeight_2015), 
            FR_FR = sum(FR_FR * final_boardWeight_2015), 
            LR_CR = sum(LR_CR * final_boardWeight_2015), 
            LR_HR = sum(LR_HR * final_boardWeight_2015), 
            LR_LR = sum(LR_LR * final_boardWeight_2015), 
            HR_CR = sum(HR_CR * final_boardWeight_2015), 
            HR_HR = sum(HR_HR * final_boardWeight_2015), 
            CR_CR = sum(CR_CR * final_boardWeight_2015))

linkedtrips_best_mode_xfer <- data.frame(t(linkedtrips_best_mode_xfer), stringsAsFactors = F)
colnames(linkedtrips_best_mode_xfer) <- linkedtrips_best_mode_xfer[c(1),]
linkedtrips_best_mode_xfer <- linkedtrips_best_mode_xfer[-c(1),]
linkedtrips_best_mode_xfer <- cbind(TRANSFER_TYPE = row.names(linkedtrips_best_mode_xfer), linkedtrips_best_mode_xfer)

# Target boardings by MODE
target_boardings_mode <- xtabs(targets2015~technology, data = boarding_targets[boarding_targets$surveyed==1,])
write.table("target_boardings_mode", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",")
write.table(target_boardings_mode, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Target boardings by operator
target_boardings_operator <- xtabs(targets2015~operator, data = boarding_targets)
write.table("target_boardings_operator", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(target_boardings_operator, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by SURVEY MODE
boardings_survey_mode <- xtabs(final_boardWeight_2015~SURVEY_MODE, data = TPS)
write.table("boardings_survey_mode", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_survey_mode, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by OPERATOR
boardings_operator <- xtabs(final_boardWeight_2015~operator, data = TPS)
write.table("boardings_operator", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_operator, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by SURVEY MODE
linkedtrips_survey_mode <- xtabs(final_tripWeight_2015~SURVEY_MODE, data = TPS)
write.table("linkedtrips_survey_mode", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_survey_mode, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Linkedtrips by BEST MODE
linkedtrips_best_mode <- xtabs(final_tripWeight_2015~BEST_MODE, data = TPS)
write.table("linkedtrips_best_mode", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)

# Boardings by BEST MODE and transfer type
#linkedtrips_best_mode_xfer <- xtabs(boardWeight_2015~TRANSFER_TYPE+SURVEY_MODE, data = TPS)
write.table("linkedtrips_best_mode_xfer", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(linkedtrips_best_mode_xfer, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)

# Expansion Factor Distribution by Survey Mode
# Puts values into intervals
# Plot values

uniformity <- TPS[,c("SURVEY_MODE", "final_expansionFactor")] %>%
  mutate(EFBIN = cut(final_expansionFactor,c(0.25,0.5,0.75, 0.85,0.95,1.05,1.15, 1.25,1.5,2,3,5,10),right=FALSE, include.lowest=FALSE))

uAnalysisPUMA <- group_by(uniformity, SURVEY_MODE, EFBIN)

efPlotData <- summarise(uAnalysisPUMA, PC = n()) %>%
  mutate(PC=PC/sum(PC))

ggplot(efPlotData, aes(x=EFBIN, y=PC))  + 
  geom_bar(colour="black", fill="#DD8888", width=.7, stat="identity") + 
  guides(fill=FALSE) +
  xlab("RANGE OF EXPANSION FACTOR") + ylab("PERCENTAGE") +
  ggtitle("EXPANSION FACTOR DISTRIBUTION BY SURVEY MODE") + 
  facet_wrap(~SURVEY_MODE, ncol=2) + 
  theme_bw()+
  theme(axis.title.x = element_text(face="bold"),
        axis.title.y = element_text(face="bold"),
        axis.text.x  = element_text(angle=90, size=15),
        axis.text.y  = element_text(size=15))  +
  scale_y_continuous(labels = percent_format())

ggsave(file.path(VALIDATION_Dir, "EF-Distribution by Survey Mode.png"), width=15,height=10)

# EF Distribution Total
#-------------------------------------------
uAnalysisPUMA <- group_by(uniformity, EFBIN)
efPlotData <- summarise(uAnalysisPUMA, PC = n()) %>%
  mutate(PC=PC/sum(PC))

ggplot(efPlotData, aes(x=EFBIN, y=PC))  + 
  geom_bar(colour="black", fill="#DD8888", width=.7, stat="identity") + 
  guides(fill=FALSE) +
  xlab("RANGE OF EXPANSION FACTOR") + ylab("PERCENTAGE") +
  ggtitle("EXPANSION FACTOR DISTRIBUTION") + 
  theme_bw()+
  theme(axis.title.x = element_text(face="bold"),
        axis.title.y = element_text(face="bold"),
        axis.text.x  = element_text(angle=90, size=15),
        axis.text.y  = element_text(size=15))  +
  scale_y_continuous(labels = percent_format())

ggsave(file.path(VALIDATION_Dir, "EF-Distribution.png"), width=15,height=10)

# Old vs New Boardings Weights Comparison
#---------------------------------------------
weights_comparison <- TPS[,c("Unique_ID", "hh_id", "SURVEY_MODE", "operator", "route", "onoff_enter_station", "onoff_exit_station", 
                             "final_boardWeight_2015", "boardWeight_2015", "final_expansionFactor")]
weights_comparison_no_outlier <- weights_comparison[weights_comparison$boardWeight_2015<200,]   # Create a variable that constrains weight to <200

# Scatter plot
p1 <- ggplot(weights_comparison, aes(x=boardWeight_2015, y=final_boardWeight_2015, color=SURVEY_MODE)) + 
  geom_point(shape=1) +
  geom_abline(intercept = 0, slope = 5, linetype = 2, color = "red") + 
  geom_abline(intercept = 0, slope = 1, linetype = 2) + 
  geom_abline(intercept = 0, slope = 0.25, linetype = 2, color = "red") + 
  labs(x="Original Boarding Weight", y="Adjusted Boarding Weight")
ggsave(file.path(VALIDATION_Dir, "Old vs New Boarding Weight.png"), plot = p1, width=12,height=8, device = "png", dpi = 600)

p2 <- ggplot(weights_comparison_no_outlier, aes(x=boardWeight_2015, y=final_boardWeight_2015, color=SURVEY_MODE)) + 
  geom_point(shape=1) +
  geom_abline(intercept = 0, slope = 5, linetype = 2, color = "red") + 
  geom_abline(intercept = 0, slope = 1, linetype = 2) + 
  geom_abline(intercept = 0, slope = 0.25, linetype = 2, color = "red") + 
  labs(x="Original Boarding Weight", y="Adjusted Boarding Weight")
ggsave(file.path(VALIDATION_Dir, "Old vs New Boarding Weight - No Outlier.png"), plot = p2, width=12,height=8, device = "png", dpi = 600)


# Boradings by Route Comparison
#-----------------------------------------------
boardings_by_route <- weights_comparison %>%
  group_by(operator, route) %>%
  summarise(originalBoardings = sum(boardWeight_2015), adjustedBoardings = sum(final_boardWeight_2015))

write.table("old vs new boarding by route", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(boardings_by_route, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)


# Entry Exit Summaries for BART and Caltrain
#------------------------------------------------

# Bart entry-exit
bart_exit <- weights_comparison[weights_comparison$operator=="BART", ] %>%
  group_by(onoff_exit_station) %>%
  summarise(originalExit = sum(boardWeight_2015), adjustedExit = sum(final_boardWeight_2015))

bart_entry_exit <- weights_comparison[weights_comparison$operator=="BART", ] %>%
  group_by(onoff_enter_station) %>%
  summarise(originalEnter = sum(boardWeight_2015), adjustedEnter = sum(final_boardWeight_2015)) %>%
  left_join(bart_exit, by = c("onoff_enter_station" = "onoff_exit_station"))

# Caltrain entry-exit
caltrain_exit <- weights_comparison[weights_comparison$operator=="Caltrain", ] %>%
  group_by(onoff_exit_station) %>%
  summarise(originalExit = sum(boardWeight_2015), adjustedExit = sum(final_boardWeight_2015))

caltrain_entry_exit <- weights_comparison[weights_comparison$operator=="Caltrain", ] %>%
  group_by(onoff_enter_station) %>%
  summarise(originalEnter = sum(boardWeight_2015), adjustedEnter = sum(final_boardWeight_2015)) %>%
  left_join(caltrain_exit, by = c("onoff_enter_station" = "onoff_exit_station"))

write.table("Bart entry-exit", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(bart_entry_exit, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)

write.table("Caltrain entry-exit", file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T)
write.table(caltrain_entry_exit, file.path(VALIDATION_Dir, "PopSim_Summaries_Paste.csv"), sep = ",", append = T, row.names = F)



#=========================================================================================================================
# WRITE OUT FINAL TPS DATASET WITH FINAL WEIGHTS
#=========================================================================================================================

write.csv(TPS, file.path(TPS_Dir, "TPS_PopulationSim_Weights.csv"), row.names = F)


## FINISH

# Turn back warnings;
options(warn = oldw)




