# TODO: move to github
# TODO: clean up, comment
# TODO: work through survey B example
# TODO: move to real example
# TODO: add excess variables to mini ex and handle appropriately


library(reshape2)
library(dplyr)

# work through mini example
survey.A <- read.csv('mini data A.csv', header = TRUE)
survey.B <- read.csv('mini data B.csv', header= TRUE)
dictionary <- read.csv('mini dictionary.csv', header = TRUE)

# generalize survey A
survey.A.melt <- melt(survey.A, id = 'ID')
names(survey.A.melt)[names(survey.A.melt) == 'variable'] <- 'Survey_Variable'
names(survey.A.melt)[names(survey.A.melt) == 'value'] <- 'Survey_Response'
survey.A.melt <- transform(survey.A.melt, Survey = 'A')

survey.A.join <- merge(x = survey.A.melt, y = dictionary, 
                       by = c("Survey", "Survey_Variable", "Survey_Response"), 
                       all.x = TRUE)

# select columns of interest and reshape
survey.A.clean <- survey.A.join[,c('ID', 'Generic_Variable', 'Generic_Response')]

# build a flat file 
survey.A.flat <- dcast(survey.A.clean, ID ~ Generic_Variable, value.var = 'Generic_Response')



