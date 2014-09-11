# Mini Example.R
#
# The broad goal is to develop a procedure to translate any number of on-board survey data sets into a single dataset
# with common variables and common responses.  The following mini-example is my first step.


# Overhead
library(reshape2)
suppressMessages(library(dplyr))
library(stringr)

# Data reads -- two survey data sets and one dictionary file
survey.A <- read.csv('mini data A.csv', header = TRUE)
survey.B <- read.csv('mini data B.csv', header= TRUE)
dictionary <- read.csv('mini dictionary.csv', header = TRUE)

# Reshape survey A into a four column database - ID, Variable, Response, Survey
survey.A.melt <- melt(survey.A, id = 'ID')
survey.A.melt <- select(survey.A.melt, ID, Survey_Variable = variable, Survey_Response = value)
survey.A.melt <- transform(survey.A.melt, Survey = 'A')

# Reshape survey B into a four column database - ID, Variable, Response, Survey
survey.B.melt <- melt(survey.B, id = 'ID')
survey.B.melt <- select(survey.B.melt, ID, Survey_Variable = variable, Survey_Response = value)
survey.B.melt <- transform(survey.B.melt, Survey = 'B')

# Bind survey data A and B, then join with dictionary (removing variables with no dictionary map)
survey.combine <- rbind(survey.A.melt, survey.B.melt)
survey.general <- left_join(x = survey.combine, y = dictionary, by = c("Survey", "Survey_Variable", "Survey_Response"))
survey.general <- filter(survey.general, !is.na(Generic_Variable))

# Prepare and flatten the joined file
survey.prep <- survey.general %.%
  mutate(Unique_ID = paste(ID, Survey, sep = "-")) %.%
  select(-Survey_Variable, -Survey_Response, -ID, -Survey)

survey.flat <- dcast(survey.prep, Unique_ID ~ Generic_Variable, value.var = 'Generic_Response')

survey.flat <- cbind(survey.flat, colsplit(survey.flat$Unique_ID, "-", c("ID", "Survey")))


