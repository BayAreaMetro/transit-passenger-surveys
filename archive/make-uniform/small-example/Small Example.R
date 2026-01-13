# Small Example.R
#
# The broad goal is to develop a procedure to translate any number of on-board survey data sets into a single dataset
# with common variables and common responses.  This small example builds off a mini example (../mini-example) and 
# starts to work with real data.

# Overhead
library(reshape2)
suppressMessages(library(dplyr))
library(stringr)

# Parameters
survey.A.key <- 'ETC'
survey.B.key <- 'RED'

# Data reads -- two survey data sets and one dictionary file
survey.A <- read.csv('ETC Example No Locations.csv', header = TRUE)
survey.B <- read.csv('RED Example No Locations.csv', header= TRUE)
dictionary.all <- read.csv('Example Dictionary.csv', header = TRUE)

# Prepare seperate dictionaries for categorical and non-categorical variables
dictionary.non <- dictionary.all %.%
  filter(Generic_Response == 'NONCATEGORICAL') %.%
  select(Survey, Survey_Variable, Generic_Variable)

dictionary.cat <- dictionary.all %.%
  filter(Generic_Response != 'NONCATEGORICAL') %.%
  mutate(Survey_Response = as.character(Survey_Response))

# Select the data in the dictionary for each survey
dictionary.A <- dictionary.all %.%
  filter(Survey == survey.A.key)

variables.A <- as.character(factor(unique(unlist(dictionary.A$Survey_Variable, use.names = TRUE))))

survey.A.select <- survey.A[ ,variables.A]


dictionary.B <- dictionary.all %.%
  filter(Survey == survey.B.key)

variables.B <- as.character(factor(unique(unlist(dictionary.B$Survey_Variable, use.names = TRUE))))

survey.B.select <- survey.B[ ,variables.B]

# Reshape survey A into a four column database - ID, Variable, Response (as a string), Survey
survey.A.melt <- melt(survey.A.select, id = 'ID')
survey.A.melt <- select(survey.A.melt, ID, Survey_Variable = variable, Survey_Response = value)
survey.A.melt <- mutate(survey.A.melt, Survey = survey.A.key)

# Reshape survey B into a four column database - ID, Variable, Response, Survey
survey.B.melt <- melt(survey.B.select, id = 'ID')
survey.B.melt <- select(survey.B.melt, ID, Survey_Variable = variable, Survey_Response = value)
survey.B.melt <- mutate(survey.B.melt, Survey = survey.B.key)

# Bind survey data A and B
survey.combine <- rbind(survey.A.melt, survey.B.melt)

# Join the dictionary and prepare the categorical variables
survey.cat <- mutate(survey.combine, Survey_Response = as.character(Survey_Response))
survey.cat <- left_join(survey.cat, dictionary.cat, by = c("Survey", "Survey_Variable", "Survey_Response"))
survey.cat <- filter(survey.cat, !is.na(Generic_Variable))

# Join the dictionary and prepare the non-categorical variables
survey.non <- left_join(survey.combine, dictionary.non, by = c("Survey", "Survey_Variable"))
survey.non <- survey.non %.%
  filter(!is.na(Generic_Variable)) %.%
  mutate(Generic_Response = Survey_Response)

# Combine the categorical and non-categorical survey data and prepare to flatten
survey.cat.to_flat <- survey.cat %.%
  select(-Survey_Variable, -Survey_Response) %.%
  mutate(Generic_Response = as.factor(Generic_Response))

survey.non.to_flat <- survey.non %.%
  select(-Survey_Variable, -Survey_Response) %.%
  mutate(Generic_Response = as.factor(Generic_Response))

survey.to_flat <- rbind(survey.cat.to_flat, survey.non.to_flat)

# Put together and then take apart a unique ID when flattening
survey.to_flat <- survey.to_flat %.%
  mutate(Unique_ID = paste(ID, Survey, sep = "-")) %.%
  select(-ID, -Survey)

survey.flat <- dcast(survey.to_flat, Unique_ID ~ Generic_Variable, value.var = 'Generic_Response')

survey.flat <- cbind(survey.flat, colsplit(survey.flat$Unique_ID, "-", c("ID", "Survey")))

