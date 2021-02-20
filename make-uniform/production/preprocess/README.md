This folder contains scripts to preprocess raw survey data.

Each "preprocessing_[operator].ipynb" file creates the following outputs:
* "[survey_data_file_name] NO POUND OR SINGLE QUOTE.csv" - the clean .csv file as input of 'Build Standard Database.R'
* "vars_for_standard_dictionary.csv" - variable dictionary which will be added to 'Dictionary for Standard Database.csv'
* "all_routes_raw.csv" - a dataset of all transfer routes that occurred in a survey, which is then manually modified to "all_routes_canonical.csv" by adding canonical route names, canonical operator names, and technologies. "all_routes_canonical.csv" will be added to 'canonical_route_crosswalk.csv'

[add_survey_routes_to_canonical_crosswalk.ipynb](add_survey_routes_to_canonical_crosswalk.ipynb):
* gather survey routes data from operators with more than one technologies (not including surveys whose survey routes info is already included in 'canonical_route_crosswalk.csv')
* output [survey_routes_raw.csv](survey_routes_raw.csv), which is then manually modified to [survey_routes_canonical.csv](survey_routes_canonical.csv) by adding canonical route names and technologies, and modifying canonical operator name (for Golden Gate Ferry)




