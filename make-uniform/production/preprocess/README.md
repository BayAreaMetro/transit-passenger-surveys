This folder contains scripts to preprocess raw survey data.

Each "preprocessing_[operator].ipynb" file creates the following outputs:
* "[survey_data_file_name] NO POUND OR SINGLE QUOTE.csv" - the clean .csv file as input of 'Build Standard Database.R'
* "vars_for_standard_dictionary.csv" - variable dictionary which will be added to 'Dictionary for Standard Database.csv'
* "all_routes_raw.csv" - a dataset of all transfer routes that occurred in a survey, which is then manually modified to "all_routes_canonical.csv" by adding canonical route names, canonical operator names, and technologies. "all_routes_canonical.csv" will be added to 'canonical_route_crosswalk.csv'

[add_survey_routes_to_canonical_crosswalk.ipynb](add_survey_routes_to_canonical_crosswalk.ipynb):
* gather survey routes data from operators with more than one technologies (not including surveys whose survey routes info is already included in 'canonical_route_crosswalk.csv')
* output [survey_routes_raw.csv](survey_routes_raw.csv), which is then manually modified to [survey_routes_canonical.csv](survey_routes_canonical.csv) by adding canonical route names and technologies, and modifying canonical operator name (for Golden Gate Ferry)

[check_BART_2015_dictionary.ipynb](check_BART_2015_dictionary.ipynb): examine and correct the errors in the variable/response coding for BART 2015 survey in the previous `Dictionary for Standard Database.csv`.

[truncate_transfers_MUNI_ACTransit.ipynb](truncate_transfers_MUNI_ACTransit.ipynb): these surveys contain responses that show more than three transfers before or after the surveyed route, however, the standard database only tracks up to three before/after transfers. This script modifies the raw data by changing values of "number_transfers_orig_board"/"number_transfers_alight_dest" from 4 to 3.

[updatea_canonical_route_crosswalk.ipynb](updatea_canonical_route_crosswalk.ipynb): a review of `canonical_route_crosswalk.csv` was conducted in April 2021 to 1) correct the operators and technologies of some routes, 2) use different fields to represent canonical_operator and technology in less granular categories (`canonical_operator`, `technology`) and in detail (`operator_detail`, `technology_detail`). The reference file is stored in [Box](https://mtcdrive.box.com/s/d7hmg5t29p50l5tz5obevsppvxc905at).
