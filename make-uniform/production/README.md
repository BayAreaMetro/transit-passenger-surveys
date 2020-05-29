## onboard surveys data process

### Steps

* Obtain raw survey data from vendors.
* Format raw survey data into a CSV file. Note: no pound or single quote in data.
* Update relevant index files:
  + "canonical_route_crosswalk.csv.": add new transit routes and define the canonical route name, canonical operator name, and technology following the standard format.
  + "Dictionary for Standard Database.csv": add the variable names used in the new survey.
* Run the "Build Standard Database.R" script. This script contains notes where user interventions are needed ("# _User Intervention_") and where file directories, reference files, etc. need to be updated when adding a new operator's survey data.


### Reference files
["canonical_route_crosswalk.csv"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/production/canonical_route_crosswalk.csv): maps transit route names used in different 
surveys (*"survey name"*) to standard transit route names (*"canonical_name"*), operator names (*"canonical_operator"*), and *"technology"* - heavy rail, local bus, express, light rail, ferry, etc. 
Each route in the Bay Area has a `canonical` or reference name. Each route name from a survey should be matched to the `canonical` route name. Using this match to assign technologies to each of the routes collected in the survey allows travel model path labels to be assigned to each trip. When the operator data is read in, it assumes every route in the survey uses the same technology (e.g., all Muni routes are local bus). However, some operators operate multiple technologies. These bespoke technologies are added in this file. 
When a new operator's survey data is added, update this file either by directly adding rows to the .csv file, or using ["standard_route_name_db"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/canonical_names/standard_route_name_db.R) in folder ["canonical_names"](https://github.com/BayAreaMetro/onboard-surveys/tree/master/make-uniform/canonical_names). 
Please note that the `canonical` station names for BART and Caltrain are stored in the `f_canonical_station_path` shape file and appended via spatial matching to other surveys.
 
["Dictionary for Standard Database.csv"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/production/Dictionary%20for%20Standard%20Database.csv): maps survey variable names in different surveys to standard survey variables. 
When adding a new operator's survey data, update the dictionary. The existing entries in the dictionary *should* explicate the expected task.

["Passenger_Railway_Stations_2018.shp"](https://mtcdrive.app.box.com/file/336758231534): lists the agency name, mode (Rapid Rail, Light Rail, Commuter Rail), lat/lon coordinates for all rail stations.
Don't need to update now. Need to update when new stations are added (?).

["taps_lat_long.csv"](https://mtcdrive.app.box.com/file/325990811451): lists the lat/lon, county designation, and type of TAPs.

["tazs.shp"](https://mtcdrive.app.box.com/file/325991964206): TAZs inventory with geometry metrics.
Don't need to update now.

["mazs.shp"](https://mtcdrive.app.box.com/file/325995833087): MAZs inventory with geometry metrics.
Don't need to update now.

