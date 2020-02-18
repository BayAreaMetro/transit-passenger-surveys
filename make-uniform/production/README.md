## onbard surveys data process

### Steps

* Obtain raw survey data from vendors.
* Format raw survey data into a CSV file. Note: no pound or single quote in data.
* Update relevant index files:
  + "canonical_route_crosswalk.csv.": add new transit routes and define the canonical route name, canonical operator name, and technology following the standard format.
  + "rail_names_inputs.csv": add new survey with corresponding fields.
  + "Dictionary for Standard Database.csv": add the variable names used in the new survey.
* Run the "Build Standard Database.Rmd" file. Note: update file directories, operator dictionary, etc. as specified in the script.


### Reference files
["canonical_route_crosswalk.csv"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/production/canonical_route_crosswalk.csv): maps transit route names used in different 
surveys (*"survey name"*) to standard transit route names (*"canonical_name"*), operator names (*"canonical_operator"*), and *"technology"* - heavy rail, local bus, express, light rail, ferry, etc.
Need to update when new surveys are added.

["rail_names_inputs.csv"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/production/rail_names_inputs.csv):_______.
Need to update when new surveys are added (?). 

["Dictionary for Standard Database.csv"](https://github.com/BayAreaMetro/onboard-surveys/blob/master/make-uniform/production/Dictionary%20for%20Standard%20Database.csv): maps survey variable names in different surveys to standard survey variables. 
Need to update when new surveys are added.

["Passenger_Railway_Stations_2018.shp"](https://mtcdrive.app.box.com/file/336758231534): lists the agency name, mode (Rapid Rail, Light Rail, Commuter Rail), lat/lon coordinates for all rail stations.
Don't need to update now. Need to update when new stations are added (?).

["taps_lat_long.csv"](https://mtcdrive.app.box.com/file/325990811451): lists the lat/lon, county designation, and type of TAPs.

["tazs.shp"](https://mtcdrive.app.box.com/file/325991964206): TAZs inventory with geometry metrics.
Don't need to update now.

["mazs.shp"](https://mtcdrive.app.box.com/file/325995833087): MAZs inventory with geometry metrics.
Don't need to update now.

