## onboard surveys data process

### Steps

1. Obtain raw survey data from vendors.
2. Format raw survey data into a CSV file. Note: no pound or single quote in data. Preprocessing scripts for fixing the raw data are in [preprocess](preprocess)
3. Update relevant index files:
   1. [canonical_route_crosswalk.csv](canonical_route_crosswalk.csv) add new transit routes and define the canonical route name, canonical operator name, and technology following the standard format.
   2. [Dictionary_for_Standard_Database.csv](Dictionary_for_Standard_Database.csv): add the variable names used in the new survey.
4. Run the [Build_Standard_Database.R](Build_Standard_Database.R) script. This script contains notes where user interventions are needed ("# _User Intervention_") and where file directories, reference files, etc. need to be updated when adding a new survey's data.
5. Check that things look correct for the complete dataset by refreshing [TransitPassengerSurvey_fullStandardizedDataset.twb](TransitPassengerSurvey_fullStandardizedDataset.twb), which is published internally [here](https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/workbooks/1896779?:origin=card_share_link) 


### Reference files

* [canonical_route_crosswalk.csv](canonical_route_crosswalk.csv): maps transit route names used in different surveys (*`survey_route_name`*) to standard transit route names (*`canonical_route`*), operator names (*`canonical_operator`, `operator_detail`*), and *`technology`/,`technology_detail`* - heavy rail, local bus, express, light rail, ferry, etc. 
Each route in the Bay Area has a `canonical` or reference name. Each route name from a survey should be matched to the `canonical` route name. Using this match to assign technologies to each of the routes collected in the survey allows travel model path labels to be assigned to each trip. When the survey data is read in, it assumes every route in the survey uses the same technology (e.g., all Muni routes are local bus). However, some operators operate multiple technologies. These bespoke technologies are added in this file. 

* When a new survey's data is added, update this file either by directly adding rows to the .csv file, or using [standard_route_name_db.R](../canonical_names/standard_route_name_db.R) in folder [canonical_names](../canonical_names).  Please note that the `canonical` station names for BART and Caltrain are stored in the `f_canonical_station_path` shape file and appended via spatial matching to other surveys.
 
* [Dictionary_for_Standard_Database.csv](Dictionary_for_Standard_Database.csv): maps survey variable names in different surveys to standard survey variables.  When adding a new survey's data, update the dictionary. The existing entries in the dictionary *should* explicate the expected task.

  * The dictionary is loaded into the [tableau](TransitPassengerSurvey_fullStandardizedDataset.twb) and [viewable internally here](https://10ay.online.tableau.com/t/metropolitantransportationcommission/views/TransitPassengerSurvey_fullStandardizedDataset/Dictionary).

  * **Survey Metadata** required in the data and dictionary:

    1. `ID` represents the survey taker ID. This is used to create `unique_ID` (which also includes `survey_name` and `survey_year`). Note: this field must be named ID (case-insensitive) in the survey data as well.
    1. `weight` represents [TODO: get details on this]
    1. `date_string` represents the interview date/time. This is used to create the output variables `day_of_the_week`, `weekpart`, `field_start`, `field_end`.
    1. `time_string` represents the interview time which is standardized to `survey_time`
    1. `time_period` is the time period of the boarding time for the surveyed vehicle; [Build_Standard_Database.R](Build_Standard_Database.R) standardizes it to one of the [MTC Travel Model time periods](https://github.com/BayAreaMetro/modeling-website/wiki/TimePeriods) or *`WEEKEND`*.
       * The output variable `day_part` is based upon `time_period` if it exists and it's set based upon `survey_time` otherwise.
    1. **Non Operator-based Surveys**
       * The `survey_tech` variable must be included in the survey dataset. Each row should have one of the following values: `heavy rail`, `commuter rail`, `ferry`, `light rail`, `express bus` or `local bus`. It does not need to be in the `Dictionary_for_Standard_Database.csv`
       * The `canoncial_operator` variable must be included in the survey dataset. It does not need to be in the `Dictionary_for_Standard_Database.csv`.

* [Passenger_Railway_Stations_2018.shp (internal link)](https://mtcdrive.box.com/s/dq6f8ca95os4sbsd9aste54dx0c3zrks): lists the agency name, mode (Rapid Rail, Light Rail, Commuter Rail), lat/lon coordinates for all rail stations. Don't need to update now. Need to update when new stations are added (?).

* [tazs.shp (internal link)](https://mtcdrive.box.com/s/42s7lvbq0snvvjeyigi36z295rsmcpd4): TM2 TAZs inventory with geometry metrics. Don't need to update now.

* [mazs.shp (internal link)](https://mtcdrive.box.com/s/k7tpfjq11pqpfewdpexqfz7uyw6p7nx9): MAZs inventory with geometry metrics.
Don't need to update now.
