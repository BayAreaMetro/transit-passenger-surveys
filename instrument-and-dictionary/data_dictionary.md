
# Data Dictionary

Below is table describing the key variables found in MTC's transit passenger survey public release file.

<br/>

| **Variable**        | **Explanation**                                   | **Notes**                                                                                                                     |
|:--------------------|:--------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------|
| Unique_ID           | Unique record ID                                  | Concatenation of survey record, operator, and year                                                                            |
| access_mode         | Mode of access to very first transit encounter    | Access mode to the first transit vehicle, not necessarily the surveyed vehicle if the trip has a transit transfer             |
| depart_hour         | Hour leaving home prior to the transit trip       |                                                                                                                               |
| dest_purp           | Destination trip purpose                          |                                                                                                                               |
| direction           | Travel direction of surveyed vehicle              |                                                                                                                               |
| egress_mode         | Mode of egress from very last transit encounter   | Egress mode from the last transit vehicle, not necessarily the surveyed vehicle if the trip has a transit transfer            |
| eng_proficient      | English proficiency                               |                                                                                                                               |
| fare_category       | Fare category                                     |                                                                                                                               |
| fare_medium         | Fare medium                                       |                                                                                                                               |
| first_board_lat     | Latitude of first transit boarding location       |                                                                                                                               |
| first_board_lon     | Longitude of first transit boarding location      |                                                                                                                               |
| gender              | Passenger gender                                  |                                                                                                                               |
| hispanic            | Hispanic/Latino status of passenger               |                                                                                                                               |
| household_income    | Income range of passenger's household             |                                                                                                                               |
| interview_language  | Language survey interview was conducted           |                                                                                                                               |
| last_alight_lat     | Latitude of last transit alighting location       |                                                                                                                               |
| last_alight_lon     | Longitude of first boarding location              |                                                                                                                               |
| onoff_enter_station | Rail boarding station                             |                                                                                                                               |
| onoff_exit_station  | Rail alighting station                            |                                                                                                                               |
| orig_purp           | Orgin trip purpose                                |                                                                                                                               |
| persons             | Number of persons in passenger's household        |                                                                                                                               |
| return_hour         | Hour next expected home after transit trip        |                                                                                                                               |
| route               | Transit route                                     |                                                                                                                               |
| student_status      | Student status                                    |                                                                                                                               |
| survey_alight_lat   | Latitude of survey vehicle alighting location     |                                                                                                                               |
| survey_alight_lon   | Longitude of survey vehicle alighting location    |                                                                                                                               |
| survey_board_lat    | Latitude of survey vehicle boarding location      |                                                                                                                               |
| survey_board_lon    | Longitude of survey vehicle boarding location     |                                                                                                                               |
| survey_type         | Type of survey instrument                         | Tablet personal interview, two-step CATI, and paper                                                                           |
| vehicles            | Number of household vehicles                      |                                                                                                                               |
| weekpart            | Weekday or weekend                                |                                                                                                                               |
| weight              | Boarding weight                                   | A single trip may have multiple boardings - this weight is essentially an "unlinked" boarding weight                          |
| work_status         | Worker status                                     |                                                                                                                               |
| workers             | Number of household workers                       |                                                                                                                               |
| ID                  | Record ID for operator survey                     | Not fully concatenated ID as Unique_ID, above                                                                                 |
| operator            | Transit operator                                  |                                                                                                                               |
| survey_year         | Year survey conducted                             |                                                                                                                               |
| survey_tech         | Survey vehicle technology type                    |                                                                                                                               |
| approximate_age     | Age of passenger                                  | Approximate age, as year born is asked                                                                                        |
| tour_purp           | Tour purpose                                      |                                                                                                                               |
| auto_suff           | Number of household vehicles relative to workers  | Vechicles equal to or greater than workers is auto sufficient, fewer vehicles than workers is auto negotiating                |
| transfer_from       | Operator immediately transferred from             |                                                                                                                               |
| transfer_to         | Operator immediately transferring to              |                                                                                                                               |
| first_board_tech    | Vehicle technology type of first transit boarding |                                                                                                                               |
| last_alight_tech    | Vehicle technology type of last transit alighting |                                                                                                                               |
| path_access         | Access mode in aggregated drive, walk, or missing | Bike access aggregated into drive category here                                                                               |
| path_egress         | Egress mode in aggregated drive, walk, or missing | Bike egress aggregated into drive category here                                                                               |
| path_line_haul      | Path line haul mode                               | COM=commuter rail, EXP=express bus, HVY=heavy rail, LOC=local bus, LRF=light rail/ferry                                       |
| path_label          | Full path                                         | Concatenation of path access, line haul, and path egress                                                                      |
| boardings           | Number of boardings in transit trip               |                                                                                                                               |
| race                | Race of passenger                                 |                                                                                                                               |
| language_at_home    | Language spoken at home                           |                                                                                                                               |
| day_of_the_week     | Day of the week survey occurred                   |                                                                                                                               |
| field_start         | First day of data collection for survey           |                                                                                                                               |
| field_end           | Last day of data collection for survey            |                                                                                                                               |
| day_part            | Time period of the day survey was administered    | Early AM=5-6 AM, AM Peak=6-10 AM, Midday=10 AM to 3 PM, PM Peak=3 to 7 PM, Evening=7 PM to midnight, Night = midnight to 5 AM |
| dest_maz            | MAZ of destination                                | Travel model geography                                                                                                        |
| home_maz            | MAZ of passenger's home                           | Travel model geography                                                                                                        |
| orig_maz            | MAZ of origin                                     | Travel model geography                                                                                                        |
| school_maz          | MAZ of passenger's school                         | Travel model geography                                                                                                        |
| workplace_maz       | MAZ of passenger's workplace                      | Travel model geography                                                                                                        |
| dest_taz            | TAZ of destination                                | Travel model geography                                                                                                        |
| home_taz            | TAZ of passenger's home                           | Travel model geography                                                                                                        |
| orig_taz            | TAZ of origin                                     | Travel model geography                                                                                                        |
| school_taz          | TAZ of passenger's school                         | Travel model geography                                                                                                        |
| workplace_taz       | TAZ of passenger's workplace                      | Travel model geography                                                                                                        |
| first_board_tap     | TAP of first boarding location                    | Travel model geography                                                                                                        |
| last_alight_tap     | TAP of last alighting location                    | Travel model geography                                                                                                        |
| trip_weight         | Trip weight                                       | A single trip may have multiple boardings - this weight is essentially an "linked" trip weight, taking into account transfers |
| field_language      | Language of survey interview                      | Equal to interview language, above                                                                                            |
| home_county         | County of passenger's home                        |                                                                                                                               |
| workplace_county    | County of passenger's workplace                   |                                                                                                                               |
| school_county       | County of passenger's school                      |                                                                                                                               |
| survey_time         | Time survey conducted                             | This may be survey start or completion time                                                                                   |
| boardWeight_2015    | Boarding weight scaled to 2015 operator ridership | Not all routes are surveyed and survey years vary. This variable scales ridership to MTC Statistical Summary 2015 totals.     || tripWeight_2015     | linked trip weight for “boardWeight_2105”, above  | Accounts for transfers                                                                                                        |
| final_boardWeight_2015| 2015 board weight adjusted with PopulationSim   | [Entropy maximization weight](https://mtcdrive.box.com/s/dvt9uqukc868v1b51ginvrwqlbfkeqnz),so transfers between surveys agree || final_tripWeight_2015 | linked trip weight for “final_boardWeight_2015” | Accounts for transfers                                                                                                        |
| final_expansionFactor | Expansion factor                                | Expansion factor to convert "tripWeight_2015" to "final_tripWeight_2015"                                                      |
                              
<br/>  

[Return to summary](README.md)
A description of this work is summarized [here]