* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* CreateRegionalData.sas                                                    
*                                                                                            
* Purpose: Combine agency-specific datasets into a regional dataset.  Each agency will have
*          a script dedicated to building a database from the raw data files (see 
*          BuildDatabase_AGENCY.sas) as well as a script to create a standard dataset from
*          the agency-specific data (CreateUsableData_AGENCY.sas).  This script combines
*          the agency-specific data into a regional database.
*          Work in progress. 
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 02 XX)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set up the directories;
%let tap_geocode_filename = 'M:\Data\OnBoard\Data and Reports\Geocoding Engine\TAP Geocode\obs_boarding_alighting_results.csv'; 
%let output_directory = M:\Data\OnBoard\Data and Reports\_working\Data summaries;
run;

* Read in data;

* ACE;
data aceA; set OnBoard.ace_ready;

run;

* AC transit;
data acA; set OnBoard.ac_ready;
   
   * Shorter long strings set during manual read or raw data;
   short_race = put(race,19.);
   drop race;
   rename short_race = race;

   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_language = put(language_at_home,24.);
   drop language_at_home;
   rename short_language = language_at_home;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_field_lang = put(field_language,7.);
   drop field_language;
   rename short_field_lang = field_language;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

   * Add English proficiency, which was added to subsequent surveys;
   eng_proficient = put('Missing',10.);

run;

* BART;
data bartA; set OnBoard.bart_ready;

run;


* County Connection;
data ccA; set OnBoard.cc_ready;

   * Make string length consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

   * Add English proficiency, which was added to subsequent surveys;
   eng_proficient = put('Missing',10.);

* Santa Rosa City Bus;
data srcbA; set OnBoard.srcb_ready;

   * Make route a string;
   char_route = put('odd work around',100.);
   char_route = route;
   drop route;
   rename char_route = route;

   * Make string length consistent with others;
   char_direction = put(direction,20.);
   drop direction;
   rename char_direction = direction;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

   * Add English proficiency, which was added to subsequent surveys;
   eng_proficient = put('Missing',10.);

run;

* Napa Vine;
data napaA; set OnBoard.napa_ready;
run;

* Petaluma Transit;
data petA; set OnBoard.pet_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

   * Add English proficiency, which was added to subsequent surveys;
   eng_proficient = put('Missing',10.);

   short_day_of_the_week = put(day_of_the_week,10.);
   drop day_of_the_week;
   rename short_day_of_the_week = day_of_the_week;

run;

* SamTrans;
data samA; set OnBoard.sam_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* Sonoma Transit;
data sonA; set OnBoard.son_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

   * Add English proficiency, which was added to subsequent surveys;
   eng_proficient = put('Missing',10.);

   short_day_of_the_week = put(day_of_the_week,10.);
   drop day_of_the_week;
   rename short_day_of_the_week = day_of_the_week;

run;

* Tri Delta;
data triA; set OnBoard.tri_ready;
run;


* Union City;
data unionA; set OnBoard.union_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   short_day_of_the_week = put(day_of_the_week,10.);
   drop day_of_the_week;
   rename short_day_of_the_week = day_of_the_week;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* LAVTA;
data lavtaA; set OnBoard.lavta_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   short_day_of_the_week = put(day_of_the_week,10.);
   drop day_of_the_week;
   rename short_day_of_the_week = day_of_the_week;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* Golden Gate Transit Bus;
data ggtbA; set OnBoard.ggtb_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;   

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* Golden Gate Transit Ferry;
data ggtfA; set OnBoard.ggtf_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* WETA;
data wetaA; set OnBoard.weta_ready;

   * Make string lengths consistent with others;
   char_route = put(route,100.);
   drop route;
   rename char_route = route;

   short_daypart = put(daypart,10.);
   drop daypart;
   rename short_daypart = daypart;

   short_direction = put(direction,20.);
   drop direction;
   rename short_direction = direction;

   short_sex = put(sex,10.);
   drop sex;
   rename short_sex = sex;

   short_eng_proficient = put(eng_proficient,10.);
   drop eng_proficient;
   rename short_eng_proficient = eng_proficient;

   long_transfer_from = put(transfer_from,45.);
   drop transfer_from;
   rename long_transfer_from = transfer_from;

   long_transfer_to = put(transfer_to,45.);
   drop transfer_to;
   rename long_transfer_to = transfer_to;

run;

* Combine the data;
proc append base = acA data = aceA   force;
proc append base = acA data = bartA  force;
proc append base = acA data = ccA    force;
proc append base = acA data = srcbA  force;
proc append base = acA data = petA   force;
proc append base = acA data = samA   force;
proc append base = acA data = unionA force;
proc append base = acA data = sonA   force;
proc append base = acA data = lavtaA force;
proc append base = acA data = ggtbA  force;
proc append base = acA data = ggtfA  force;
proc append base = acA data = wetaA  force;
proc append base = acA data = napaA  force;
proc append base = acA data = triA   force;
run;

data OnBoard.regional_ready; set acA;
   
   * Make directions consistent;
   if TRIM(direction) = 'COUNTERCLO' then direction = 'COUNTERCLOCKWISE';
   if TRIM(direction) = 'COUNTERCLOC' then direction = 'COUNTERCLOCKWISE';
   if TRIM(direction) = 'NORTH' then direction = 'NORTHBOUND';
   if TRIM(direction) = 'SOUTH' then direction = 'SOUTHBOUND';
   if TRIM(direction) = 'EAST'  then direction = 'EASTBOUND';
   if TRIM(direction) = 'WEST'  then direction = 'WESTBOUND';

   if TRIM(direction) = '8A' then direction = 'COUNTERCLOCKWISE';
   if TRIM(direction) = '8B' then direction = 'CLOCKWISE';

   if TRIM(direction) = 'AM' then direction = 'LOOP';
   if TRIM(direction) = 'PM' then direction = 'LOOP';

   * Rename sex gender;
   rename sex = gender;

   * Add a missing race;
   if weight ^= . and race = ' ' then race = 'Missing';

   * Give a military time to the surveyed dayparts;
   daypart_start = 0;
   daypart_end   = 0;
   if TRIM(daypart) = 'EARLY AM' then daypart_start = 5;
   if TRIM(daypart) = 'EARLY AM' then daypart_end   = 6;

   if TRIM(daypart) = 'AM PEAK' then daypart_start = 6;
   if TRIM(daypart) = 'AM PEAK' then daypart_end   = 10;

   if TRIM(daypart) = 'MIDDAY' then daypart_start = 10;
   if TRIM(daypart) = 'MIDDAY' then daypart_end   = 15;

   if TRIM(daypart) = 'PM PEAK' then daypart_start = 15;
   if TRIM(daypart) = 'PM PEAK' then daypart_end   = 19;

   if TRIM(daypart) = 'NIGHT' then daypart_start = 19;
   if TRIM(daypart) = 'NIGHT' then daypart_end   = 21;

   trip_weight = weight;
   if boardings > 0 then trip_weight = weight / boardings;

run;

proc freq data = OnBoard.regional_ready; tables direction daypart daypart*operator;
proc freq data = OnBoard.regional_ready; tables survey_type survey_mode;
proc freq data = OnBoard.regional_ready; tables transfer_to transfer_from;
proc freq data = OnBoard.regional_ready; tables operator;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 0: Add TAP Geo-codes;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

proc import datafile = "&tap_geocode_filename."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 5000;
run;

data tapGeoA; set input; keep unique_id board_tap alight_tap;

  rename board_tap = first_boarding_tap;
  rename alight_tap = last_alighting_tap;

data tapGeoB; set tapGeoA;

  id = input(substr(unique_id,1,find(unique_id,"_")-1),best12.);
  operator = '12345678901234567890123456789012345678901234567890';
  operator = substr(unique_id,find(unique_id,"_")+1,length(unique_id));

run;

data readyA; set OnBoard.regional_ready; 

proc sort data = tapGeoB; by id operator; 
proc sort data = readyA; by id operator;

data readyB; merge readyA tapGeoB; by id operator;
run;

data OnBoard.regional_ready; set readyB; drop unique_id;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 1: QA/QC Summaries;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc freq data = readyA; tables daypart daypart*operator;
run;

proc freq data = readyA; tables hispanic hispanic*operator;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 2: Race by hispanic by operator by weekpart;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc summary data = readyA nway;
   class race hispanic household_income operator weekpart;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;

proc export data = write
   outfile = "&output_directory.\RaceHispanicByOperator.csv"
   replace;
run;

proc freq data = readyA; tables operator race hispanic household_income weekpart day_of_the_week;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 3: Access,Egress & Boardings characteristics;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc summary data = readyA nway;
   class operator weekpart access_mode egress_mode boardings;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\AccessEgressTransfers.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 4:  Time leaving home, Time returning home, & Tour purpose;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc summary data = readyA nway;
   class operator weekpart depart_hour return_hour tour_purp;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\DepartReturnPurpose.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 5:  Auto sufficiency, Work status, Student status, Transfers;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc summary data = readyA nway;
   class operator weekpart autoSuff boardings work_status student_status;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\AutoWorkStudent.csv"
   replace;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 6:  Age, Fare media, Fare category;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   survey_type = 'brief' then delete;

proc freq data = readyA; tables fare_category;
run;

proc summary data = readyA nway;
   class operator weekpart age fare_medium fare_category;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\AgeMediumCategory.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 7:  County of Residence, School, & Workplace;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;
run;

* Compute County from the TAZ;
data readyB; set readyA;

   homeCounty = put('Missing',30.);
   if homeTAZ > 0     and homeTAZ < 191 then homeCounty = 'San Francisco';
   if homeTAZ >= 191  and homeTAZ < 347  then homeCounty = 'San Mateo';
   if homeTAZ >= 347  and homeTAZ < 715  then homeCounty = 'Santa Clara';
   if homeTAZ >= 715  and homeTAZ < 1040 then homeCounty = 'Alameda';
   if homeTAZ >= 1040 and homeTAZ < 1211 then homeCounty = 'Contra Costa';
   if homeTAZ >= 1211 and homeTAZ < 1291 then homeCounty = 'Solano';
   if homeTAZ >= 1291 and homeTAZ < 1318 then homeCounty = 'Napa';
   if homeTAZ >= 1318 and homeTAZ < 1404 then homeCounty = 'Sonoma';
   if homeTAZ >= 1404 then homeCounty = 'Marin'; 

   workCounty = put('Missing',30.);
   if workTAZ > 0     and workTAZ < 191 then workCounty = 'San Francisco';
   if workTAZ >= 191  and workTAZ < 347  then workCounty = 'San Mateo';
   if workTAZ >= 347  and workTAZ < 715  then workCounty = 'Santa Clara';
   if workTAZ >= 715  and workTAZ < 1040 then workCounty = 'Alameda';
   if workTAZ >= 1040 and workTAZ < 1211 then workCounty = 'Contra Costa';
   if workTAZ >= 1211 and workTAZ < 1291 then workCounty = 'Solano';
   if workTAZ >= 1291 and workTAZ < 1318 then workCounty = 'Napa';
   if workTAZ >= 1318 and workTAZ < 1404 then workCounty = 'Sonoma';
   if workTAZ >= 1404 then workCounty = 'Marin'; 

   schoolCounty = put('Missing',30.);
   if schoolTAZ         and schoolTAZ < 191 then schoolCounty = 'San Francisco';
   if schoolTAZ >= 191  and schoolTAZ < 347  then schoolCounty = 'San Mateo';
   if schoolTAZ >= 347  and schoolTAZ < 715  then schoolCounty = 'Santa Clara';
   if schoolTAZ >= 715  and schoolTAZ < 1040 then schoolCounty = 'Alameda';
   if schoolTAZ >= 1040 and schoolTAZ < 1211 then schoolCounty = 'Contra Costa';
   if schoolTAZ >= 1211 and schoolTAZ < 1291 then schoolCounty = 'Solano';
   if schoolTAZ >= 1291 and schoolTAZ < 1318 then schoolCounty = 'Napa';
   if schoolTAZ >= 1318 and schoolTAZ < 1404 then schoolCounty = 'Sonoma';
   if schoolTAZ >= 1404 then schoolCounty = 'Marin'; 


proc freq data = readyB; tables homeCounty workCounty schoolCounty homeCounty*operator;
run;

proc summary data = readyB nway;
   class operator weekpart homeCounty workCounty schoolCounty work_status student_status;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\HomeWorkSchoolCounty.csv"
   replace;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 8:  Language at home, survey language, english proficiency;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc freq data = readyA; tables interview_language language_at_home field_language eng_proficient;
run;

proc summary data = readyA nway;
   class operator weekpart interview_language language_at_home field_language eng_proficient;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\Language.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 9:  Field Dates and day of week;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc freq data = readyA; tables operator field_start field_end; 
          format field_start worddate18.;
		  format field_end   worddate18.;
run;

proc summary data = readyA nway;
   class operator weekpart field_start field_end day_of_the_week;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
   format field_start worddate18.;
   format field_end   worddate18.;
run;

proc export data = write
   outfile = "&output_directory.\FieldDates.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 10:  First Boarding and Last Alighting;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

proc summary data = readyA nway;
   class operator weekpart first_boarding_x first_boarding_y last_alighting_x last_alighting_y;
   var weight trip_weight;
   output out = write
   sum = weight trip_weight;
run;

proc export data = write
   outfile = "&output_directory.\BoardingAndAlighting.csv"
   replace;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 11:  Distribution of Expansion Weights;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;

data write; set readyA; keep id operator weekpart weight;

proc export data = write
   outfile = "&output_directory.\DistributionOfWeights.csv"
   replace;
run;







