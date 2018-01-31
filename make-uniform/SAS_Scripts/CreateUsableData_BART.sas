* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* CreateUsableData.sas                                                    
*                                                                                            
* Purpose: Process and combine the raw consultant-delivered data set into a usable dataset.
*          See BuildDatabases.sas for script to create SAS databases.  Specific to the BART
           Pilot.
*          Work in progress.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 08 XX)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the file locations;
%let gis_geocode     = 'M:\Data\OnBoard\Data and Reports\BART\Geocode\gis_geocode.csv';
%let output_directory = M:\Data\OnBoard\Data and Reports\BART\_working;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 1: Compute tour purpose and trip purpose per Travel Model One designations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Trip purpose;
data dataA; set OnBoard.rawBart;

   orig_purp = put('missing',25.);
   dest_purp = put('missing',25.);

   if ORIGIN_PLACE_TYPE_CODE = 1  then orig_purp = "home";
   if ORIGIN_PLACE_TYPE_CODE = 2  then orig_purp = "work";
   if ORIGIN_PLACE_TYPE_CODE = 3  then orig_purp = "work-related";
   if ORIGIN_PLACE_TYPE_CODE = 4  then orig_purp = "university";
   if ORIGIN_PLACE_TYPE_CODE = 5  then orig_purp = "high school";
   if ORIGIN_PLACE_TYPE_CODE = 5  and YEAR_BORN > 1999 then orig_purp = "grade school";
   if ORIGIN_PLACE_TYPE_CODE = 6  then orig_purp = "shopping";
   if ORIGIN_PLACE_TYPE_CODE = 7  then orig_purp = "other discretionary";
   if ORIGIN_PLACE_TYPE_CODE = 8  then orig_purp = "eat out";
   if ORIGIN_PLACE_TYPE_CODE = 9  then orig_purp = "social recreation";
   if ORIGIN_PLACE_TYPE_CODE = 10 then orig_purp = "social recreation";
   if ORIGIN_PLACE_TYPE_CODE = 11 then orig_purp = "other maintenance";
   if ORIGIN_PLACE_TYPE_CODE = 12 then orig_purp = "other maintenance";
   if ORIGIN_PLACE_TYPE_CODE = 13 then orig_purp = "other discretionary";
   if ORIGIN_PLACE_TYPE_CODE = 14 then orig_purp = "other discretionary";
   if ORIGIN_PLACE_TYPE_CODE = 15 then orig_purp = "escorting";

   if DESTINATION_PLACE_TYPE_CODE = 1  then dest_purp = "home";
   if DESTINATION_PLACE_TYPE_CODE = 2  then dest_purp = "work";
   if DESTINATION_PLACE_TYPE_CODE = 3  then dest_purp = "work-related";
   if DESTINATION_PLACE_TYPE_CODE = 4  then dest_purp = "university";
   if DESTINATION_PLACE_TYPE_CODE = 5  then dest_purp = "high school";
   if DESTINATION_PLACE_TYPE_CODE = 5  and YEAR_BORN > 1999 then dest_purp = "grade school";
   if DESTINATION_PLACE_TYPE_CODE = 6  then dest_purp = "shopping";
   if DESTINATION_PLACE_TYPE_CODE = 7  then dest_purp = "other discretionary";
   if DESTINATION_PLACE_TYPE_CODE = 8  then dest_purp = "eat out";
   if DESTINATION_PLACE_TYPE_CODE = 9  then dest_purp = "social recreation";
   if DESTINATION_PLACE_TYPE_CODE = 10 then dest_purp = "social recreation";
   if DESTINATION_PLACE_TYPE_CODE = 11 then dest_purp = "other maintenance";
   if DESTINATION_PLACE_TYPE_CODE = 12 then dest_purp = "other maintenance";
   if DESTINATION_PLACE_TYPE_CODE = 13 then dest_purp = "other discretionary";
   if DESTINATION_PLACE_TYPE_CODE = 14 then dest_purp = "other discretionary";
   if DESTINATION_PLACE_TYPE_CODE = 15 then dest_purp = "escorting";

run;

proc freq data = dataA; tables orig_purp dest_purp;
run;

* Tour purpose;
data dataA; set dataA;
   tour_purp = put('missing',25.);


* Start with the fairly straightforward purposes;
data dataB; set dataA;

  * workers;
  if tour_purp = 'missing' and orig_purp = "home" and dest_purp = "work" then tour_purp = "work";
  if tour_purp = 'missing' and orig_purp = "work" and dest_purp = "home" then tour_purp = "work";

  * students;
  if tour_purp = 'missing' and orig_purp = "grade school" or dest_purp = "grade school" then tour_purp = "grade school";
  if tour_purp = 'missing' and orig_purp = "high school" or dest_purp = "high school" then tour_purp = "high school";

  * non-working university students;
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * non-workers, non-students, home-based travel (set home to home as other discretionary);
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp = "home" and dest_purp^= "home" then tour_purp = dest_purp;
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp = "home" and dest_purp = "home" then tour_purp = "other discretionary";
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp^= "home" and dest_purp = "home" then tour_purp = orig_purp;
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp^= "home" and dest_purp = orig_purp then tour_purp = orig_purp;

  * non-workers, non-students, non-home-based travel, assign the orig_purp the tour_purp, except escorting;
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp^= "home" and dest_purp^= "home" and (orig_purp = "escorting" or dest_purp = "escorting") then tour_purp = "escorting";
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and STUDENT_STATUS = 'No' and orig_purp^= "home" and dest_purp^= "home" and orig_purp^= "escorting" and dest_purp^= "escorting" then tour_purp = orig_purp;

run;

* Use the fairly straightforward information from the work and school questions;
data dataC; set dataB; 

  * If no work and university is present at all, then university;
  if tour_purp = 'missing' and BEEN_TO_WORK = 'No' and GOING_TO_WORK = 'No' and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * if work (school) before then home, assume work (school) tour;
  if tour_purp = 'missing' and BEEN_TO_WORK = 'Yes' and dest_purp = "home"  then tour_purp = "work";
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and YEAR_BORN < 1996 and BEEN_2SCHOOL_TODAY = 'Yes' and dest_purp = "home" then tour_purp = "university"; 

  * if work (school) after, and home before, assume work (school) tour;
  if tour_purp = 'missing' and GOING_TO_WORK = 'Yes' and orig_purp = "home" then tour_purp = "work";
  if tour_purp = 'missing' and EMPLOYMENT_STATUS = 'No' and YEAR_BORN < 1996 and WILL_GO2SCHOOL_TODAY = 'Yes' and orig_purp = "home" then tour_purp = "university";

  * if no work before or after, but work is a leg, assume work tour (already covered for university);
  if tour_purp = 'missing' and BEEN_TO_WORK = 'No' and GOING_TO_WORK = 'No' and (orig_purp = "work" or dest_purp = "work") then tour_purp = "work";

  * if start at work, no work after, assume a work tour (already covered for university);
  if tour_purp = 'missing' and orig_purp = "work" and GOING_TO_WORK = 'No' then tour_purp = "work";

  * if start at work, no work after, assume a work tour (already covered for university);
  if tour_purp = 'missing' and GOING_TO_WORK = 'No' and orig_purp = "work" then tour_purp = "work";

  * Use the return to work for at work tours, assuming anyone going back to work but not stopping at home is making;
  if tour_purp = 'missing' and BEEN_TO_WORK = 'Yes' and orig_purp^= "home" and dest_purp= "work" then tour_purp = "at work";
  if tour_purp = 'missing' and GOING_TO_WORK = 'Yes' and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";


run;

* Move to the ones that require more faith;
data dataD; set dataC;
   
   * If you're still left, assume working before or after trip (which does not have a home or work end) puts you on a work tour;
   if tour_purp = 'missing' and (BEEN_TO_WORK = 'Yes' or GOING_TO_WORK = 'Yes') then tour_purp = "work";

   * If you're still left, and one end is home, then other end is purpose;
   if tour_purp = 'missing' and orig_purp = "home" then tour_purp = dest_purp;
   if tour_purp = 'missing' and dest_purp = "home" then tour_purp = orig_purp;

   * If you're still left, then orig_purp;
   if tour_purp = 'missing' then tour_purp = orig_purp;

   * Change home to other discretionary;
   if tour_purp = "home" then tour_purp = "other discretionary";

run; 

* Check frequencies;
proc freq data = dataD; tables tour_purp;
run;


data OnBoard.bart_purp; set dataD;
   label orig_purp = "Travel Model One Activity at origin"
         dest_purp = "Travel Model One Activity at destination"
         tour_purp = "Travel Model One Tour purpose (approximate)";

run;

proc freq data = dataD; tables HAVE_5_MIN;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 2: Append TAZ geo-codes to home, work, school, origin, and destination;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Read in CATI geo-codes (created by geo-code engine);
data input; infile "&gis_geocode." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format ID best12.;
			format measure $30.;
			format maz best12.;
			format taz1454 best12.;

			informat ID best32.;
			informat measure $30.;
			informat maz best32.;
			informat taz1454 best32.;

			input    ID  
			         measure $  
			         maz
                     taz1454;

run;

proc freq data = input; tables measure;
run;

* Extract the needed data from the file;
data geoA; set input;

  homeTAZ   = .;
  workTAZ   = .;
  schoolTAZ = .;
  originTAZ = .;
  destTAZ   = .;

  homeMAZ   = .;
  workMAZ   = .;
  schoolMAZ = .;
  originMAZ = .;
  destMAZ   = .;

  if trim(measure) = 'ORIGIN' then originTAZ = taz1454;
  if trim(measure) = 'ORIGIN' then originMAZ = maz;

  if trim(measure) = 'DESTINATION' then destTAZ = taz1454;
  if trim(measure) = 'DESTINATION' then destMAZ = maz;

  if trim(measure) = 'WORKP' then workTAZ = taz1454;
  if trim(measure) = 'WORKP' then workMAZ = maz;

  if trim(measure) = 'SCHOOL' then schoolTAZ = taz1454;
  if trim(measure) = 'SCHOOL' then schoolMAZ = maz;

  if trim(measure) = 'HOME' then workTAZ = taz1454;
  if trim(measure) = 'HOME' then workMAZ = maz;


proc summary data = geoA threads nway;
  class ID;
  var homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
  output out = geoB
  max = homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
run;

data OnBoard.bart_key_loc; set geoB; drop _TYPE_ _FREQ_;
run;
 
data dataA; set OnBoard.bart_purp; keep ID orig_purp dest_purp;
data gisA;  set OnBoard.bart_key_loc; 

proc sort data = dataA threads; by ID;
proc sort data = gisA  threads; by ID;

data gisB; merge dataA gisA; by ID;
run;

data gisC; set gisB;

   if orig_purp = 'home' then homeTAZ = originTAZ;
   if orig_purp = 'home' then homeMAZ = originMAZ;

   if orig_purp = 'work' then workTAZ = originTAZ;
   if orig_purp = 'work' then workMAZ = originMAZ;

   if orig_purp = 'high school'  then schoolTAZ = originTAZ;
   if orig_purp = 'grade school' then schoolTAZ = originTAZ;
   if orig_purp = 'university'   then schoolTAZ = originTAZ;

   if orig_purp = 'high school'  then schoolMAZ = originMAZ;
   if orig_purp = 'grade school' then schoolMAZ = originMAZ;
   if orig_purp = 'university'   then schoolMAZ = originMAZ;

   if dest_purp = 'home' then homeTAZ = destTAZ;
   if dest_purp = 'home' then homeMAZ = destMAZ;

   if dest_purp = 'work' then workTAZ = destTAZ;
   if dest_purp = 'work' then workMAZ = destMAZ;

   if dest_purp = 'high school'  then schoolTAZ = destTAZ;
   if dest_purp = 'grade school' then schoolTAZ = destTAZ;
   if dest_purp = 'university'   then schoolTAZ = destTAZ;

   if dest_purp = 'high school'  then schoolMAZ = destMAZ;
   if dest_purp = 'grade school' then schoolMAZ = destMAZ;
   if dest_purp = 'university'   then schoolMAZ = destMAZ;

run;

proc freq data = gisC; tables homeTAZ;
run;

data OnBoard.bart_key_loc; set gisC; drop orig_purp dest_purp;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 3: Automobile sufficiency;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data dataA; set OnBoard.bart_purp;

proc freq data = dataA; tables EMPLOYED_ADULTS_IN_HH VEH_IN_HH;
run;

* recode variables to be directly usable;
data dataB; set dataA;
   vehicles = .;
   if VEH_IN_HH = 'None' then vehicles = 0;
   if VEH_IN_HH = '1' then vehicles = 1;
   if VEH_IN_HH = '2' then vehicles = 2;
   if VEH_IN_HH = '3' then vehicles = 3;
   if VEH_IN_HH = '4 or more' then vehicles = 4;

   workers = .;
   if EMPLOYED_ADULTS_IN_HH = 'None' then workers = 0;
   if EMPLOYED_ADULTS_IN_HH = '1' then workers = 1;
   if EMPLOYED_ADULTS_IN_HH = '2' then workers = 2;
   if EMPLOYED_ADULTS_IN_HH = '3' then workers = 3;
   if EMPLOYED_ADULTS_IN_HH = '4' then workers = 4;
   if EMPLOYED_ADULTS_IN_HH = '5' then workers = 5;
   if EMPLOYED_ADULTS_IN_HH = '6 or more' then workers = 6;

proc freq data = dataB; tables vehicles workers VEH_IN_HH EMPLOYED_ADULTS_IN_HH vehicles*VEH_IN_HH workers*EMPLOYED_ADULTS_IN_HH;
run;

data dataC; set dataB;
   autoSuff = put('Missing',24.);

   if vehicles = 0 then autoSuff = 'Zero autos';

   if workers > vehicles and vehicles > 0 and workers > 0 then autoSuff = 'Workers > autos';

   if workers <= vehicles and vehicles > 0 and workers > 0 then autoSuff = 'Workers <= autos';

   if workers = 0 and vehicles > 0 then autoSuff = 'Workers <= autos';

data dataD; set dataC;
   if autoSuff = 'Missing' then autoSuff = .;

proc freq data = dataD; tables autoSuff;
run;

data OnBoard.bart_purp_asuff; set dataD;
   label autoSuff = "Travel Model One Automobile Sufficiency";
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 4: Determine mode sequence and Travel Model One mode;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Assign the operator and route;
data dataA; set OnBoard.bart_purp_asuff;

   first_prior_raw   = TRANSFER_FROM_1ST;
   second_prior_raw  = TRANSFER_FROM_2ND;
   third_prior_raw   = .; *TRANSFER_FROM_3RD;
   fourth_prior_raw  = .; *TRANSFER_FROM_4TH;

   first_after_raw  = TRANSFER_TO_1ST;
   second_after_raw = TRANSFER_TO_2ND;
   third_after_raw  = TRANSFER_TO_3RD;
   fourth_after_raw = TRANSFER_TO_4TH;

run;

proc freq data = dataA; tables first_prior_raw first_after_raw;
proc freq data = dataA; tables second_prior_raw second_after_raw;
proc freq data = dataA; tables third_prior_raw third_after_raw;
proc freq data = dataA; tables fourth_prior_raw fourth_after_raw;
run;

* Define a macro to extract operator from the raw string;
%MACRO SET_OPERATOR(INPUT, OUTPUT);

   &OUTPUT = 'None                                         ';
   if index(&INPUT,'AC Transit')        ge 1 then &OUTPUT = 'AC TRANSIT';
   if index(&INPUT,'Amtrak')            ge 1 then &OUTPUT = 'AMTRAK';
   if index(&INPUT,'BART')              ge 1 then &OUTPUT = 'BART';
   if index(&INPUT,'AirBART')           ge 1 then &OUTPUT = 'AIR BART';
   if index(&INPUT,'Capitol Corridor')  ge 1 then &OUTPUT = 'AMTRAK';
   if index(&INPUT,'Caltrain')          ge 1 then &OUTPUT = 'CALTRAIN';
   if index(&INPUT,'County Connection') ge 1 then &OUTPUT = 'COUNTY CONNECTION';
   if index(&INPUT,'Fairfield and')     ge 1 then &OUTPUT = 'FAIRFIELD-SUISUN';
   if index(&INPUT,'Golden Gate Tran')  ge 1 then &OUTPUT = 'GOLDEN GATE TRANSIT';
   if index(&INPUT,'Golden Gate Ferry') ge 1 then &OUTPUT = 'GOLDEN GATE FERRY';
   if index(&INPUT,'Muni')              ge 1 then &OUTPUT = 'MUNI';
   if index(&INPUT,'SamTrans')          ge 1 then &OUTPUT = 'SAMTRANS';
   if index(&INPUT,'SF Bay Ferry')      ge 1 then &OUTPUT = 'SF BAY FERRY';
   if index(&INPUT,'SolTrans')          ge 1 then &OUTPUT = 'SOLTRANS';
   if index(&INPUT,'Tri Delta')         ge 1 then &OUTPUT = 'TRI-DELTA';
   if index(&INPUT,'Union City')        ge 1 then &OUTPUT = 'UNION CITY';
   if index(&INPUT,'WestCAT')           ge 1 then &OUTPUT = 'WESTCAT';
   if index(&INPUT,'VINE')              ge 1 then &OUTPUT = 'NAPA VINE';
   if index(&INPUT,'VTA')               ge 1 then &OUTPUT = 'VTA';

   if index(&INPUT,'AirTrain')          ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'Vacaville')         ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'Berkeley lab')      ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'UCSF')              ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'Hospital Shuttle')  ge 1 then &OUTPUT = 'PRIVATE SHUTTLE';
   if index(&INPUT,'Calistoga')         ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'Lake Transit')      ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'American Canyon')   ge 1 then &OUTPUT = 'OTHER';
   if index(&INPUT,'Saint Helena')      ge 1 then &OUTPUT = 'OTHER';
   
%MEND SET_OPERATOR;
run;

* Define a macro to extract technology from the raw string;
%MACRO SET_TECHNOLOGY(OPERATOR, OUTPUT);

   * Assign a base technology;
   &OUTPUT = put('None',20.);
   if &OPERATOR = 'AC TRANSIT'          then &OUTPUT = 'local bus';
   if &OPERATOR = 'AIR BART'            then &OUTPUT = 'local bus';
   if &OPERATOR = 'AMTRAK'              then &OUTPUT = 'commuter rail';
   if &OPERATOR = 'BART'                then &OUTPUT = 'heavy rail';
   if &OPERATOR = 'CALTRAIN'            then &OUTPUT = 'commuter rail';
   if &OPERATOR = 'COUNTY CONNECTION'   then &OUTPUT = 'local bus';
   if &OPERATOR = 'FAIRFIELD-SUISUN'    then &OUTPUT = 'local bus';
   if &OPERATOR = 'GOLDEN GATE TRANSIT' then &OUTPUT = 'express bus';
   if &OPERATOR = 'GOLDEN GATE FERRY'   then &OUTPUT = 'ferry';
   if &OPERATOR = 'MUNI'                then &OUTPUT = 'local bus';
   if &OPERATOR = 'NAPA VINE'           then &OUTPUT = 'local bus';
   if &OPERATOR = 'SAMTRANS'            then &OUTPUT = 'local bus';
   if &OPERATOR = 'SF BAY FERRY'        then &OUTPUT = 'ferry';
   if &OPERATOR = 'SOLTRANS'            then &OUTPUT = 'local bus';
   if &OPERATOR = 'TRI-DELTA'           then &OUTPUT = 'local bus';
   if &OPERATOR = 'UNION CITY'          then &OUTPUT = 'local bus';
   if &OPERATOR = 'WESTCAT'             then &OUTPUT = 'local bus';
   if &OPERATOR = 'VTA'                 then &OUTPUT = 'local bus';

   if &OPERATOR = 'OTHER'               then &OUTPUT = 'local bus';
   if &OPERATOR = 'PRIVATE SHUTTLE'     then &OUTPUT = 'local bus';
   

%MEND SET_TECHNOLOGY;
run;

* Update technologies based on manual inspection of results;
%MACRO UPDATE_TECH(OPERATOR, RAW, OUTPUT);

   if &OPERATOR = 'MUNI' and INDEX(&RAW,'Light Rail') ge 1 then &OUTPUT = 'light rail';
   if &OPERATOR = 'VTA'  and INDEX(&RAW,'Light Rail') ge 1 then &OUTPUT = 'light rail';

%MEND UPDATE_TECH;
run;


* Apply the operator macro;
data dataB; set dataA;
   
   %SET_OPERATOR(first_prior_raw,  first_prior_operator);
   %SET_OPERATOR(second_prior_raw, second_prior_operator);
   %SET_OPERATOR(third_prior_raw,  third_prior_operator);
   %SET_OPERATOR(fourth_prior_raw, fourth_prior_operator);

   %SET_OPERATOR(first_after_raw,  first_after_operator);
   %SET_OPERATOR(second_after_raw, second_after_operator);
   %SET_OPERATOR(third_after_raw,  third_after_operator);
   %SET_OPERATOR(fourth_after_raw, fourth_after_operator);

run;

proc freq data = dataB; tables first_prior_operator second_prior_operator third_prior_operator fourth_prior_operator;
proc freq data = dataB; tables first_after_operator second_after_operator third_after_operator fourth_prior_operator;
proc freq data = dataB; tables first_prior_raw_other*first_prior_operator;
run;
 
* Apply the technology macro;
data dataC; set dataB;
   
   %SET_TECHNOLOGY(first_prior_operator,  first_prior_mode);
   %SET_TECHNOLOGY(second_prior_operator, second_prior_mode);
   %SET_TECHNOLOGY(third_prior_operator,  third_prior_mode);
   %SET_TECHNOLOGY(fourth_prior_operator, fourth_prior_mode);

   %SET_TECHNOLOGY(first_after_operator,  first_after_mode);
   %SET_TECHNOLOGY(second_after_operator, second_after_mode);
   %SET_TECHNOLOGY(third_after_operator,  third_after_mode);
   %SET_TECHNOLOGY(fourth_after_operator, fourth_after_mode);

run;

proc freq data = dataC; tables first_prior_mode second_prior_mode third_prior_mode;
run;


* Update the special case Muni technologies;
data dataD; set dataC;

   %UPDATE_TECH(first_prior_operator,  first_prior_raw_, first_prior_mode);
   %UPDATE_TECH(second_prior_operator, second_prior_raw, second_prior_mode);
   %UPDATE_TECH(third_prior_operator,  third_prior_raw,  third_prior_mode);
   %UPDATE_TECH(fourth_prior_operator, fourth_prior_raw, fourth_prior_mode);

   %UPDATE_TECH(first_after_operator,  first_after_raw,  first_after_mode);
   %UPDATE_TECH(second_after_operator, second_after_raw, second_after_mode);
   %UPDATE_TECH(third_after_operator,  third_after_raw,  third_after_mode);
   %UPDATE_TECH(fourth_after_operator, fourth_after_raw, fourth_after_mode);
 
run;

proc freq data = dataD; tables first_prior_operator second_prior_operator third_prior_operator fourth_prior_operator;
proc freq data = dataD; tables first_after_operator second_after_operator third_after_operator fourth_prior_operator;

proc freq data = dataD; tables first_prior_mode second_prior_mode third_prior_mode fourth_prior_mode;
proc freq data = dataD; tables first_after_mode second_after_mode third_after_mode fourth_prior_mode;
run;

* Set the transfer to, transfer from, access, egress, and survey mode;
data dataE; set dataD;
   rename ACCESS_MODE = SURVEY_ACCESS_MODE;
   rename EGRESS_MODE = SURVEY_EGRESS_MODE;

data dataF; set dataE;

   * Survey mode;
   survey_mode = put('heavy rail',15.);

   * Transfer to and from;
   transfer_from = first_prior_operator;
   if TRIM(second_prior_operator) ^= 'None' then transfer_from = second_prior_operator;
   if TRIM(third_prior_operator)  ^= 'None' then transfer_from = third_prior_operator;

   transfer_to = first_after_operator;
 
   * Access mode;
   access_mode = put('Missing',23.);
   if ACCESS_MODE_1ST_RTE_CODE = 1 then access_mode = 'walk';
   if ACCESS_MODE_1ST_RTE_CODE = 2 then access_mode = 'bike';
   if ACCESS_MODE_1ST_RTE_CODE = 4 then access_mode = 'pnr';
   if ACCESS_MODE_1ST_RTE_CODE = 5 then access_mode = 'pnr';
   if ACCESS_MODE_1ST_RTE_CODE = 6 then access_mode = 'pnr';
   if ACCESS_MODE_1ST_RTE_CODE = 7 then access_mode = 'knr';
   if ACCESS_MODE_1ST_RTE_CODE = 8 then access_mode = 'knr';
   if ACCESS_MODE_1ST_RTE_CODE = 9 then access_mode = 'knr';
   
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 1 then access_mode = 'walk';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 2 then access_mode = 'bike';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 4 then access_mode = 'pnr';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 5 then access_mode = 'pnr';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 6 then access_mode = 'pnr';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 7 then access_mode = 'knr';
   if access_mode = 'Missing' and ACCESS_MODE_CODE = 8 then access_mode = 'knr';

   * Egress mode;
   egress_mode = put('Missing',23.);
   if EGRESS_MODE_LAST_RTE_CODE = 1 then egress_mode = 'walk';
   if EGRESS_MODE_LAST_RTE_CODE = 2 then egress_mode = 'bike';
   if EGRESS_MODE_LAST_RTE_CODE = 4 then egress_mode = 'pnr';
   if EGRESS_MODE_LAST_RTE_CODE = 5 then egress_mode = 'pnr';
   if EGRESS_MODE_LAST_RTE_CODE = 7 then egress_mode = 'knr';
   
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 1 then egress_mode = 'walk';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 2 then egress_mode = 'bike';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 4 then egress_mode = 'pnr';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 5 then egress_mode = 'pnr';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 6 then egress_mode = 'pnr';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 7 then egress_mode = 'knr';
   if egress_mode = 'Missing' and EGRESS_MODE_CODE = 8 then egress_mode = 'knr';

   * Mode of first transit vehicle boarded;
   first_transit_mode = survey_mode;
   if TRIM(first_prior_mode)  ^= 'None' then first_transit_mode = first_prior_mode;

   * Mode of last transit vehicle boarded;
   last_transit_mode = survey_mode;
   if TRIM(first_after_mode)  ^= 'None' then last_transit_mode = first_after_mode;
   if TRIM(second_after_mode) ^= 'None' then last_transit_mode = second_after_mode;
   if TRIM(third_after_mode)  ^= 'None' then last_transit_mode = third_after_mode;
   if TRIM(fourth_after_mode) ^= 'None' then last_transit_mode = fourth_after_mode;

run;

proc freq data = dataF; tables access_mode survey_mode egress_mode transfer_to transfer_from;
proc freq data = dataF; tables first_transit_mode last_transit_mode;
run;

* Determine the Travel Model One path and set a simplified character sequence;
data dataG; set dataF;

   * Apply the hierarchy (assume bike is more similar to drive);
   path_access = 'X';
   if access_mode = 'walk' then path_access = 'W';
   if access_mode = 'pnr'  then path_access = 'D';
   if access_mode = 'knr'  then path_access = 'D';
   if access_mode = 'bike' then path_access = 'D';

   path_egress = 'X';
   if egress_mode = 'walk' then path_egress = 'W';
   if egress_mode = 'pnr'  then path_egress = 'D';
   if egress_mode = 'knr'  then path_egress = 'D';
   if egress_mode = 'bike' then path_egress = 'D';

   path_line_haul = 'XXX';
   if first_prior_mode = 'commuter rail' or second_prior_mode = 'commuter rail' or third_prior_mode = 'commuter rail' or
      first_after_mode = 'commuter rail' or second_after_mode = 'commuter rail' or third_after_mode = 'commuter rail' or
      survey_mode = 'commuter rail' then
	  path_line_haul = 'COM';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'heavy rail' or second_prior_mode = 'heavy rail' or third_prior_mode = 'heavy rail'  or
      first_after_mode = 'heavy rail' or second_after_mode = 'heavy rail' or third_after_mode = 'heavy rail'  or
      survey_mode = 'heavy rail') then
	  path_line_haul = 'HVY';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'express bus' or second_prior_mode = 'express bus' or third_prior_mode = 'express bus' or 
      first_after_mode = 'express bus' or second_after_mode = 'express bus' or third_after_mode = 'express bus' or 
      survey_mode = 'express bus') then
	  path_line_haul = 'EXP';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'ferry' or second_prior_mode = 'ferry' or third_prior_mode = 'ferry' or
      first_after_mode = 'ferry' or second_after_mode = 'ferry' or third_after_mode = 'ferry' or
      survey_mode = 'ferry') then
	  path_line_haul = 'LRF';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'light rail' or second_prior_mode = 'light rail' or third_prior_mode = 'light rail' or
      first_after_mode = 'light rail' or second_after_mode = 'light rail' or third_after_mode = 'light rail' or
      survey_mode = 'light rail') then
	  path_line_haul = 'LRF';

	if path_line_haul = 'XXX' then path_line_haul = 'LOC';

   path_label = cat(path_access, '-', path_line_haul, '-', path_egress);

run;

proc freq data = dataG; tables path_access path_egress path_line_haul path_label;
run;

* Computer number of boardings;
data dataH; set dataG;

  boardings = 1;
  if TRIM(first_prior_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(second_prior_mode) ^= 'None' then boardings = boardings + 1;
  if TRIM(third_prior_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(fourth_prior_mode) ^= 'None' then boardings = boardings + 1;

  if TRIM(first_after_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(second_after_mode) ^= 'None' then boardings = boardings + 1;
  if TRIM(third_after_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(fourth_after_mode) ^= 'None' then boardings = boardings + 1; 

run;

* Compare to ETC's trip_legs_code;
proc freq data = dataH; tables TOTAL_TRANSFERS boardings boardings*TOTAL_TRANSFERS;
run;

* Check for missing operators;
data dataI; set dataH; keep ID TOTAL_TRANSFERS boardings first_prior_mode TRANSFER_FROM_1ST second_prior_mode TRANSFER_FROM_2ND first_after_mode TRANSFER_TO_1ST 
                            second_after_mode TRANSFER_TO_2ND third_after_mode TRANSFER_TO_3RD fourth_after_mode TRANSFER_TO_4TH;
   if (boardings - 1) = TOTAL_TRANSFERS then delete;
   if TOTAL_TRANSFERS = . then delete;
run;

data OnBoard.bart_purp_asuff_path; set dataH;
   label path_label         = "Travel Model One Mode Choice Path";
   label access_mode        = "Simplified Access Mode";
   label egress_mode        = "Simplified Egress Mode";
   label transfer_from      = "Operator of system transferred immediately before";
   label transfer_to        = "Operator of system transferred immediately after";
   label first_transit_mode = "Mode of first transit encountered";
   label last_transit_mode  = "Mode of last transit encountered";
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 5: Prepare socio-demographic information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* rename the survey variable for student status;
data dataA; set OnBoard.bart_purp_asuff_path;
   rename STUDENT_STATUS   = survey_student_status;
   rename HH_INCOME_Code_  = HH_INCOME_CODE;
   rename OTHER_LANG_Code_ = OTHER_LANG_CODE;
run;

proc freq data = dataA; tables YEAR_BORN;
run;

data dataA; set dataA; 

   work_status = put('Missing',23.);
   if EMPLOYMENT_STATUS = 'Yes' then work_status = 'full- or part-time';
   if EMPLOYMENT_STATUS = 'No' then work_status = 'non-worker';
   if orig_purp = 'work' or dest_purp = 'work' then work_status = 'full- or part-time';

   student_status = put('Missing',20.);
   if survey_student_status = 'No' then student_status = 'non-student';
   if survey_student_status = 'Yes' then student_status = 'full- or part-time';
   if orig_purp = 'grade school' or dest_purp = 'grade school' then student_status = 'full- or part-time';
   if orig_purp = 'high school'  or dest_purp = 'high school'  then student_status = 'full- or part-time';
   if orig_purp = 'university'   or dest_purp = 'university'   then student_status = 'full- or part-time';

   age = 2014 - YEAR_BORN;
   if YEAR_BORN = 0 then age = .;

   fare_medium = put('Missing',36.);
   if USED_CLIPPER_CARD = 'Yes' then fare_medium = 'Clipper';
   if USED_CLIPPER_CARD = 'No'  then fare_medium = 'Ticket or Token';

   fare_category = put('Missing',36.);
   if FARE_TYPE_CODE = 1 then fare_category = 'adult';
   if FARE_TYPE_CODE = 2 then fare_category = 'adult';
   if FARE_TYPE_CODE = 3 then fare_category = 'adult';
   if FARE_TYPE_CODE = 5 then fare_category = 'senior';
   if FARE_TYPE_CODE = 6 then fare_category = 'disabled';
   if FARE_TYPE_CODE = 7 then fare_category = 'student';
   if FARE_TYPE_CODE = 9 then fare_category = 'other';

   * Match AC Transit: NOT HISPANIC/LATINO OR OF SPANISH ORIGIN or HISPANIC/LATINO OR OF SPANISH ORIGIN;
   hispanic = put('Missing',59.);
   if HISP_LATINO_SPANISH = 'Yes' then hispanic = 'HISPANIC/LATINO OR OF SPANISH ORIGIN';
   if HISP_LATINO_SPANISH = 'No'  then hispanic = 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN'; 
   
   * Match AC Transit: BLACK, WHITE, ASIAN, OTHER;
   race = put('Missing',19.);
   race_dmy_ind = 0;
   if RACE_AMERICANINDIAN_ALASKANNATIV = 'Yes' then race_dmy_ind = 1;

   race_dmy_asn = 0;
   if RACE_ASIAN = 'Yes' then race_dmy_asn = 1;

   race_dmy_blk = 0;
   if RACE_BLACK_AFRICANAM = 'Yes' then race_dmy_blk = 1;

   race_dmy_hwi = 0;
   if RACE_NATHAWAIIAN_PACISLAND = 'Yes' then race_dmy_hwi = 1;

   race_dmy_wht = 0;
   if RACE_WHITE = 'Yes' then race_dmy_wht = 1;

   race_dmy_oth = 0;
   if LENGTH(RACE_OR_ETHNICITY_OTHER) > 2 then race_dmy_oth = 1;

   race_dmy_sum = race_dmy_ind + race_dmy_asn + race_dmy_blk + race_dmy_hwi + race_dmy_wht + race_dmy_oth;

   if race_dmy_sum > 1 then race = 'OTHER';
   if race_dmy_sum = 1 and race_dmy_ind = 1 then race = 'OTHER';
   if race_dmy_sum = 1 and race_dmy_asn = 1 then race = 'ASIAN';
   if race_dmy_sum = 1 and race_dmy_blk = 1 then race = 'BLACK';
   if race_dmy_sum = 1 and race_dmy_hwi = 1 then race = 'OTHER';
   if race_dmy_sum = 1 and race_dmy_wht = 1 then race = 'WHITE';
   if race_dmy_sum = 1 and race_dmy_oth = 1 then race = 'OTHER';

   * ENGLISH ONLY, ...;
   language_at_home = put('Missing',24.);
   if LANG_OTHER_THAN_ENG = 'No' then language_at_home = 'ENGLISH ONLY';

   * Common;
   if OTHER_LANG_CODE = 63  then language_at_home = 'CHINESE (MANDARIN)';
   if OTHER_LANG_CODE = 111 then language_at_home = 'FRENCH';
   if OTHER_LANG_CODE = 143 then language_at_home = 'SPANISH';
   if OTHER_LANG_CODE = 208 then language_at_home = 'KOREAN';
   if OTHER_LANG_CODE = 356 then language_at_home = 'POLISH';
   if OTHER_LANG_CODE = 358 then language_at_home = 'PORTUGUESE';
   if OTHER_LANG_CODE = 385 then language_at_home = 'RUSSIAN';
   if OTHER_LANG_CODE = 408 then language_at_home = 'FRENCH CREOLE';
   if OTHER_LANG_CODE = 426 then language_at_home = 'SPANISH';
   if OTHER_LANG_CODE = 491 then language_at_home = 'CHINESE (CANTONESE)';
   if OTHER_LANG_CODE = 503 then language_at_home = 'TAGALOG';
   if OTHER_LANG_CODE = 504 then language_at_home = 'CHINESE';
   
   * Less common;
   if OTHER_LANG_CODE = 3   then language_at_home = 'AFRIKAANS';
   if OTHER_LANG_CODE = 11  then language_at_home = 'AMHARIC';
   if OTHER_LANG_CODE = 13  then language_at_home = 'ARABIC';
   if OTHER_LANG_CODE = 17  then language_at_home = 'ARMENIAN';
   if OTHER_LANG_CODE = 18  then language_at_home = 'ARMENIAN';
   if OTHER_LANG_CODE = 37  then language_at_home = 'BENGALI';
   if OTHER_LANG_CODE = 45  then language_at_home = 'BOSNIAN';
   if OTHER_LANG_CODE = 56  then language_at_home = 'CUBUANO';
   if OTHER_LANG_CODE = 84  then language_at_home = 'DUTCH';
   if OTHER_LANG_CODE = 102 then language_at_home = 'FARSI';
   if OTHER_LANG_CODE = 129 then language_at_home = 'GERMAN';
   if OTHER_LANG_CODE = 135 then language_at_home = 'GREEK';
   if OTHER_LANG_CODE = 146 then language_at_home = 'HINDI';
   if OTHER_LANG_CODE = 148 then language_at_home = 'HUNGARIAN';
   if OTHER_LANG_CODE = 152 then language_at_home = 'INDONESIAN';
   if OTHER_LANG_CODE = 156 then language_at_home = 'ITALIAN';
   if OTHER_LANG_CODE = 159 then language_at_home = 'JAPANESE';
   if OTHER_LANG_CODE = 220 then language_at_home = 'LATIN';
   if OTHER_LANG_CODE = 279 then language_at_home = 'MONGOLIAN';
   if OTHER_LANG_CODE = 297 then language_at_home = 'NORWEGIAN';
   if OTHER_LANG_CODE = 321 then language_at_home = 'PERSIAN';
   if OTHER_LANG_CODE = 325 then language_at_home = 'SPANISH';
   if OTHER_LANG_CODE = 351 then language_at_home = 'PIDGIN-NIGERIAN';
   if OTHER_LANG_CODE = 351 then language_at_home = 'PUNJABI';
   if OTHER_LANG_CODE = 378 then language_at_home = 'ROMANIAN';
   if OTHER_LANG_CODE = 432 then language_at_home = 'SWEDISH';
   if OTHER_LANG_CODE = 441 then language_at_home = 'TELUGU';
   if OTHER_LANG_CODE = 443 then language_at_home = 'THAI';
   if OTHER_LANG_CODE = 446 then language_at_home = 'TONGAN';
   if OTHER_LANG_CODE = 448 then language_at_home = 'TURKISH';
   if OTHER_LANG_CODE = 456 then language_at_home = 'URDU';
   if OTHER_LANG_CODE = 465 then language_at_home = 'VIETNAMESE';
   if OTHER_LANG_CODE = 488 then language_at_home = 'HAWAIIAN';
   if OTHER_LANG_CODE = 489 then language_at_home = 'FILIPINO';
   if OTHER_LANG_CODE = 490 then language_at_home = 'SIGN LANGUAGE';
   if OTHER_LANG_CODE = 498 then language_at_home = 'MICRONESIAN';
   if OTHER_LANG_CODE = 501 then language_at_home = 'SOMOAN';
   if OTHER_LANG_CODE = 502 then language_at_home = 'SWAHILI';

   household_income = put('Missing',36.);
   if HH_INCOME_CODE = 1 then household_income = 'under $10,000';
   if HH_INCOME_CODE = 2 then household_income = '$10,000 to $25,000';
   if HH_INCOME_CODE = 3 then household_income = '$25,000 to $35,000';
   if HH_INCOME_CODE = 4 then household_income = '$35,000 to $50,000';
   if HH_INCOME_CODE = 5 then household_income = '$35,000 to $50,000';
   if HH_INCOME_CODE = 6 then household_income = '$50,000 to $75,000';
   if HH_INCOME_CODE = 7 then household_income = '$50,000 to $75,000';
   if HH_INCOME_CODE = 8 then household_income = '$75,000 to $100,000';
   if HH_INCOME_CODE = 9 then household_income = '$100,000 to $150,000';
   if HH_INCOME_CODE = 10 then household_income = '$150,000 or higher';

   sex = put('Missing',10.); 
   if RESP_GENDER_CODE = 1 then sex = 'male';
   if RESP_GENDER_CODE = 2 then sex = 'female';

   * interviewer language;
   interview_language = put('Missing',36.);
   if SRVY_IN_ENGL_SPANISH = 'English' then interview_language = 'ENGLISH';
   if SRVY_IN_ENGL_SPANISH = 'Spanish' then interview_language = 'SPANISH';

   * English proficiency;
   eng_proficient = put('Missing',10.);
   if ENGLISH_FLUENCY_CODE = 1 then eng_proficient = 'VERY WELL';
   if ENGLISH_FLUENCY_CODE = 2 then eng_proficient = 'WELL';
   if ENGLISH_FLUENCY_CODE = 3 then eng_proficient = 'NOT WELL';
   if ENGLISH_FLUENCY_CODE = 4 then eng_proficient = 'NOT AT ALL';

run;

proc freq data = dataA; tables work_status student_status age fare_medium fare_category hispanic race language_at_home household_income sex interview_language eng_proficient;
run;

data OnBoard.bart_purp_asuff_path_sd; set dataA;
   label work_status        = "Employment status";
   label student_status     = "Student status";
   label age                = "Age";
   label fare_medium        = "Fare media";
   label fare_category      = "Fare category";
   label hispanic           = "Hispanic";
   label race               = "Racial identity";
   label language_at_home   = "Langauge spoken at home";
   label household_income   = "Household income ($2012)";
   label sex                = "Sex";
   label interview_language = "Language of interview";
   label eng_proficient     = "How well do you speak English";
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 7: Other model information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data dataA; set OnBoard.bart_purp_asuff_path_sd;

   depart_hour = HOUR(TIME_LAST_LEFT_HOME);

   return_hour = .;
   if TRIP_END_TIME_CODE < 17 then return_hour = TRIP_END_TIME_CODE + 4;
   if TRIP_END_TIME_CODE = 17 then return_hour = 4;
   if TRIP_END_TIME_CODE = 18 then return_hour = 21;
   if TRIP_END_TIME_CODE = 19 then return_hour = 22;
   if TRIP_END_TIME_CODE = 20 then return_hour = 23;

   weight = 0.0;
   if (QA_QCFLAG_Code = 1) then weight = 1.0;

   route = put(cat(ENTER_STATION,EXIT_STATION),100.);

   direction = put('missing',20.);

run;

proc freq data = dataA; tables depart_hour return_hour;
run;

* Extract the daypart and the weekpart from the date;
data dataB; set dataA;

   daypart = put('missing',10.);
   time_start = HOUR(Time_Started);

   if time_start > 2  and time_start < 6  then daypart = 'EARLY AM';
   if time_start > 5  and time_start < 11 then daypart = 'AM PEAK';
   if time_start > 10 and time_start < 15 then daypart = 'MIDDAY';
   if time_start > 14 and time_start < 19 then daypart = 'PM PEAK';
   if time_start > 18 or  time_start < 3  then daypart = 'EVENING';

   day_of_the_week = put('missing',10.);
   if WEEKDAY(date) = 1 then day_of_the_week = 'SUNDAY';
   if WEEKDAY(date) = 2 then day_of_the_week = 'MONDAY';
   if WEEKDAY(date) = 3 then day_of_the_week = 'TUESDAY';
   if WEEKDAY(date) = 4 then day_of_the_week = 'WEDNESDAY';
   if WEEKDAY(date) = 5 then day_of_the_week = 'THURSDAY';
   if WEEKDAY(date) = 6 then day_of_the_week = 'FRIDAY';
   if WEEKDAY(date) = 7 then day_of_the_week = 'SATURDAY';

   weekpart = put('WEEKDAY',7.);
   if day_of_the_week = 'SUNDAY' then weekpart = 'WEEKEND';
   if day_of_the_week = 'SATURDAY' then weekpart = 'WEEKEND';

   survey_type = put('tablet_pi',10.);
   if PAPER_SURVEY_CODE ~= . then survey_type = 'full_paper';

proc freq data = dataB; tables daypart day_of_the_week weekpart survey_type;
run;

data dataC; set dataB; keep ID weight route direction daypart weekpart day_of_the_week orig_purp dest_purp tour_purp autoSuff access_mode survey_mode egress_mode
                            path_access path_egress path_line_haul path_label transfer_from transfer_to boardings 
							first_transit_mode last_transit_mode
							work_status student_status
                            age fare_medium fare_category hispanic race language_at_home household_income sex eng_proficient 
                            interview_language depart_hour return_hour survey_type;

   label depart_hour = 'Home-based tour hour of departure from home';
   label return_hour = 'Home-based tour hour of return to home';

run;

data OnBoard.bart_purp_asuff_path_sd_time; set dataC;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 8: Boarding & alighting locations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data dataA; set OnBoard.bart_purp_asuff_path_sd;

  first_boarding_x = ACCESS_LON;
  first_boarding_y = ACCESS_LAT;

  last_alighting_x = EGRESS_LON;
  last_alighting_y = EGRESS_LAT;

  survey_boarding_x  = ENTRY_LON;
  survey_boarding_y  = ENTRY_LAT;

  survey_alighting_x = EXIT_LON;
  survey_alighting_y = EXIT_LAT;
run;

* Merge data with the trimmed survey;
data dataB; set dataA; keep ID survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y 
                            first_boarding_x first_boarding_y last_alighting_x last_alighting_y;

data dataC; set OnBoard.bart_purp_asuff_path_sd_time;

proc sort data = dataB; by ID;
proc sort data = dataC; by ID;

data dataD; merge dataB dataC; by ID;
run;

* Merge data with key locations;
data locA; set OnBoard.bart_key_loc;

proc sort data = dataD; by ID;
proc sort data = locA;  by ID;

data dataE; merge dataD locA; by ID;

data dataF; set dataE;

   operator       = put('BART PRE-TEST',50.);
   field_start    = MDY(6,24,2014); 
   field_end      = MDY(6,28,2014);

   orig_purp_field = put('missing',25.);
   dest_purp_field = put('missing',25.);

   field_language = put(interview_language,7.);

run;

data OnBoard.bart_ready; set dataF;

   if weight < 0.1 then delete;
run;

* Clear working memory;
proc datasets lib = work kill; 
quit; run;
