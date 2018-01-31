
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* CreateUsableData.sas                                                    
*                                                                                            
* Purpose: Process and combine the raw consultant-delivered data set into a usable dataset.
*          See BuildDatabases.sas for script to create SAS databases.  Specific to ACE.
*          Work in progress.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 06 XX)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the file locations;
%let gis_geocode     = 'M:\Data\OnBoard\Data and Reports\ACE\Geocode\gis_geocode.csv';
%let output_directory = M:\Data\OnBoard\Data and Reports\ACE\_working;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 1: Compute tour purpose and trip purpose per Travel Model One designations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Trip purpose;
data dataA; set OnBoard.rawAce;
   orig_purp = "                         ";
   dest_purp = "                         ";

   if Origin_Type = 1  then orig_purp = "work";
   if Origin_Type = 2  then orig_purp = "work-related";
   if Origin_Type = 3  then orig_purp = "home";
   if Origin_Type = 4  then orig_purp = "social recreation";
   if Origin_Type = 5  then orig_purp = "shopping";
   if Origin_Type = 6  then orig_purp = "high school";
   if Origin_Type = 6  and Year_of_Birth>98 then orig_purp = "grade school";
   if Origin_Type = 7  then orig_purp = "university";
   if Origin_Type = 8  then orig_purp = "other maintenance";
   if Origin_Type = 9  then orig_purp = "other maintenance";
   if Origin_Type = 10 then orig_purp = "escorting";
   if Origin_Type = 11 then orig_purp = "eat out";
   if Origin_Type = 12 then orig_purp = "other discretionary";
   if Origin_Type = 13 then orig_purp = "other maintenance";

   if Destination_Type = 1  then dest_purp = "work";
   if Destination_Type = 2  then dest_purp = "work-related";
   if Destination_Type = 3  then dest_purp = "home";
   if Destination_Type = 4  then dest_purp = "social recreation";
   if Destination_Type = 5  then dest_purp = "shopping";
   if Destination_Type = 6  then dest_purp = "high school";
   if Destination_Type = 6  and Year_of_Birth>98 then dest_purp = "grade school";
   if Destination_Type = 7  then dest_purp = "university";
   if Destination_Type = 8  then dest_purp = "other maintenance";
   if Destination_Type = 9  then dest_purp = "other maintenance";
   if Destination_Type = 10 then dest_purp = "escorting";
   if Destination_Type = 11 then dest_purp = "eat out";
   if Destination_Type = 12 then dest_purp = "other discretionary";
   if Destination_Type = 13 then dest_purp = "other maintenance";

run;

* Adjust trip purpose with follow-up questions;
data dataA; set dataA;

   if Confirm_O_D = 2 and New_Origin_Type = 1  then orig_purp = "work";
   if Confirm_O_D = 2 and New_Origin_Type = 2  then orig_purp = "work-related";
   if Confirm_O_D = 2 and New_Origin_Type = 3  then orig_purp = "home";
   if Confirm_O_D = 2 and New_Origin_Type = 4  then orig_purp = "social recreation";
   if Confirm_O_D = 2 and New_Origin_Type = 5  then orig_purp = "shopping";
   if Confirm_O_D = 2 and New_Origin_Type = 6  then orig_purp = "high school";
   if Confirm_O_D = 2 and New_Origin_Type = 6  and Year_of_Birth>98 then orig_purp = "grade school";
   if Confirm_O_D = 2 and New_Origin_Type = 7  then orig_purp = "university";
   if Confirm_O_D = 2 and New_Origin_Type = 8  then orig_purp = "other maintenance";
   if Confirm_O_D = 2 and New_Origin_Type = 9  then orig_purp = "other maintenance";
   if Confirm_O_D = 2 and New_Origin_Type = 10 then orig_purp = "escorting";
   if Confirm_O_D = 2 and New_Origin_Type = 11 then orig_purp = "eat out";
   if Confirm_O_D = 2 and New_Origin_Type = 12 then orig_purp = "other discretionary";
   if Confirm_O_D = 2 and New_Origin_Type = 13 then orig_purp = "other maintenance";

   if Confirm_O_D = 3 and New_Destination_Type = 1  then dest_purp = "work";
   if Confirm_O_D = 3 and New_Destination_Type = 2  then dest_purp = "work-related";
   if Confirm_O_D = 3 and New_Destination_Type = 3  then dest_purp = "home";
   if Confirm_O_D = 3 and New_Destination_Type = 4  then dest_purp = "social recreation";
   if Confirm_O_D = 3 and New_Destination_Type = 5  then dest_purp = "shopping";
   if Confirm_O_D = 3 and New_Destination_Type = 6  then dest_purp = "high school";
   if Confirm_O_D = 3 and New_Destination_Type = 6  and Year_of_Birth>98 then dest_purp = "grade school";
   if Confirm_O_D = 3 and New_Destination_Type = 7  then dest_purp = "university";
   if Confirm_O_D = 3 and New_Destination_Type = 8  then dest_purp = "other maintenance";
   if Confirm_O_D = 3 and New_Destination_Type = 9  then dest_purp = "other maintenance";
   if Confirm_O_D = 3 and New_Destination_Type = 10 then dest_purp = "escorting";
   if Confirm_O_D = 3 and New_Destination_Type = 11 then dest_purp = "eat out";
   if Confirm_O_D = 3 and New_Destination_Type = 12 then dest_purp = "other discretionary";
   if Confirm_O_D = 3 and New_Destination_Type = 13 then dest_purp = "other maintenance";

proc freq data = dataA; tables orig_purp dest_purp;
run;

* Tour purpose;
data dataA; set dataA;
   tour_purp = "                         ";

* Start with the fairly straightforward purposes;
data dataB; set dataA;

  * workers;
  if tour_purp = "                         " and orig_purp = "home" and dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and orig_purp = "work" and dest_purp = "home" then tour_purp = "work";

  * students;
  if tour_purp = "                         " and orig_purp = "grade school" or dest_purp = "grade school" then tour_purp = "grade school";
  if tour_purp = "                         " and orig_purp = "high school" or dest_purp = "high school" then tour_purp = "high school";

  * non-working university students;
  if tour_purp = "                         " and Employment_Status > 1 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * non-workers, home-based travel (set home to home as other discretionary), could be part of a school tour, but we don't know;
  if tour_purp = "                         " and Employment_Status > 2 and orig_purp = "home" and dest_purp^= "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and Employment_Status > 2 and orig_purp = "home" and dest_purp = "home" then tour_purp = "other discretionary";
  if tour_purp = "                         " and Employment_Status > 2 and orig_purp^= "home" and dest_purp = "home" then tour_purp = orig_purp;
  if tour_purp = "                         " and Employment_Status > 2 and orig_purp^= "home" and dest_purp = orig_purp then tour_purp = orig_purp;

  * non-workers, non-students, non-home-based travel, assign the orig_purp the tour_purp, except escorting;
  if tour_purp = "                         " and Employment_Status > 2 and Student_Status = 1 and orig_purp^= "home" and dest_purp^= "home" and (orig_purp = "escorting" or dest_purp = "escorting") then tour_purp = "escorting";
  if tour_purp = "                         " and Employment_Status > 2 and Student_Status = 1 and orig_purp^= "home" and dest_purp^= "home" and orig_purp^= "escorting" and dest_purp^= "escorting" then tour_purp = orig_purp;

run;

* Use the fairly straightforward information from the work questions (Work_Before_or_After is go to work same day, q391 is back to work after trip, q392 is work before trip);
data dataC; set dataB; 

  * if work before then home, assume work tour;
  if tour_purp = "                         " and Work_Before_or_After = 1 and dest_purp = "home"  then tour_purp = "work";

  * if work after, and home before, assume work tour;
  if tour_purp = "                         " and Work_Before_or_After = 2 and orig_purp = "home" then tour_purp = "work";

  * if no work before or after, but work is a leg, assume work tour;
  if tour_purp = "                         " and Work_Before_or_After = 3 and (orig_purp = "work" or dest_purp = "work") then tour_purp = "work";
  
  * if work before, then work, then non-work, assume work tour;
  if tour_purp = "                         " and Work_Before_or_After = 1 and orig_purp = "work" then tour_purp = "work";

  * if work before, then non-work to non-work, assume work tour;
  if tour_purp = "                         " and Work_Before_or_After = 1 and orig_purp ^= "home" then tour_purp = "work"; 

  * If work before leaving home, work is irrelevant;
  if tour_purp = "                         " and Work_Before_or_After = 1 and orig_purp = "home" then tour_purp = dest_purp;

  * If work after arriving home, work is irrelevant;
  if tour_purp = "                         " and Work_Before_or_After = 2 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work that day, work is irrelevant;
  if tour_purp = "                         " and Work_Before_or_After = 3 and orig_purp = "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and Work_Before_or_After = 3 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work and university is present at all, then university;
  if tour_purp = "                         " and Work_Before_or_After = 3 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * If no work before or after and same orig and dest, orig is best guess;
  if tour_purp = "                         " and Work_Before_or_After = 3 and orig_purp = dest_purp then tour_purp = orig_purp;

  * Use the return to work for at work tours, assuming anyone going back to work but not stopping at home is making;
  if tour_purp = "                         " and Work_Before_or_After = 1 and orig_purp^= "home" and dest_purp= "work"  then tour_purp = "at work";
  if tour_purp = "                         " and Work_Before_or_After = 2 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and Work_After_Trip = 1 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and Work_Before_Trip = 1 and orig_purp^= "home" and dest_purp = "work" then tour_purp = "at work";

  * If back to work and origin is home, then work tour;
  if tour_purp = "                         " and Work_After_Trip = 1 and orig_purp = "home" then tour_purp = "work";

  * If work before and destination is home, then work tour;
  if tour_purp = "                         " and Work_Before_Trip = 1 and dest_purp = "home" then tour_purp = "work";

  * If no back to work, but work is a party of the trip, then work;
  if tour_purp = "                         " and Work_After_Trip = 2  and orig_purp = "work" or dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and Work_Before_Trip = 2 and orig_purp = "work" or dest_purp = "work"  then tour_purp = "work";

  * If no back to work, but university if part of the trip, then university;
  if tour_purp = "                         " and Work_After_Trip = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";
  if tour_purp = "                         " and Work_Before_Trip = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

run;

* Move to the ones that require more faith or have odd 390/391/392 combinations;
data dataD; set dataC;
   
   * If you're still left, assume working before or after trip (which does not have a home or work end) puts you on a work tour;
   if tour_purp = "                         " and Work_Before_or_After = 1 then tour_purp = "work";
   if tour_purp = "                         " and Work_Before_or_After = 2 then tour_purp = "work"; 

   * If you're still left, and no working before or after, assume orig_purp;
   if tour_purp = "                         " and Work_Before_or_After = 3 then tour_purp = orig_purp;

   * If you're still left, and went back to work before a home-based trip, then work is irrelevant;
   if tour_purp = "                         " and Work_After_Trip = 1 and dest_purp = "home" then tour_purp = orig_purp;
   if tour_purp = "                         " and Work_Before_Trip = 1 and orig_purp = "home" then tour_purp = dest_purp;

   * If you're still left, and one end is home, then other end is purpose;
   if tour_purp = "                         " and orig_purp = "home" then tour_purp = dest_purp;
   if tour_purp = "                         " and dest_purp = "home" then tour_purp = orig_purp;

   * If you're still left, and non-home, then orig_purp;
   if tour_purp = "                         " then tour_purp = orig_purp;

   * Change home to other discretionary;
   if tour_purp = "home" then tour_purp = "other discretionary";

run; 

* Check frequencies;
proc freq data = dataD; tables tour_purp orig_purp dest_purp orig_purp*dest_purp;
run;


data OnBoard.ace_purp; set dataD;
   label orig_purp = "Travel Model One Activity at origin"
         dest_purp = "Travel Model One Activity at destination"
         tour_purp = "Travel Model One Tour purpose (approximate)";

run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 2: Append TAZ geo-codes to home, work, school, origin, and destination;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Read in CATI geo-codes (created by geo-code engine);
data input; infile "&gis_geocode." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format Survey_ID best12.;
			format measure $50.;
			format maz best12.;
			format taz1454 best12.;

			informat Survey_ID best32.;
			informat measure $50.;
			informat maz best32.;
			informat taz1454 best32.;

			input    Survey_ID  
			         measure $  
			         maz
                     taz1454;

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

  if trim(measure) = 'Origin POINT' then originTAZ = taz1454;
  if trim(measure) = 'Origin POINT' then originMAZ = maz;

  if trim(measure) = 'Destination POINT' then destTAZ = taz1454;
  if trim(measure) = 'Destination POINT' then destMAZ = maz;

  if Confirm_O_D = 2 and trim(measure) = 'New Origin POINT' then originTAZ = taz1454;
  if Confirm_O_D = 2 and trim(measure) = 'New Origin POINT' then originMAZ = maz;

  if Confirm_O_D = 3 and trim(measure) = 'New Destination POINT' then destTAZ = taz1454;
  if Confirm_O_D = 3 and trim(measure) = 'New Destination POINT' then destMAZ = maz;

  if trim(measure) = 'Work POINT' then workTAZ = taz1454;
  if trim(measure) = 'Work POINT' then workMAZ = maz;

  if trim(measure) = 'School POINT' then schoolTAZ = taz1454;
  if trim(measure) = 'School POINT' then schoolMAZ = maz;

  if trim(measure) = 'Home POINT' then workTAZ = taz1454;
  if trim(measure) = 'Home POINT' then workMAZ = maz;


proc summary data = geoA threads nway;
  class Survey_ID;
  var homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
  output out = geoB
  max = homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
run;

data OnBoard.ace_key_loc; set geoB; drop _TYPE_ _FREQ_;
run;
 
data dataA; set OnBoard.ace_purp; keep Survey_ID orig_purp dest_purp;
data gisA;  set OnBoard.ace_key_loc; 

proc sort data = dataA threads; by Survey_ID;
proc sort data = gisA  threads; by Survey_ID;

data gisB; merge dataA gisA; by Survey_ID;
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

data OnBoard.ace_key_loc; set gisC; drop orig_purp dest_purp;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 3: Automobile sufficiency;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data dataA; set OnBoard.ace_purp;

proc freq data = dataA; tables __of_Employed_in_Household __of_Employed_in_Household_Other __of_Drivable_Veh__in_House __of_Drivable_Veh__in_House_Othe;
run;

* recode variables to be directly usable;
data dataB; set dataA;
   vehicles = .;
   if __of_Drivable_Veh__in_House = 1 then vehicles = 0;
   if __of_Drivable_Veh__in_House = 2 then vehicles = 1;
   if __of_Drivable_Veh__in_House = 3 then vehicles = 2;
   if __of_Drivable_Veh__in_House = 4 then vehicles = 3;
   if __of_Drivable_Veh__in_House = 5 then vehicles = 4;

   if __of_Drivable_Veh__in_House_Othe ^= . then vehicles = __of_Drivable_Veh__in_House_Othe;

   workers = .;
   if __of_Employed_in_Household = 1 then workers = 0;
   if __of_Employed_in_Household = 2 then workers = 1;
   if __of_Employed_in_Household = 3 then workers = 2;
   if __of_Employed_in_Household = 4 then workers = 3;
   if __of_Employed_in_Household = 5 then workers = 4;
   if __of_Employed_in_Household = 6 then workers = 5;
   if __of_Employed_in_Household = 7 then workers = 6;

   if __of_Employed_in_Household_Other ^= . then workers = __of_Employed_in_Household_Other;

proc freq data = dataB; tables vehicles workers;
run;

data dataC; set dataB;
   autoSuff = 'Missing                 ';

   if vehicles = 0 then autoSuff = 'Zero autos';

   if workers > vehicles and vehicles > 0 and workers > 0 then autoSuff = 'Workers > autos';

   if workers <= vehicles and vehicles > 0 and workers > 0 then autoSuff = 'Workers <= autos';

   if workers = 0 and vehicles > 0 then autoSuff = 'Workers <= autos';

data dataD; set dataC;
   if autoSuff = 'Missing                 ' then autoSuff = .;

proc freq data = dataD; tables autoSuff;
run;

data OnBoard.ace_purp_asuff; set dataD;
   label autoSuff = "Travel Model One Automobile Sufficiency";
   label vehicles = "Number of vehicles in the household (capped)";
   label workers  = "Number of workers in the household (capped)";
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 4: Determine mode sequence and Travel Model One mode;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Assign the operator and route;
data dataA; set OnBoard.ace_purp_asuff;
   first_prior_raw  = _st_Before_Transfer_Boarding_Typ;
   second_prior_raw = _nd_Before_Transfer_Boarding_Typ;
   third_prior_raw  = _rd_Before_Transfer_Boarding_Typ;

   first_after_raw  = _st_After_Transfer_Boarding_Type;
   second_after_raw = _nd_After_Transfer_Boarding_Type;
   third_after_raw  = _rd_After_Transfer_Boarding_Type;
run;

* Define a macro to extract operator from the raw string;
%MACRO SET_OPERATOR(INPUT, OUTPUT);

   &OUTPUT = 'None                                         ';
   if index(&INPUT,'AC Transit') ge 1 then &OUTPUT = 'AC TRANSIT';
   if index(&INPUT,'BART')       ge 1 then &OUTPUT = 'BART';
   if index(&INPUT,'CCCTA')      ge 1 then &OUTPUT = 'COUNTY CONNECTION';
   if index(&INPUT,'Caltrain')   ge 1 then &OUTPUT = 'CALTRAIN';
   if index(&INPUT,'MAX')        ge 1 then &OUTPUT = 'MODESTO TRANSIT';
   if index(&INPUT,'MUNI')       ge 1 then &OUTPUT = 'MUNI';
   if index(&INPUT,'SJ RTD')     ge 1 then &OUTPUT = 'SAN JOAQUIN TRANSIT';
   if index(&INPUT,'Shuttle')    ge 1 then &OUTPUT = 'PRIVATE SHUTTLE';
   if index(&INPUT,'VTA')        ge 1 then &OUTPUT = 'VTA';
   if index(&INPUT,'Wheels')     ge 1 then &OUTPUT = 'WHEELS (LAVTA)';

%MEND SET_OPERATOR;
run;

* Define a macro to extract technology from the raw string;
%MACRO SET_TECHNOLOGY(OPERATOR, OUTPUT);

   * Assign a base technology;
   &OUTPUT = 'None                      ';
   if &OPERATOR = 'AC TRANSIT'          then &OUTPUT = 'local bus';
   if &OPERATOR = 'BART'                then &OUTPUT = 'heavy rail';
   if &OPERATOR = 'COUNTY CONNECTION'   then &OUTPUT = 'local bus';
   if &OPERATOR = 'CALTRAIN'            then &OUTPUT = 'commuter rail';
   if &OPERATOR = 'MODESTO TRANSIT'     then &OUTPUT = 'local bus';
   if &OPERATOR = 'MUNI'                then &OUTPUT = 'local bus';
   if &OPERATOR = 'SAN JOAQUIN TRANSIT' then &OUTPUT = 'local bus';
   if &OPERATOR = 'PRIVATE SHUTTLE'     then &OUTPUT = 'local bus';
   if &OPERATOR = 'VTA'                 then &OUTPUT = 'local bus';
   if &OPERATOR = 'WHEELS (LAVTA)'      then &OUTPUT = 'local bus';

%MEND SET_TECHNOLOGY;
run;

* Define a macro to set the route to get the technology for the more nuanced carriers;
%MACRO SET_ROUTE(INPUT, OPERATOR, OUTPUT);

   &OUTPUT = 'None                                 ';
   if &OPERATOR = 'AC TRANSIT' then &OUTPUT = substr(&INPUT,length('AC Transit') + 1,length(&INPUT) - length('AC Transit') + 1);
   if &OPERATOR = 'MUNI'       then &OUTPUT = substr(&INPUT,length('MUNI')       + 1,length(&INPUT) - length('MUNI')       + 1);
   if &OPERATOR = 'VTA'        then &OUTPUT = substr(&INPUT,length('VTA')        + 1,length(&INPUT) - length('VTA')        + 1);

%MEND SET_ROUTE;
run;

* Update technologies based on manual inspection of results;
%MACRO UPDATE_TECH(OPERATOR, ROUTE, OUTPUT);

   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '900' then &OUTPUT = 'light rail';
   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '901' then &OUTPUT = 'light rail';
   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '902' then &OUTPUT = 'light rail';

   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '970' then &OUTPUT = 'express bus';
   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '971' then &OUTPUT = 'express bus'; 
   if &OPERATOR = 'VTA' and TRIM(&ROUTE) = '972' then &OUTPUT = 'express bus';

   if &OPERATOR = 'AC TRANSIT' and TRIM(&ROUTE) = 'U' then &OUTPUT = 'express bus';
   if &OPERATOR = 'AC TRANSIT' and TRIM(&ROUTE) = 'DB1' then &OUTPUT = 'express bus';

%MEND UPDATE_TECH;
run;


* Apply the operator macro;
data dataB; set dataA;
   
   %SET_OPERATOR(first_prior_raw,  first_prior_operator);
   %SET_OPERATOR(second_prior_raw, second_prior_operator);
   %SET_OPERATOR(third_prior_raw,  third_prior_operator);

   %SET_OPERATOR(first_after_raw, first_after_operator);
   %SET_OPERATOR(second_after_raw, second_after_operator);
   %SET_OPERATOR(third_after_raw, third_after_operator);

run;

* Apply the technology macro;
data dataC; set dataB;
   
   %SET_TECHNOLOGY(first_prior_operator,  first_prior_mode);
   %SET_TECHNOLOGY(second_prior_operator, second_prior_mode);
   %SET_TECHNOLOGY(third_prior_operator,  third_prior_mode);

   %SET_TECHNOLOGY(first_after_operator, first_after_mode);
   %SET_TECHNOLOGY(second_after_operator, second_after_mode);
   %SET_TECHNOLOGY(third_after_operator, third_after_mode);

run;

* Update the special case AC Transit, Muni, and VTA technologies;
data dataD; set dataC;

   %SET_ROUTE(first_prior_raw,  first_prior_operator,  first_prior_route);
   %SET_ROUTE(second_prior_raw, second_prior_operator, second_prior_route);
   %SET_ROUTE(third_prior_raw,  third_prior_operator,  third_prior_route);

   %SET_ROUTE(first_after_raw,  first_after_operator,  first_after_route);
   %SET_ROUTE(second_after_raw, second_after_operator, second_after_route);
   %SET_ROUTE(third_after_raw,  third_after_operator,  third_after_route);

run;

data dataE; set dataD;

   %UPDATE_TECH(first_prior_operator,  first_prior_route,  first_prior_mode);
   %UPDATE_TECH(second_prior_operator, second_prior_route, second_prior_mode);
   %UPDATE_TECH(third_prior_operator,  third_prior_route,  third_prior_mode);

   %UPDATE_TECH(first_after_operator,  first_after_route,  first_after_mode);
   %UPDATE_TECH(second_after_operator, second_after_route, second_after_mode);
   %UPDATE_TECH(third_after_operator,  third_after_route,  third_after_mode);
 
run;

proc freq data = dataE; tables first_prior_operator second_prior_operator third_prior_operator;
proc freq data = dataE; tables first_after_operator second_after_operator third_after_operator;

proc freq data = dataE; tables first_prior_mode second_prior_mode third_prior_mode;
proc freq data = dataE; tables first_after_mode second_after_mode third_after_mode;
run;

* Set the transfer to, transfer from, access, egress, and survey mode;
proc freq data = dataE; tables Origin_Access_Mode;
run;

data dataF; set dataE;

   * Survey mode;
   survey_mode = put('commuter rail',15.);

   * Transfer to and from;
   transfer_from = first_prior_operator;
   if TRIM(second_prior_operator) ^= 'None' then transfer_from = second_prior_operator;
   if TRIM(third_prior_operator)  ^= 'None' then transfer_from = third_prior_operator;

   transfer_to = first_after_operator;
 
   * Access mode;
   access_mode = 'Missing                ';
   if Origin_Access_Mode = 1 then access_mode = 'walk';
   if Origin_Access_Mode = 2 then access_mode = 'bike';
   if Origin_Access_Mode = 3 then access_mode = 'pnr';
   if Origin_Access_Mode = 4 then access_mode = 'pnr';
   if Origin_Access_Mode = 5 then access_mode = 'knr';
   if Origin_Access_Mode = 6 then access_mode = 'knr';
   if Origin_Access_Mode = 7 then access_mode = 'pnr';
   if access_mode = 'Missing                ' then access_mode = .;

   * Egress mode;
   egress_mode = 'Missing                ';
   if Destination_Egress_Mode = 1 then egress_mode = 'walk';
   if Destination_Egress_Mode = 2 then egress_mode = 'bike';
   if Destination_Egress_Mode = 3 then egress_mode = 'pnr';
   if Destination_Egress_Mode = 4 then egress_mode = 'pnr';
   if Destination_Egress_Mode = 5 then egress_mode = 'knr';
   if Destination_Egress_Mode = 6 then egress_mode = 'knr';
   if Destination_Egress_Mode = 7 then egress_mode = 'pnr';
   if egress_mode = 'Missing                ' then egress_mode = .;

   * Mode of first transit vehicle boarded;
   first_transit_mode = survey_mode;
   if TRIM(first_prior_mode)  ^= 'None' then first_transit_mode = first_prior_mode;

   * Mode of last transit vehicle boarded;
   last_transit_mode = survey_mode;
   if TRIM(first_after_mode)  ^= 'None' then last_transit_mode = first_after_mode;
   if TRIM(second_after_mode) ^= 'None' then last_transit_mode = second_after_mode;
   if TRIM(third_after_mode)  ^= 'None' then last_transit_mode = third_after_mode;

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

   path_line_haul = 'COM';

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

  if TRIM(first_after_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(second_after_mode) ^= 'None' then boardings = boardings + 1;
  if TRIM(third_after_mode)  ^= 'None' then boardings = boardings + 1; 

run;

* Compare to Mark's trip_legs_code;
proc freq data = dataH; tables __of_Transfers boardings boardings*__of_Transfers;
run;

data OnBoard.ace_purp_asuff_path; set dataH;
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
data dataA; set OnBoard.ace_purp_asuff_path;
   rename Student_Status = survey_student_status;
run;

proc freq data = dataA; tables Year_of_Birth;
run;


data dataA; set dataA; 

   work_status = 'Missing                ';
   if Employment_status = 1 then work_status = 'full- or part-time';
   if Employment_status = 2 then work_status = 'full- or part-time';
   if Employment_status = 3 then work_status = 'non-worker';
   if Employment_status = 4 then work_status = 'non-worker';
   if Employment_status = 5 then work_status = 'non-worker';
   if orig_purp = 'work' or dest_purp = 'work' then work_status = 'full- or part-time';

   student_status = 'Missing             ';
   if survey_student_status = 1 then student_status = 'non-student';
   if survey_student_status = 2 then student_status = 'full- or part-time';
   if survey_student_status = 3 then student_status = 'full- or part-time';
   if survey_student_status = 4 then student_status = 'full- or part-time';
   if orig_purp = 'grade school' or dest_purp = 'grade school' then student_status = 'full- or part-time';
   if orig_purp = 'high school'  or dest_purp = 'high school'  then student_status = 'full- or part-time';
   if orig_purp = 'university'   or dest_purp = 'university'   then student_status = 'full- or part-time';

   age = 113 - Year_of_Birth;
   if Year_of_Birth = 0 then age = .;

   fare_medium = 'Missing                             ';
   if Fare_Payment__Ticket_ = 1   then fare_medium = 'cash (one way)';
   if Fare_Payment__Ticket_ = 2   then fare_medium = 'cash (round trip)';
   if Fare_Payment__Ticket_ = 3   then fare_medium = 'pass (20 ride)';
   if Fare_Payment__Ticket_ = 4   then fare_medium = 'pass (monthly)';
   if Fare_Payment__Ticket_ = 5   then fare_medium = 'other';

   fare_category = 'Missing                             ';
   if Fare_Type = 1 then fare_category = 'adult';
   if Fare_Type = 2 then fare_category = 'senior';
   if Fare_Type = 3 then fare_category = 'disabled';
   if Fare_Type = 4 then fare_category = 'senior';
   if Fare_Type = 5 then fare_category = 'other';

   * Match AC Transit: NOT HISPANIC/LATINO OR OF SPANISH ORIGIN or HISPANIC/LATINO OR OF SPANISH ORIGIN;
   hispanic = 'Missing                                                    ';
   if Hispanic__Latino__or_of_Spanish = 1 then hispanic = 'HISPANIC/LATINO OR OF SPANISH ORIGIN';
   if Hispanic__Latino__or_of_Spanish = 2 then hispanic = 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN'; 
   
   * Match AC Transit: BLACK, WHITE, ASIAN, OTHER;
   race = 'Missing            ';
   if Race_Ethnicity = 1 then race = 'WHITE';
   if Race_Ethnicity = 2 then race = 'BLACK';
   if Race_Ethnicity = 3 then race = 'ASIAN';
   if Race_Ethnicity = 4 then race = 'OTHER';
   if Race_Ethnicity = 5 then race = 'OTHER';
   if Race_Ethnicity = 6 then race = 'OTHER';
   if Race_Ethnicity = 7 then race = 'OTHER';

   * ENGLISH ONLY, ...;
   language_at_home = 'Missing                 ';
   if Language_Other_Than_English = 2 then language_at_home = 'ENGLISH ONLY';

   if Language = 1  then language_at_home = 'SPANISH';
   if Language = 2  then language_at_home = 'CHINESE-CANTONESE';
   if Language = 3  then language_at_home = 'CHINESE-MANDARIN';
   if Language = 4  then language_at_home = 'VIETNAMESE';
   if Language = 5  then language_at_home = 'KOREAN';
   if Language = 6  then language_at_home = 'TAGALOG';
   if Language = 7  then language_at_home = 'RUSSIAN';
   if Language = 8  then language_at_home = 'PORTUGUESE';
   if Language = 9  then language_at_home = 'FRENCH';
   if Language = 10 then language_at_home = 'FRENCH CREOLE';
   if Language = 11 then language_at_home = 'POLISH';
   if Language = 12 then language_at_home = 'OTHER'; 

   household_income = 'Missing                             ';
   if Total_Household_Income = 1 then household_income = 'under $10,000';
   if Total_Household_Income = 2 then household_income = '$10,000 to $25,000';
   if Total_Household_Income = 3 then household_income = '$25,000 to $35,000';
   if Total_Household_Income = 4 then household_income = '$35,000 to $50,000';
   if Total_Household_Income = 5 then household_income = '$50,000 to $75,000';
   if Total_Household_Income = 6 then household_income = '$75,000 to $100,000';
   if Total_Household_Income = 7 then household_income = '$100,000 to $150,000';
   if Total_Household_Income = 8 then household_income = '$150,000 or higher';

   sex = put('Missing',10.); 
   if Gender = 1 then sex = 'male';
   if Gender = 2 then sex = 'female';

   * interviewer language (per Mark (Redhill), all done in English);
   interview_language = put('ENGLISH',36.);

   * English proficiency;
   eng_proficient = put('Missing',10.);
   if English_Fluency = 1 then eng_proficient = 'VERY WELL';
   if English_Fluency = 2 then eng_proficient = 'WELL';
   if English_Fluency = 3 then eng_proficient = 'NOT WELL';
   if English_Fluency = 4 then eng_proficient = 'NOT AT ALL';
 

run;

proc freq data = dataA; tables work_status student_status age fare_medium fare_category hispanic race language_at_home household_income sex interview_language eng_proficient;
run;

data OnBoard.ace_purp_asuff_path_sd; set dataA;
   label work_status     = "Employment status";
   label student_status  = "Student status";
   label age             = "Age";
   label fare_medium     = "Fare media";
   label fare_category   = "Fare category";
   label hispanic        = "Hispanic";
   label race            = "Racial identity";
   label language_at_home = "Langauge spoken at home";
   label household_income = "Household income ($2012)";
   label sex              = "Sex";
   label interview_language    = "Language of interview";
   label eng_proficient   = "How well do you speak English";
run;

 
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 7: Other model information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data dataA; set OnBoard.ace_purp_asuff_path_sd;

   depart_hour = .;
   depart_hour = Time_Left_Home + 3; 

   return_hour = .;
   return_hour = Time_Return_Home + 3;

run;

proc freq data = dataA; tables depart_hour return_hour;
run;

* Extract the daypart and the weekpart from the date;
data dataB; set dataA;

   daypart = put('missing',10.);
   if AM_PM_Trip = 1 then daypart = 'AM PEAK';
   if AM_PM_Trip = 2 then daypart = 'PM PEAK';

   day_of_the_week = put('missing',10.);
   if WEEKDAY(Date) = 1 then day_of_the_week = 'SUNDAY';
   if WEEKDAY(Date) = 2 then day_of_the_week = 'MONDAY';
   if WEEKDAY(Date) = 3 then day_of_the_week = 'TUESDAY';
   if WEEKDAY(Date) = 4 then day_of_the_week = 'WEDNESDAY';
   if WEEKDAY(Date) = 5 then day_of_the_week = 'THURSDAY';
   if WEEKDAY(Date) = 6 then day_of_the_week = 'FRIDAY';
   if WEEKDAY(Date) = 7 then day_of_the_week = 'SATURDAY';

   weekpart = 'WEEKDAY';

   * ID length dictates survey type (per Mark);
   survey_type = put('tablet_pi',10.);
   if Survey_ID > 999999 then survey_type = 'brief_cati';

proc freq data = dataB; tables daypart weekpart survey_type;
run;


data dataC; set dataB; keep Survey_ID weight daypart weekpart day_of_the_week orig_purp dest_purp tour_purp autoSuff access_mode survey_mode egress_mode
                            workers vehicles
                            path_access path_egress path_line_haul path_label transfer_from transfer_to boardings 
							first_transit_mode last_transit_mode
							work_status student_status
                            age fare_medium fare_category hispanic race language_at_home household_income sex eng_proficient 
                            interview_language depart_hour return_hour survey_type;

   label depart_hour = 'Home-based tour hour of departure from home';
   label return_hour = 'Home-based tour hour of return to home';

run;

data OnBoard.ace_purp_asuff_path_sd_time; set dataC;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 8: Boarding & alighting locations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data dataA; set OnBoard.ace_purp_asuff_path_sd; keep Survey_ID VAR44 VAR45 VAR60 VAR61 VAR75 VAR76 
                                                     ACE_Train_Boarding_POINT_X ACE_Train_Boarding_POINT_Y 
                                                     ACE_Train_Alighting_POINT_X ACE_Train_Alighting_POINT_Y
                                                     VAR107 VAR108 VAR123 VAR124 VAR138 VAR139;

   rename VAR44 = first_before_boarding_x;
   rename VAR45 = first_before_boarding_y;

   rename VAR60 = second_before_boarding_x;
   rename VAR61 = second_before_boarding_y;

   rename VAR75 = third_before_boarding_x;
   rename VAR76 = third_before_boarding_y;

   rename ACE_Train_Boarding_POINT_X = survey_boarding_x;
   rename ACE_Train_Boarding_POINT_Y = survey_boarding_y;

   rename ACE_Train_Alighting_POINT_X = survey_alighting_x;
   rename ACE_Train_Alighting_POINT_Y = survey_alighting_y;

   rename VAR107 = first_after_alighting_x;
   rename VAR108 = first_after_alighting_y;

   rename VAR123 = second_after_alighting_x;
   rename VAR124 = second_after_alighting_y;

   rename VAR138 = third_after_alighting_x;
   rename VAR139 = third_after_alighting_y;

run;

* Set the first boarding and last alighting;
data dataB; set dataA;
  
  first_boarding_x = first_before_boarding_x;
  first_boarding_y = first_before_boarding_y;

  * Place survey in first boarding if no first boarding;
  if first_boarding_x = . then first_boarding_x = survey_boarding_x;
  if first_boarding_y = . then first_boarding_y = survey_boarding_y;

  * Get the last alighting;
  last_alighting_x = survey_alighting_x;
  last_alighting_y = survey_alighting_y;
  if first_after_alighting_x ^= . then last_alighting_x = first_after_alighting_x;
  if first_after_alighting_y ^= . then last_alighting_y = first_after_alighting_y;

  if second_after_alighting_x ^= . then last_alighting_x = second_after_alighting_x;
  if second_after_alighting_y ^= . then last_alighting_y = second_after_alighting_y;

  if third_after_alighting_x ^= . then last_alighting_x = third_after_alighting_x;
  if third_after_alighting_y ^= . then last_alighting_y = third_after_alighting_y;

run;

* Merge data with the trimmed survey;
data dataC; set dataB; keep Survey_ID survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y 
                            first_boarding_x first_boarding_y last_alighting_x last_alighting_y;

data dataD; set OnBoard.ace_purp_asuff_path_sd_time;

proc sort data = dataC; by Survey_ID;
proc sort data = dataD; by Survey_ID;

data dataE; merge dataD dataC; by Survey_ID;
run;

* Merge data with key locations;
data locA; set OnBoard.ace_key_loc;

proc sort data = dataE; by Survey_ID;
proc sort data = locA;  by Survey_ID;

data dataF; merge dataE locA; by Survey_ID;


data dataF; set dataF;
   operator       = put('ACE',50.);
   field_start    = MDY(4,9,2014); 
   field_end      = MDY(4,17,2014);

   orig_purp_field = put('missing',25.);
   dest_purp_field = put('missing',25.);

   route = put('ACE',100.);

   direction = put('missing',20.);
   if daypart = 'AM PEAK' then direction = 'WESTBOUND';
   if daypart = 'PM PEAK' then direction = 'EASTBOUND';

   field_language = put(interview_language,7.);

   rename Survey_ID = id;
run;

data OnBoard.ace_ready; set dataF;
run;

* Clear working memory;
proc datasets lib = work kill; 
quit; run;
