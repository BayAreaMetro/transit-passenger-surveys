* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* CreateUsableData.sas                                                    
*                                                                                            
* Purpose: Process and combine the raw consultant-delivered data set into a usable dataset.
*          See BuildDatabases.sas for script to create SAS databases.  Specific to Golden 
*          Gate Transit (bus service) Work in progress.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 01 XX)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the file locations;
%let gis_geocode_weekday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Geocode\gis_geocode_weekday.csv';
%let gis_geocode_weekend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Geocode\gis_geocode_weekend.csv';
%let gis_geocode_maz_weekday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Geocode\gis_geocode_weekday_maz.csv';
%let gis_geocode_maz_weekend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Geocode\gis_geocode_weekend_maz.csv';
%let output_directory = M:\Data\OnBoard\Data and Reports\Golden Gate Transit\_working;
run;

* Add weekpart and combine weekend and weekday files;
* - CATI;
data day; set OnBoard.rawGgtbCatiWkday;
   weekpart = 'WEEKDAY';

data end; set OnBoard.rawGgtbCatiWkend;
   weekpart = 'WEEKEND';

proc append base = day data = end force;

data OnBoard.rawGgtbCati; set day;
run;

* - GIS;
data day; set OnBoard.rawGgtbGisWkday;
   weekpart = 'WEEKDAY';

data end; set OnBoard.rawGgtbGisWkend;
   weekpart = 'WEEKEND';

proc append base = day data = end force;

data OnBoard.rawGgtbGis; set day;
run;

* - Info;
data day; set OnBoard.rawGgtbInfoWkday;
   weekpart = 'WEEKDAY';

data end; set OnBoard.rawGgtbInfoWkend;
   weekpart = 'WEEKEND';

proc append base = day data = end force;

data OnBoard.rawGgtbInfo; set day;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 1: Compute tour purpose and trip purpose per Travel Model One designations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Trip purpose (q408 is year of birth);
data catiA; set OnBoard.rawGgtbCati;
   orig_purp = "                         ";
   dest_purp = "                         ";

   if q1 = 1  then orig_purp = "work";
   if q1 = 2  then orig_purp = "work-related";
   if q1 = 3  then orig_purp = "home";
   if q1 = 4  then orig_purp = "social recreation";
   if q1 = 5  then orig_purp = "shopping";
   if q1 = 6  then orig_purp = "high school";
   if q1 = 6  and q408>97 then orig_purp = "grade school";
   if q1 = 7  then orig_purp = "university";
   if q1 = 8  then orig_purp = "other maintenance";
   if q1 = 9  then orig_purp = "other maintenance";
   if q1 = 10 then orig_purp = "eat out";
   if q1 = 11 then orig_purp = "escorting";
   if q1 = 12 then orig_purp = "other maintenance";

   if q2 = 1  then dest_purp = "work";
   if q2 = 2  then dest_purp = "work-related";
   if q2 = 3  then dest_purp = "home";
   if q2 = 4  then dest_purp = "social recreation";
   if q2 = 5  then dest_purp = "shopping";
   if q2 = 6  then dest_purp = "high school";
   if q2 = 6  and q408>97 then dest_purp = "grade school";
   if q2 = 7  then dest_purp = "university";
   if q2 = 8  then dest_purp = "other maintenance";
   if q2 = 9  then dest_purp = "other maintenance";
   if q2 = 10 then dest_purp = "eat out";
   if q2 = 11 then dest_purp = "escorting";
   if q2 = 12 then dest_purp = "other maintenance";

run;

* Adjust trip purpose with follow-up questions;
data catiA; set catiA;

   if q10 = 1 and q11 = 1 and q12 = 1  then orig_purp = "work";
   if q10 = 1 and q11 = 1 and q12 = 2  then orig_purp = "work-related";
   if q10 = 1 and q11 = 1 and q12 = 3  then orig_purp = "home";
   if q10 = 1 and q11 = 1 and q12 = 4  then orig_purp = "social recreation";
   if q10 = 1 and q11 = 1 and q12 = 5  then orig_purp = "shopping";
   if q10 = 1 and q11 = 1 and q12 = 6  then orig_purp = "high school";
   if q10 = 1 and q11 = 1 and q12 = 6  and q408>97 then orig_purp = "grade school";
   if q10 = 1 and q11 = 1 and q12 = 7  then orig_purp = "university";
   if q10 = 1 and q11 = 1 and q12 = 8  then orig_purp = "other maintenance";
   if q10 = 1 and q11 = 1 and q12 = 9  then orig_purp = "other maintenance";
   if q10 = 1 and q11 = 1 and q12 = 10 then orig_purp = "eat out";
   if q10 = 1 and q11 = 1 and q12 = 11 then orig_purp = "escorting";
   if q10 = 1 and q11 = 1 and q12 = 12 then orig_purp = "other maintenance";

   if q10 = 1 and q11 = 2 and q12 = 1  then dest_purp = "work";
   if q10 = 1 and q11 = 2 and q12 = 2  then dest_purp = "work-related";
   if q10 = 1 and q11 = 2 and q12 = 3  then dest_purp = "home";
   if q10 = 1 and q11 = 2 and q12 = 4  then dest_purp = "social recreation";
   if q10 = 1 and q11 = 2 and q12 = 5  then dest_purp = "shopping";
   if q10 = 1 and q11 = 2 and q12 = 6  then dest_purp = "high school";
   if q10 = 1 and q11 = 2 and q12 = 6  and q408>97 then dest_purp = "grade school";
   if q10 = 1 and q11 = 2 and q12 = 7  then dest_purp = "university";
   if q10 = 1 and q11 = 2 and q12 = 8  then dest_purp = "other maintenance";
   if q10 = 1 and q11 = 2 and q12 = 9  then dest_purp = "other maintenance";
   if q10 = 1 and q11 = 2 and q12 = 10 then dest_purp = "eat out";
   if q10 = 1 and q11 = 2 and q12 = 11 then dest_purp = "escorting";
   if q10 = 1 and q11 = 2 and q12 = 12 then dest_purp = "other maintenance";

proc freq data = catiA; tables orig_purp dest_purp;
run;

* Tour purpose;
data catiA; set catiA;
   tour_purp = "                         ";

* Start with the fairly straightforward purposes (q382 is worker status, q385 is student status);
data catiB; set catiA;

  * workers;
  if tour_purp = "                         " and orig_purp = "home" and dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and orig_purp = "work" and dest_purp = "home" then tour_purp = "work";

  * students;
  if tour_purp = "                         " and orig_purp = "grade school" or dest_purp = "grade school" then tour_purp = "grade school";
  if tour_purp = "                         " and orig_purp = "high school" or dest_purp = "high school" then tour_purp = "high school";

  * non-working university students;
  if tour_purp = "                         " and q382 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * non-workers, home-based travel (set home to home as other discretionary), could be part of a school tour, but we don't know;
  if tour_purp = "                         " and q382 = 2 and orig_purp = "home" and dest_purp^= "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and q382 = 2 and orig_purp = "home" and dest_purp = "home" then tour_purp = "other discretionary";
  if tour_purp = "                         " and q382 = 2 and orig_purp^= "home" and dest_purp = "home" then tour_purp = orig_purp;
  if tour_purp = "                         " and q382 = 2 and orig_purp^= "home" and dest_purp = orig_purp then tour_purp = orig_purp;

  * non-workers, non-students, non-home-based travel, assign the orig_purp the tour_purp, except escorting;
  if tour_purp = "                         " and q382 = 2 and q385 = 2 and orig_purp^= "home" and dest_purp^= "home" and (orig_purp = "escorting" or dest_purp = "escorting") then tour_purp = "escorting";
  if tour_purp = "                         " and q382 = 2 and q385 = 2 and orig_purp^= "home" and dest_purp^= "home" and orig_purp^= "escorting" and dest_purp^= "escorting" then tour_purp = orig_purp;

run;

* Use the fairly straightforward information from the work questions (q390 is go to work same day, q391 is back to work after trip, q392 is work before trip);
data catiC; set catiB; 

  * if work before then home, assume work tour;
  if tour_purp = "                         " and q390 = 1 and dest_purp = "home"  then tour_purp = "work";

  * if work after, and home before, assume work tour;
  if tour_purp = "                         " and q390 = 2 and orig_purp = "home" then tour_purp = "work";

  * if no work before or after, but work is a leg, assume work tour;
  if tour_purp = "                         " and q390 = 3 and (orig_purp = "work" or dest_purp = "work") then tour_purp = "work";
  
  * if work before, then work, then non-work, assume work tour;
  if tour_purp = "                         " and q390 = 1 and orig_purp = "work" then tour_purp = "work";

  * if work before, then non-work to non-work, assume work tour;
  if tour_purp = "                         " and q390 = 1 and orig_purp ^= "home" then tour_purp = "work"; 


  * If work before leaving home, work is irrelevant;
  if tour_purp = "                         " and q390 = 1 and orig_purp = "home" then tour_purp = dest_purp;

  * If work after arriving home, work is irrelevant;
  if tour_purp = "                         " and q390 = 2 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work that day, work is irrelevant;
  if tour_purp = "                         " and q390 = 3 and orig_purp = "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and q390 = 3 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work and university is present at all, then university;
  if tour_purp = "                         " and q390 = 3 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * If no work before or after and same orig and dest, orig is best guess;
  if tour_purp = "                         " and q390 = 3 and orig_purp = dest_purp then tour_purp = orig_purp;

  * Use the return to work for at work tours, assuming anyone going back to work but not stopping at home is making;
  if tour_purp = "                         " and q390 = 1 and orig_purp^= "home" and dest_purp= "work"  then tour_purp = "at work";
  if tour_purp = "                         " and q390 = 2 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and q391 = 1 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and q392 = 1 and orig_purp^= "home" and dest_purp = "work" then tour_purp = "at work";

  * If back to work and origin is home, then work tour;
  if tour_purp = "                         " and q391 = 1 and orig_purp = "home" then tour_purp = "work";

  * If work before and destination is home, then work tour;
  if tour_purp = "                         " and q392 = 1 and dest_purp = "home" then tour_purp = "work";

  * If no back to work, but work is a party of the trip, then work;
  if tour_purp = "                         " and q391 = 2 and orig_purp = "work" or dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and q392 = 2 and orig_purp = "work" or dest_purp = "work"  then tour_purp = "work";

  * If no back to work, but university if part of the trip, then university;
  if tour_purp = "                         " and q391 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";
  if tour_purp = "                         " and q392 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

run;

* Move to the ones that require more faith or have odd 390/391/392 (and 393/394/395) combinations;
data catiD; set catiC;
   
   * If you're still left, assume working before or after trip (which does not have a home or work end) puts you on a work tour;
   if tour_purp = "                         " and q390 = 1 then tour_purp = "work";
   if tour_purp = "                         " and q390 = 2 then tour_purp = "work"; 

   * If you're still left, and non-worker, assume school before or after trip puts you an a school tour;
   if tour_purp = "                         " and q382 = 2 and q408 < 95 and               q393 < 3 then tour_purp = "university";
   if tour_purp = "                         " and q382 = 2 and q408 < 97 and q408 > 94 and q393 < 3 then tour_purp = "high school";
   if tour_purp = "                         " and q382 = 2 and q408 > 97 and               q393 < 3 then tour_purp = "grade school";

   * If you're still left, and no working before or after, assume orig_purp;
   if tour_purp = "                         " and q390 = 3 then tour_purp = orig_purp;

   * If you're still left, and went back to work before a home-based trip, then work is irrelevant;
   if tour_purp = "                         " and q391 = 1 and dest_purp = "home" then tour_purp = orig_purp;
   if tour_purp = "                         " and q392 = 1 and orig_purp = "home" then tour_purp = dest_purp;

   * If you're still left, and one end is home, then other end is purpose;
   if tour_purp = "                         " and orig_purp = "home" then tour_purp = dest_purp;
   if tour_purp = "                         " and dest_purp = "home" then tour_purp = orig_purp;

   * If you're still left, and non-home, then orig_purp;
   if tour_purp = "                         " then tour_purp = orig_purp;

   * Change home to other discretionary;
   if tour_purp = "home" then tour_purp = "other discretionary";

run; 

* Check frequencies;
proc freq data = catiD; tables tour_purp orig_purp dest_purp orig_purp*dest_purp;
run;


data OnBoard.ggtb_cati_purp; set catiD;
   label orig_purp = "Travel Model One Activity at origin"
         dest_purp = "Travel Model One Activity at destination"
         tour_purp = "Travel Model One Tour purpose (approximate)";

run;



* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 2: Append TAZ geo-codes to home, work, school, origin, and destination;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Read in CATI geo-codes (created by geo-code engine);
* - Weekday;
data day; infile "&gis_geocode_weekday." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format taz1454 best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat taz1454 best32.;

			input    id  
			         q  
			         qcode $
                     taz1454;

run;

* - Weekend;
data end; infile "&gis_geocode_weekend." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format taz1454 best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat taz1454 best32.;

			input    id  
			         q  
			         qcode $
                     taz1454;

run;

data day_maz; infile "&gis_geocode_maz_weekday." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format maz best12.;
			format taz best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat maz best32.;
			informat taz best32.;

			input    id  
			         q  
			         qcode $
                     maz
					 taz;

run;

data end_maz; infile "&gis_geocode_maz_weekend." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format maz best12.;
			format taz best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat maz best32.;
			informat taz best32.;

			input    id  
			         q  
			         qcode $
                     maz
					 taz;

run;

* Combine the weekday and weekend data sets;
proc append base = day data = end force;
proc append base = day_maz data = end_maz force;

* Merge the TM1 and TM2 geographies;
proc sort data = day threads; by id q qcode;
proc sort data = day_maz threads; by id q qcode;

data both; merge day day_maz; by id q qcode;

data input; set both;
run;


* Append the TAZ geo-coding to the GIS files;
data gisA;  set OnBoard.rawGgtbGis;
data catiA; set OnBoard.ggtb_cati_purp; keep id q10 q11;
data geoA;  set input;

proc sort data = gisA threads; by id q qcode;
proc sort data = catiA threads; by id;
proc sort data = geoA threads; by id q qcode;

data geoB; merge gisA geoA; by id q qcode;
data geoC; merge geoB catiA; by id;
run;

proc freq data = geoC;
   tables q;
run;

* Build a flat file with each ID on a single row;
data geoD; set geoC;
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

  if q = 4 then originTAZ = taz1454;
  if q = 5 then destTAZ   = taz1454;

  if q = 4 then originMAZ = maz;
  if q = 5 then destMAZ   = maz;

  * Correct for updated origin or destination;
  if q10 = 1 and q11 = 1 and q = 13 then originTAZ = taz1454;
  if q10 = 1 and q11 = 2 and q = 13 then destTAZ   = taz1454; 

  if q10 = 1 and q11 = 1 and q = 13 then originMAZ = maz;
  if q10 = 1 and q11 = 2 and q = 13 then destMAZ   = maz; 

  if q = 383 then workTAZ   = taz1454;
  if q = 386 then schoolTAZ = taz1454;
  if q = 388 then homeTAZ   = taz1454;

  if q = 383 then workMAZ   = maz;
  if q = 386 then schoolMAZ = maz;
  if q = 388 then homeMAZ   = maz;

proc summary data = geoD threads nway;
  class id;
  var homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
  output out = geoE
  max = homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;

data OnBoard.ggtb_key_loc_taz; set geoE; drop _TYPE_ _FREQ_;
run;

* Set the home and work locations if they are an origin or a destionation;
data catiA; set OnBoard.ggtb_cati_purp; keep id orig_purp dest_purp;
data gisA; set OnBoard.ggtb_key_loc_taz;
run;

proc sort data = catiA threads; by id;
proc sort data = gisA threads; by id;

data gisB; merge catiA gisA; by id;
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

data OnBoard.ggtb_key_loc_taz; set gisC; drop orig_purp dest_purp;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 3: Automobile sufficiency;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* q406 is number of workers, q407 is number of drivable vehicles;
data catiA; set OnBoard.ggtb_cati_purp;

proc freq data = catiA; tables q406 q406_other q407 q407_other;
run;

data catiB; set catiA;
   autoSuff = 'Missing                 ';

   if q407 = 16 then autoSuff = 'Zero autos';

   if q406 > q407 and q406<16 and q407<16 then autoSuff = 'Workers > autos';

   if q406 <= q407 and q406<16 and q407<16 then autoSuff = 'Workers <= autos';

   if q406 = 16 and q407<16 then autoSuff = 'Workers <= autos';

data catiB; set catiB;
   if autoSuff = 'Missing                 ' then autoSuff = .;

proc freq data = catiB; tables autoSuff;
run;

data OnBoard.ggtb_cati_purp_asuff; set catiB;
   label autoSuff = "Travel Model One Automobile Sufficiency";
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 4: Determine mode sequence and Travel Model One mode;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Create a macro to compute the Travel Model One mode from the survey questions, and use
  iteratively for the sequence questions;
%MACRO TM_MODE(LABEL,MODE_Q,AC_SEL_Q,AC_Q,MUNI_Q,SAM_Q,VTA_Q,TRANSFER);

   * AC Transit;
   if &MODE_Q = 1 and &AC_SEL_Q = 1 then &LABEL = 'local bus'; 
   if &MODE_Q = 1 and &AC_SEL_Q = 2 and (&AC_Q < 57 or &AC_Q > 90) then &LABEL = 'local bus';
   if &MODE_Q = 1 and &AC_SEL_Q = 2 and &AC_Q > 56 then &LABEL = 'express bus';
   if &MODE_Q = 1 and &TRANSFER = 0 then transfer_from = 'AC TRANSIT';
   if &MODE_Q = 1 and &TRANSFER = 1 then transfer_to   = 'AC TRANSIT';
   * ACE;
   if &MODE_Q = 2 then &LABEL = 'commuter rail';
   if &MODE_Q = 2 and &TRANSFER = 0 then transfer_from = 'ACE';
   if &MODE_Q = 2 and &TRANSFER = 1 then transfer_to   = 'ACE';
   * Amtrak; 
   if &MODE_Q = 3 then &LABEL = 'commuter rail';
   if &MODE_Q = 3  and &TRANSFER = 0 then transfer_from = 'AMTRAK';
   if &MODE_Q = 3 and &TRANSFER = 1 then transfer_to   = 'AMTRAK';
   * Angel Island Ferry;
   if &MODE_Q = 4 then &LABEL = 'ferry';
   if &MODE_Q = 4 and &TRANSFER = 0 then transfer_from = 'ANGEL ISLAND FERRY';
   if &MODE_Q = 4 and &TRANSFER = 1 then transfer_to   = 'ANGEL ISLAND FERRY';
   * BART;
   if &MODE_Q = 5 then &LABEL = 'heavy rail';
   if &MODE_Q = 5 and &TRANSFER = 0 then transfer_from = 'BART';
   if &MODE_Q = 5 and &TRANSFER = 1 then transfer_to   = 'BART';
   * Blue and gold ferry;
   if &MODE_Q = 6 then &LABEL = 'ferry';
   if &MODE_Q = 6 and &TRANSFER = 0 then transfer_from = 'BLUE & GOLD FERRY';
   if &MODE_Q = 6 and &TRANSFER = 1 then transfer_to   = 'BLUE & GOLD FERRY';
   * Caltrain;
   if &MODE_Q = 7 then &LABEL = 'commuter rail';
   if &MODE_Q = 7 and &TRANSFER = 0 then transfer_from = 'CALTRAIN';
   if &MODE_Q = 7 and &TRANSFER = 1 then transfer_to   = 'CALTRAIN';
   * County connection;
   if &MODE_Q = 8 then &LABEL = 'local bus';
   if &MODE_Q = 8 and &TRANSFER = 0 then transfer_from = 'COUNTY CONNECTION';
   if &MODE_Q = 8 and &TRANSFER = 1 then transfer_to   = 'COUNTY CONNECTION';
   * Dumbarton express;
   if &MODE_Q = 9 then &LABEL = 'express bus';
   if &MODE_Q = 9 and &TRANSFER = 0 then transfer_from = 'DUMBARTON EXPRESS';
   if &MODE_Q = 9 and &TRANSFER = 1 then transfer_to   = 'DUMBARTON EXPRESS';
   * Emery-go-round;
   if &MODE_Q = 10 then &LABEL = 'local bus';
   if &MODE_Q = 10 and &TRANSFER = 0 then transfer_from = 'EMERY-GO-ROUND';
   if &MODE_Q = 10 and &TRANSFER = 1 then transfer_to   = 'EMERY-GO-ROUND';
   * Fairfield-Suisun transit;
   if &MODE_Q = 11 then &LABEL = 'local bus';
   if &MODE_Q = 11 and &TRANSFER = 0 then transfer_from = 'FAIRFIELD-SUISUN';
   if &MODE_Q = 11 and &TRANSFER = 1 then transfer_to   = 'FAIRFIELD-SUISUN';
   * Golden gate ferry;
   if &MODE_Q = 12 then &LABEL = 'ferry';
   if &MODE_Q = 12 and &TRANSFER = 0 then transfer_from = 'GOLDEN GATE FERRY';
   if &MODE_Q = 12 and &TRANSFER = 1 then transfer_to   = 'GOLDEN GATE FERRY';
   * Golden gate transit;
   if &MODE_Q = 13 then &LABEL = 'express bus';
   if &MODE_Q = 13 and &TRANSFER = 0 then transfer_from = 'GOLDEN GATE TRANSIT';
   if &MODE_Q = 13 and &TRANSFER = 1 then transfer_to   = 'GOLDEN GATE TRANSIT';
   * Marin transit;
   if &MODE_Q = 14 then &LABEL = 'local bus';
   if &MODE_Q = 14 and &TRANSFER = 0 then transfer_from = 'MARIN TRANSIT';
   if &MODE_Q = 14 and &TRANSFER = 1 then transfer_to   = 'MARIN TRANSIT';
   * Muni;
   if &MODE_Q = 15 and &MUNI_Q<15  then &LABEL = 'light rail';
   if &MODE_Q = 15 and &MUNI_Q>=15 then &LABEL = 'local bus';
   if &MODE_Q = 15 and &TRANSFER = 0 then transfer_from = 'MUNI';
   if &MODE_Q = 15 and &TRANSFER = 1 then transfer_to   = 'MUNI';
   * Napa vine;
   if &MODE_Q = 16 then &LABEL = 'local bus';
   if &MODE_Q = 16 and &TRANSFER = 0 then transfer_from = 'NAPA VINE';
   if &MODE_Q = 16 and &TRANSFER = 1 then transfer_to   = 'NAPA VINE';
   * Petaluma transit;
   if &MODE_Q = 17 then &LABEL = 'local bus';
   if &MODE_Q = 17 and &TRANSFER = 0 then transfer_from = 'PETALUMA TRANSIT';
   if &MODE_Q = 17 and &TRANSFER = 1 then transfer_to   = 'PETALUMA TRANSIT';
   * SamTrans;
   if &MODE_Q = 18 then &LABEL = 'local bus';
   if &MODE_Q = 18 and &SAM_Q = 48 then &LABEL = 'express bus';
   if &MODE_Q = 18 and &TRANSFER = 0 then transfer_from = 'SAMTRANS';
   if &MODE_Q = 18 and &TRANSFER = 1 then transfer_to   = 'SAMTRANS';
   * SF Bay Ferry;
   if &MODE_Q = 19 then &LABEL = 'ferry';
   if &MODE_Q = 19 and &TRANSFER = 0 then transfer_from = 'SF BAY FERRY';
   if &MODE_Q = 19 and &TRANSFER = 1 then transfer_to   = 'SF BAY FERRY';
   * VTA;
   if &MODE_Q = 20 then &LABEL = 'local bus';
   if &MODE_Q = 20 and &VTA_Q > 79 and &VTA_Q ^= 88 then &LABEL = 'light rail';
   if &MODE_Q = 20 and &VTA_Q >50 and &VTA_Q < 64 then &LABEL = 'express bus';
   if &MODE_Q = 20 and &TRANSFER = 0 then transfer_from = 'VTA';
   if &MODE_Q = 20 and &TRANSFER = 1 then transfer_to   = 'VTA';
   * Santa Rosa CityBus;
   if &MODE_Q = 21 then &LABEL = 'local bus';
   if &MODE_Q = 21 and &TRANSFER = 0 then transfer_from = 'SANTA ROSA CITYBUS';
   if &MODE_Q = 21 and &TRANSFER = 1 then transfer_to   = 'SANTA ROSA CITYBUS';
   * Sonoma County transit;
   if &MODE_Q = 22 then &LABEL = 'local bus';
   if &MODE_Q = 22 and &TRANSFER = 0 then transfer_from = 'SONOMA COUNTY TRANSIT';
   if &MODE_Q = 22 and &TRANSFER = 1 then transfer_to   = 'SONOMA COUNTY TRANSIT';
   * Stanford shuttles;
   if &MODE_Q = 23 then &LABEL = 'local bus';
   if &MODE_Q = 23 and &TRANSFER = 0 then transfer_from = 'STANFORD SHUTTLES';
   if &MODE_Q = 23 and &TRANSFER = 1 then transfer_to   = 'STANFORD SHUTTLES';
   * Tri-delta;
   if &MODE_Q = 24 then &LABEL = 'local bus'; 
   if &MODE_Q = 24 and &TRANSFER = 0 then transfer_from = 'TRI-DELTA';
   if &MODE_Q = 24 and &TRANSFER = 1 then transfer_to   = 'TRI-DELTA';
   * Union-city;
   if &MODE_Q = 25 then &LABEL = 'local bus'; 
   if &MODE_Q = 25 and &TRANSFER = 0 then transfer_from = 'UNION CITY';
   if &MODE_Q = 25 and &TRANSFER = 1 then transfer_to   = 'UNION CITY';
   * Vallejo ferry;
   if &MODE_Q = 26 then &LABEL = 'ferry';
   if &MODE_Q = 26 and &TRANSFER = 0 then transfer_from = 'VALLEJO FERRY';
   if &MODE_Q = 26 and &TRANSFER = 1 then transfer_to   = 'VALLEJO FERRY';
   * Vallejo transit;
   if &MODE_Q = 27 then &LABEL = 'local bus';
   if &MODE_Q = 27 and &TRANSFER = 0 then transfer_from = 'SOLTRANS';
   if &MODE_Q = 27 and &TRANSFER = 1 then transfer_to   = 'SOLTRANS';
   * Westcat;
   if &MODE_Q = 28 then &LABEL = 'local bus';
   if &MODE_Q = 28 and &TRANSFER = 0 then transfer_from = 'WESTCAT';
   if &MODE_Q = 28 and &TRANSFER = 1 then transfer_to   = 'WESTCAT';
   * Other;
   if &MODE_Q = 29 then &LABEL = 'local bus';
   if &MODE_Q = 29 and &TRANSFER = 0 then transfer_from = 'OTHER';
   if &MODE_Q = 29 and &TRANSFER = 1 then transfer_to   = 'OTHER';
   
%MEND TM_MODE;
run;


* Set the access mode;
data catiA; set OnBoard.ggtb_cati_purp_asuff;

   * Access mode;
   access_mode = 'Missing                ';
   if q15 = 1 then access_mode = 'walk';
   if q15 = 2 then access_mode = 'bike';
   if q15 = 3 then access_mode = 'pnr';
   if q15 = 4 then access_mode = 'pnr';
   if q15 = 5 then access_mode = 'knr';
   if q15 = 6 then access_mode = 'knr';
   if q15 = 7 then access_mode = 'pnr';
   if access_mode = 'Missing                ' then access_mode = .;

run;

* Set the mode sequence;
data catiB; set catiA;
   
   survey_mode = put('express bus',15.);
   transfer_from = 'None                      ';
   transfer_to   = 'None                      ';

   * Modes prior to the survey mode (re-write the transfer variable each time);
   first_prior_mode = put('None',15.);
   %TM_MODE(first_prior_mode,q19,q20,q22,q43,q46,q49,0);

   second_prior_mode = put('None',15.);
   %TM_MODE(second_prior_mode,q64,q65,q67,q88,q91,q94,0);

   third_prior_mode = put('None',15.);
   %TM_MODE(third_prior_mode,q109,q110,q112,q133,q136,q139,0);

   fourth_prior_mode = put('None',15.);
   %TM_MODE(fourth_prior_mode,q154,q155,q157,q178,q181,q184,0);

   * Modes after the survey mode (transfer to is first after only);
   first_after_mode = put('None',15.);
   %TM_MODE(first_after_mode,q199,q200,q202,q223,q226,q229,1);

   second_after_mode = put('None',15.);
   %TM_MODE(second_after_mode,q244,q245,q247,q268,q271,q274,-9);

   third_after_mode = put('None',15.);
   %TM_MODE(third_after_mode,q289,q290,q292,q313,q316,q319,-9);

   fourth_after_mode = put('None',15.);
   %TM_MODE(fourth_after_mode,q334,q335,q337,q358,q361,q364,-9);


run;

proc freq data = catiB; tables access_mode q19 first_prior_mode q42 second_prior_mode q65 third_prior_mode q88 fourth_prior_mode;
proc freq data = catiB; tables q111 first_after_mode q134 second_after_mode q157 third_after_mode q180 fourth_after_mode;
proc freq data = catiB; tables transfer_from transfer_to;
run;

* Set the egress mode;
data catiC; set catiB;

   * Egress mode;
   egress_mode = 'Missing                ';
   if q378 = 1 then egress_mode = 'walk';
   if q378 = 2 then egress_mode = 'bike';
   if q378 = 3 then egress_mode = 'pnr';
   if q378 = 4 then egress_mode = 'pnr';
   if q378 = 5 then egress_mode = 'knr';
   if q378 = 6 then egress_mode = 'pnr';
   if egress_mode = 'Missing                ' then egress_mode = .;

run;

proc freq data = catiC; tables q19 first_prior_mode q42 second_prior_mode q65 third_prior_mode q88 fourth_prior_mode;
proc freq data = catiC; tables q111 first_after_mode q134 second_after_mode q157 third_after_mode q180 fourth_after_mode;
proc freq data = catiC; tables q15 access_mode q378 egress_mode;
run;


* Determine the Travel Model One path and set a simplified character sequence;
data catiD; set catiC;

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
   if first_prior_mode = 'commuter rail' or second_prior_mode = 'commuter rail' or third_prior_mode = 'commuter rail' or fourth_after_mode = 'commuter rail' or
      first_after_mode = 'commuter rail' or second_after_mode = 'commuter rail' or third_after_mode = 'commuter rail' or fourth_after_mode = 'commuter rail' or
      survey_mode = 'commuter rail' then
	  path_line_haul = 'COM';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'heavy rail' or second_prior_mode = 'heavy rail' or third_prior_mode = 'heavy rail' or fourth_after_mode = 'heavy rail' or
      first_after_mode = 'heavy rail' or second_after_mode = 'heavy rail' or third_after_mode = 'heavy rail' or fourth_after_mode = 'heavy rail' or
      survey_mode = 'heavy rail') then
	  path_line_haul = 'HVY';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'express bus' or second_prior_mode = 'express bus' or third_prior_mode = 'express bus' or fourth_after_mode = 'express bus' or
      first_after_mode = 'express bus' or second_after_mode = 'express bus' or third_after_mode = 'express bus' or fourth_after_mode = 'express bus' or
      survey_mode = 'express bus') then
	  path_line_haul = 'EXP';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'ferry' or second_prior_mode = 'ferry' or third_prior_mode = 'ferry' or fourth_after_mode = 'ferry' or
      first_after_mode = 'ferry' or second_after_mode = 'ferry' or third_after_mode = 'ferry' or fourth_after_mode = 'ferry' or
      survey_mode = 'ferry') then
	  path_line_haul = 'LRF';

   if path_line_haul = 'XXX' and (
      first_prior_mode = 'light rail' or second_prior_mode = 'light rail' or third_prior_mode = 'light rail' or fourth_after_mode = 'light rail' or
      first_after_mode = 'light rail' or second_after_mode = 'light rail' or third_after_mode = 'light rail' or fourth_after_mode = 'light rail' or
      survey_mode = 'light rail') then
	  path_line_haul = 'LRF';

	if path_line_haul = 'XXX' then path_line_haul = 'LOC';

	path_label = cat(path_access, '-', path_line_haul, '-', path_egress);

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

proc freq data = catiD; tables path_access path_egress path_line_haul path_label;
proc freq data = catiD; tables first_transit_mode last_transit_mode;
run;

* Computer number of boardings;
data catiE; set catiD;

  boardings = 1;
  if TRIM(first_prior_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(second_prior_mode) ^= 'None' then boardings = boardings + 1;
  if TRIM(third_prior_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(fourth_prior_mode) ^= 'None' then boardings = boardings + 1;

  if TRIM(first_after_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(second_after_mode) ^= 'None' then boardings = boardings + 1;
  if TRIM(third_after_mode)  ^= 'None' then boardings = boardings + 1;
  if TRIM(fourth_after_mode)  ^= 'None' then boardings = boardings + 1; 

run;

data OnBoard.ggtb_cati_purp_asuff_path; set catiE;
   label path_label    = "Travel Model One Mode Choice Path";
   label access_mode   = "Simplified Access Mode";
   label egress_mode   = "Simplified Egress Mode";
   label transfer_from = "Operator of system transferred immediately before";
   label transfer_to   = "Operator of system transferred immediately after";
   label first_transit_mode = "Mode of first transit encountered";
   label last_transit_mode  = "Mode of last transit encountered";
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 5: Prepare socio-demographic information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data catiA; set OnBoard.ggtb_cati_purp_asuff_path; 

   work_status = 'Missing                ';
   if q382 = 1 then work_status = 'full- or part-time';
   if q382 = 2 then work_status = 'non-worker';
   if orig_purp = 'work' or dest_purp = 'work' then work_status = 'full- or part-time';

   student_status = 'Missing             ';
   if q385 = 1 then student_status = 'full- or part-time';
   if q385 = 2 then student_status = 'non-student';
   if orig_purp = 'grade school' or dest_purp = 'grade school' then student_status = 'full- or part-time';
   if orig_purp = 'high school'  or dest_purp = 'high school'  then student_status = 'full- or part-time';
   if orig_purp = 'university'   or dest_purp = 'university'   then student_status = 'full- or part-time';

   age = 113 - q408;
   if q408 = 1 then age = -9;

   fare_medium = 'Missing                             ';
   if q400 = 1   then fare_medium = 'cash (bills and coins)';
   if q400 = 2   then fare_medium = 'cash (clipper card)';
   if q400 = 3   then fare_medium = 'transfer (golden gate or ferry shuttle)';
   if q400 = 4   then fare_medium = 'other';

   fare_category = 'Missing                             ';
   if q401 = 1 then fare_category = 'adult';
   if q401 = 2 then fare_category = 'youth';
   if q401 = 3 then fare_category = 'senior';
   if q401 = 4 then fare_category = 'disabled';
   if q401 = 5 then fare_category = 'other discount';

   * Match AC Transit: NOT HISPANIC/LATINO OR OF SPANISH ORIGIN or HISPANIC/LATINO OR OF SPANISH ORIGIN;
   hispanic = 'Missing                                                    ';
   if q409 = 1 then hispanic = 'HISPANIC/LATINO OR OF SPANISH ORIGIN';
   if q409 = 2 then hispanic = 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN'; 
   
   * Match AC Transit: BLACK, WHITE, ASIAN, OTHER, MULTIRACIAL;
   race = 'Missing            ';
   if q410 = 1 then race = 'WHITE';
   if q410 = 2 then race = 'BLACK';
   if q410 = 3 then race = 'ASIAN';
   if q410 = 4 then race = 'OTHER';
   if q410 = 5 then race = 'OTHER';
   if q410 = 6 then race = 'OTHER';
   if q410 = 8 then race = 'OTHER';

   * ENGLISH ONLY, ...;
   language_at_home = 'Missing                 ';
   if q411 = 2 then language_at_home = 'ENGLISH ONLY';

   if q412 = 1  then language_at_home = 'SPANISH';
   if q412 = 2  then language_at_home = 'CHINESE-CANTONESE';
   if q412 = 3  then language_at_home = 'CHINESE-MANDARIN';
   if q412 = 4  then language_at_home = 'VIETNAMESE';
   if q412 = 5  then language_at_home = 'KOREAN';
   if q412 = 6  then language_at_home = 'TAGALOG';
   if q412 = 7  then language_at_home = 'RUSSIAN';
   if q412 = 8  then language_at_home = 'PORTUGUESE';
   if q412 = 9  then language_at_home = 'FRENCH';
   if q412 = 10 then language_at_home = 'FRENCH CREOLE';
   if q412 = 11 then language_at_home = 'POLISH';
   if q412 = 12 then language_at_home = 'OTHER'; 

   * English proficiency;
   eng_proficient = 'Missing                             ';
   if q413 = 1 then eng_proficient = 'VERY WELL';
   if q413 = 2 then eng_proficient = 'WELL';
   if q413 = 3 then eng_proficient = 'NOT WELL';
   if q413 = 4 then eng_proficient = 'NOT AT ALL';

   household_income = 'Missing                             ';
   if q414 = 1 then household_income = 'under $35,000';
   if q414 = 2 then household_income = '$35,000 or higher';
   if q414 = 3 then household_income = 'refused';

   if q415 = 1 then household_income = 'under $10,000';
   if q415 = 2 then household_income = '$10,000 to $25,000';
   if q415 = 3 then household_income = '$25,000 to $35,000';

   if q416 = 1 then household_income = '$35,000 to $50,000';
   if q416 = 2 then household_income = '$50,000 to $75,000';
   if q416 = 3 then household_income = '$75,000 to $100,000';
   if q416 = 4 then household_income = '$100,000 to $150,000';
   if q416 = 5 then household_income = '$150,000 or higher';

   sex = 'Missing                             '; 
   if q417 = 1 then sex = 'male';
   if q417 = 2 then sex = 'female';

   * CATI language (phone interview language);
   interview_language = 'Missing                             ';
   if q418 = 1 then interview_language = 'English';
   if q418 = 2 then interview_language = 'Spanish';
   if q418 = 3 then interview_language = 'Cantonese Chinese';
   if q418 = 4 then interview_language = 'Mandarin Chinese';
   if q418 = 5 then interview_language = 'Tagalog';

run;

proc freq data = catiA; tables work_status student_status age fare_medium fare_category hispanic race language_at_home household_income sex interview_language eng_proficient;
run;

data OnBoard.ggtb_cati_purp_asuff_path_sd; set catiA;
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
   label interview_language    = "Language of phone interview";
   label eng_proficient   = "How well do you speak English";
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 7: Other model information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data catiA; set OnBoard.ggtb_cati_purp_asuff_path_sd;

   * Convert the character times to time variables;
   *q396_time = input(q396,time5.);
   *q398_time = input(q398,time5.);

   *format q396_time time5.;
   *format q398_time time5.;

   q396_time = q396;
   q398_time = q398;

   depart_hour = .;
   depart_hour = HOUR(q396_time);
   if q396_time = HMS(1,0,0) then depart_hour = .;
   if q397 = 1 then depart_hour = depart_hour;
   if q397 = 1 and HOUR(q396_time) = 12 then depart_hour = 0;
   if q397 = 2 and HOUR(q396_time) < 12 then depart_hour = depart_hour + 12;

   return_hour = .;
   return_hour = HOUR(q398_time);
   if q398_time = HMS(1,0,0) then return_hour = .;
   if q399 = 1 then return_hour = return_hour;
   if q399 = 1 and HOUR(q398_time) = 12 then return_hour = 0;
   if q399 = 2 and HOUR(q398_time) < 12 then return_hour = return_hour + 12;

run;

proc freq data = catiA; tables depart_hour return_hour;
run;

data catiB; set catiA; keep id weight daypart weekpart orig_purp dest_purp tour_purp autoSuff access_mode survey_mode egress_mode
                            path_access path_egress path_line_haul path_label transfer_from transfer_to boardings 
							first_transit_mode last_transit_mode
							work_status student_status
                            age fare_medium fare_category hispanic race language_at_home household_income sex eng_proficient 
                            interview_language depart_hour return_hour;

   label depart_hour = 'Home-based tour hour of departure from home';
   label return_hour = 'Home-based tour hour of return to home';

run;


data OnBoard.ggtb_cati_ready; set catiB;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 8: Extract data from info file, build a combined data set;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data infoA; set OnBoard.rawGgtbInfo;

   orig_purp_field = "                         ";
   dest_purp_field = "                         ";

   if origin_code = 1  then orig_purp_field = "work";
   if origin_code = 2  then orig_purp_field = "work-related";
   if origin_code = 3  then orig_purp_field = "home";
   if origin_code = 4  then orig_purp_field = "social recreation";
   if origin_code = 5  then orig_purp_field = "shopping";
   if origin_code = 6  then orig_purp_field = "grade or high school";
   if origin_code = 7  then orig_purp_field = "university";
   if origin_code = 8  then orig_purp_field = "other maintenance";
   if origin_code = 9  then orig_purp_field = "other maintenance";
   if origin_code = 10 then orig_purp_field = "other";
   if origin_code = 11 then orig_purp_field = "other";
   if origin_code = 12 then orig_purp_field = "other";
   if origin_code = 13 then orig_purp_field = "other";
   if origin_code = 14 then orig_purp_field = "missing";

   if destination_code = 1  then dest_purp_field = "work";
   if destination_code = 2  then dest_purp_field = "work-related";
   if destination_code = 3  then dest_purp_field = "home";
   if destination_code = 4  then dest_purp_field = "social recreation";
   if destination_code = 5  then dest_purp_field = "shopping";
   if destination_code = 6  then dest_purp_field = "grade or high school";
   if destination_code = 7  then dest_purp_field = "university";
   if destination_code = 8  then dest_purp_field = "other maintenance";
   if destination_code = 9  then dest_purp_field = "other maintenance";
   if destination_code = 10 then dest_purp_field = "other";
   if destination_code = 11 then dest_purp_field = "other";
   if destination_code = 12 then dest_purp_field = "other";
   if destination_code = 13 then dest_purp_field = "other";
   if destination_code = 14 then dest_purp_field = "missing";

   field_language = field_survey_language;

   geo_stop = 0;
   if boarding_latitude ^= 'NA' then geo_stop = 1;

run;

proc freq data = infoA; tables orig_purp_field dest_purp_field geo_stop;
run;

* Bring in the CATI boarding/alighting locations;
data gisA; set OnBoard.rawGgtbGis;

   if q = 7 then survey_boarding_x = point_x;
   if q = 7 then survey_boarding_y = point_y;

   if q = 9 then survey_alighting_x = point_x;
   if q = 9 then survey_alighting_y = point_y;

   if q = 61 then first_boarding_x = point_x;
   if q = 61 then first_boarding_y = point_y;

   if q = 242 then first_after_alighting_x = point_x;
   if q = 242 then first_after_alighting_y = point_y;

   if q = 287 then second_after_alighting_x = point_x;
   if q = 287 then second_after_alighting_y = point_y;

   if q = 332 then third_after_alighting_x = point_x;
   if q = 332 then third_after_alighting_y = point_y;

   if q = 377 then fourth_after_alighting_x = point_x;
   if q = 377 then fourth_after_alighting_y = point_y;
 

proc summary data = gisA threads nway;
  class id;
  var survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;
  output out = gisB
  max = survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;

data gisC; set gisB; keep id survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;
run;

* Joint the info data with gis data;
proc sort data = gisC threads; by id;
proc sort data = infoA threads; by id;

data infoB; merge infoA gisC; by id;
run;

data infoC; set infoB;
  if trim(boarding_latitude)   = 'NA' then boarding_latitude = .;
  if trim(boarding_longitude)  = 'NA' then boarding_longitude = .;
  if trim(alighting_latitude)  = 'NA' then alighting_latitude = .;
  if trim(alighting_longitude) = 'NA' then alighting_longitude = .; 

  * Assume the CATI is more accurate than the field -- replace if needed;
  if survey_boarding_x = . then survey_boarding_x = boarding_longitude;
  if survey_boarding_y = . then survey_boarding_y = boarding_latitude;

  if survey_alighting_x = . then survey_alighting_x = alighting_longitude;
  if survey_alighting_y = . then survey_alighting_y = alighting_latitude; 

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

  if fourth_after_alighting_x ^= . then last_alighting_x = fourth_after_alighting_x;
  if fourth_after_alighting_y ^= . then last_alighting_y = fourth_after_alighting_y;

run;

data OnBoard.ggtb_info_ready; set infoC; keep id route direction field_language orig_purp_field dest_purp_field survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y last_alighting_x last_alighting_y;

   label orig_purp_field = "Travel Model One Activity at origin from field"
         dest_purp_field = "Travel Model One Activity at destination from field";


run;

* Prepare day-of-week files;
data dowA; set OnBoard.rawGgtbDowWkday; keep survey_id day_of_the_week;
   rename survey_id = id;

data dowB; set OnBoard.rawGgtbDowWkend; keep survey_id day_of_the_week;
   rename survey_id = id;

proc append base = dowA data = dowB force;

data OnBoard.ggtb_dow_ready; set dowA;
   label day_of_week = "Day of week of field survey";
run;


* Merge the data sets;
data catiA; set OnBoard.ggtb_cati_ready;
data locA;  set OnBoard.ggtb_key_loc_taz;
data infoA; set OnBoard.ggtb_info_ready;
data dowA;  set OnBoard.ggtb_dow_ready;

proc sort data = catiA threads; by id;
proc sort data = locA  threads; by id;
proc sort data = infoA threads; by id;
proc sort data = dowA  threads; by id;

data comboA; merge catiA locA infoA dowA; by id;

data comboB; set comboA;
   survey_type = 'brief_cati';
   if weight = . then survey_type = 'brief';

proc freq data = comboB; tables survey_type;

* Add the weekpart, operator, field start, and field end information, converted cati (all 1, no field);
data comboC; set comboB;
   operator       = 'Golden Gate Transit (bus)                         ';
*                   '12345678901234567890123456789012345678901234567890';
   field_start    = MDY(09,18,2013); 
   field_end      = MDY(10,01,2013);

data OnBoard.ggtb_ready; set comboC;
run;

* Clear working memory;
proc datasets lib = work kill; 
quit; run;
