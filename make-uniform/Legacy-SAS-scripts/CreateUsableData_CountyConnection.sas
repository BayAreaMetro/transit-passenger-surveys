* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* CreateUsableData.sas                                                    
*                                                                                            
* Purpose: Process and combine the raw consultant-delivered data set into a usable dataset.
*          See BuildDatabases.sas for script to create SAS databases.  Specific to County
*          Connection.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2013 07 16)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the file locations;
%let gis_geocode     = 'M:\Data\OnBoard\Data and Reports\County Connection\Geocode\CATI\gis_geocode.csv';
%let gis_geocode_maz = 'M:\Data\OnBoard\Data and Reports\County Connection\Geocode\CATI\gis_geocode_maz.csv';
%let output_directory = M:\Data\OnBoard\Data and Reports\County Connection\_working;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 1: Compute tour purpose and trip purpose per Travel Model One designations;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Trip purpose (q227 is year of birth);
data catiA; set OnBoard.rawCountyConnectCati;
   orig_purp = "                         ";
   dest_purp = "                         ";

   if q1 = 1  then orig_purp = "work";
   if q1 = 2  then orig_purp = "work-related";
   if q1 = 3  then orig_purp = "home";
   if q1 = 4  then orig_purp = "social recreation";
   if q1 = 5  then orig_purp = "shopping";
   if q1 = 6  then orig_purp = "high school";
   if q1 = 6  and q227>97 then orig_purp = "grade school";
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
   if q2 = 6  and q227>97 then dest_purp = "grade school";
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
   if q10 = 1 and q11 = 1 and q12 = 6  and q227>97 then orig_purp = "grade school";
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

* Start with the fairly straightforward purposes (q206 is worker status, q208 is student status);
data catiB; set catiA;

  * workers;
  if tour_purp = "                         " and orig_purp = "home" and dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and orig_purp = "work" and dest_purp = "home" then tour_purp = "work";

  * students;
  if tour_purp = "                         " and orig_purp = "grade school" or dest_purp = "grade school" then tour_purp = "grade school";
  if tour_purp = "                         " and orig_purp = "high school" or dest_purp = "high school" then tour_purp = "high school";

  * non-working university students;
  if tour_purp = "                         " and q206 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * non-workers, home-based travel (set home to home as other discretionary), could be part of a school tour, but we don't know;
  if tour_purp = "                         " and q206 = 2 and orig_purp = "home" and dest_purp^= "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and q206 = 2 and orig_purp = "home" and dest_purp = "home" then tour_purp = "other discretionary";
  if tour_purp = "                         " and q206 = 2 and orig_purp^= "home" and dest_purp = "home" then tour_purp = orig_purp;
  if tour_purp = "                         " and q206 = 2 and orig_purp^= "home" and dest_purp = orig_purp then tour_purp = orig_purp;

  * non-workers, non-students, non-home-based travel, assign the orig_purp the tour_purp, except escorting;
  if tour_purp = "                         " and q206 = 2 and q208 = 2 and orig_purp^= "home" and dest_purp^= "home" and (orig_purp = "escorting" or dest_purp = "escorting") then tour_purp = "escorting";
  if tour_purp = "                         " and q206 = 2 and q208 = 2 and orig_purp^= "home" and dest_purp^= "home" and orig_purp^= "escorting" and dest_purp^= "escorting" then tour_purp = orig_purp;

run;

* Use the fairly straightforward information from the work questions (q211 is go to work same day, q212 is back to work after trip, q213 is work before trip);
data catiC; set catiB; 

  * if work before then home, assume work tour;
  if tour_purp = "                         " and q211 = 1 and dest_purp = "home"  then tour_purp = "work";

  * if work after, and home before, assume work tour;
  if tour_purp = "                         " and q211 = 2 and orig_purp = "home" then tour_purp = "work";

  * if no work before or after, but work is a leg, assume work tour;
  if tour_purp = "                         " and q211 = 3 and (orig_purp = "work" or dest_purp = "work") then tour_purp = "work";
  
  * if work before, then work, then non-work, assume work tour;
  if tour_purp = "                         " and q211 = 1 and orig_purp = "work" then tour_purp = "work";

  * if work before, then non-work to non-work, assume work tour;
  if tour_purp = "                         " and q211 = 1 and orig_purp ^= "home" then tour_purp = "work"; 


  * If work before leaving home, work is irrelevant;
  if tour_purp = "                         " and q211 = 1 and orig_purp = "home" then tour_purp = dest_purp;

  * If work after arriving home, work is irrelevant;
  if tour_purp = "                         " and q211 = 2 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work that day, work is irrelevant;
  if tour_purp = "                         " and q211 = 3 and orig_purp = "home" then tour_purp = dest_purp;
  if tour_purp = "                         " and q211 = 3 and dest_purp = "home" then tour_purp = orig_purp;

  * If no work and university is present at all, then university;
  if tour_purp = "                         " and q211 = 3 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

  * If no work before or after and same orig and dest, orig is best guess;
  if tour_purp = "                         " and q211 = 3 and orig_purp = dest_purp then tour_purp = orig_purp;

  * Use the return to work for at work tours, assuming anyone going back to work but not stopping at home is making;
  if tour_purp = "                         " and q211 = 1 and orig_purp^= "home" and dest_purp= "work"  then tour_purp = "at work";
  if tour_purp = "                         " and q211 = 2 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and q212 = 1 and orig_purp = "work" and dest_purp^= "home" then tour_purp = "at work";
  if tour_purp = "                         " and q213 = 1 and orig_purp^= "home" and dest_purp = "work" then tour_purp = "at work";

  * If back to work and origin is home, then work tour;
  if tour_purp = "                         " and q212 = 1 and orig_purp = "home" then tour_purp = "work";

  * If work before and destination is home, then work tour;
  if tour_purp = "                         " and q213 = 1 and dest_purp = "home" then tour_purp = "work";

  * If no back to work, but work is a party of the trip, then work;
  if tour_purp = "                         " and q212 = 2 and orig_purp = "work" or dest_purp = "work" then tour_purp = "work";
  if tour_purp = "                         " and q213 = 2 and orig_purp = "work" or dest_purp = "work"  then tour_purp = "work";

  * If no back to work, but university if part of the trip, then university;
  if tour_purp = "                         " and q212 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";
  if tour_purp = "                         " and q213 = 2 and orig_purp = "university" or dest_purp = "university" then tour_purp = "university";

run;

proc freq data = catiC; tables tour_purp orig_purp*dest_purp;
run;


* Move to the ones that require more faith or have odd 390/391/392 combinations;
data catiD; set catiC;
   
   * If you're still left, assume working before or after trip (which does not have a home or work end) puts you on a work tour;
   if tour_purp = "                         " and q211 = 1 then tour_purp = "work";
   if tour_purp = "                         " and q211 = 2 then tour_purp = "work"; 

   * If you're still left, and no working before or after, assume orig_purp;
   if tour_purp = "                         " and q211 = 3 then tour_purp = orig_purp;

   * If you're still left, and went back to work before a home-based trip, then work is irrelevant;
   if tour_purp = "                         " and q212 = 1 and dest_purp = "home" then tour_purp = orig_purp;
   if tour_purp = "                         " and q213 = 1 and orig_purp = "home" then tour_purp = dest_purp;

   * If you're still left, and one end is home, then other end is purpose;
   if tour_purp = "                         " and orig_purp = "home" then tour_purp = dest_purp;
   if tour_purp = "                         " and dest_purp = "home" then tour_purp = orig_purp;

   * If you're still left, and non-home, then orig_purp;
   if tour_purp = "                         " then tour_purp = orig_purp;

   * Change home to other discretionary;
   if tour_purp = "home" then tour_purp = "other discretionary";

run; 

* Check frequencies;
proc freq data = catiD; tables tour_purp;
run;

* Simple data set to do checks with;
data catiE; set catiD; keep id q206 q208 q211 q212 q213 orig_purp dest_purp tour_purp;
run;

data OnBoard.cc_cati_purp; set catiD;
   label orig_purp = "Travel Model One Activity at origin"
         dest_purp = "Travel Model One Activity at destination"
         tour_purp = "Travel Model One Tour purpose (approximate)";

run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 2: Append TAZ geo-codes to home, work, school, origin, and destination;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Read in CATI geo-codes;
data input; infile "&gis_geocode." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format taz1454 best12.;
			format out_of_region best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat taz1454 best32.;
			informat out_of_region best32.;

			input    id  
			         q  
			         qcode $
                     taz1454
			         out_of_region;

run;

data input_maz; infile "&gis_geocode_maz." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format maz best12.;
			format taz best12.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat taz best32.;
			informat maz best32.;

			input    id  
			         q  
			         qcode $
                     maz
                     taz;

run;

* Merge the TM1 and TM2 geographies;
proc sort data = input threads; by id q qcode;
proc sort data = input_maz threads; by id q qcode;

data both; merge input input_maz; by id q qcode;

data input; set both;
run;

* Append the TAZ geo-coding to the GIS files;
data gisA;  set OnBoard.rawCountyConnectGis;
   rename surveyid = id;
data catiA; set OnBoard.cc_cati_purp; keep id q10 q11;
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

  if q = 207 then workTAZ   = taz1454;
  if q = 209 then schoolTAZ = taz1454;
  if q = 210 then homeTAZ   = taz1454;

  if q = 207 then workMAZ   = maz;
  if q = 209 then schoolMAZ = maz;
  if q = 210 then homeMAZ   = maz;

proc summary data = geoD threads nway;
  class id;
  var homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;
  output out = geoE
  max = homeTAZ workTAZ schoolTAZ originTAZ destTAZ homeMAZ workMAZ schoolMAZ originMAZ destMAZ;

data OnBoard.cc_key_loc_taz; set geoE; drop _TYPE_ _FREQ_;
run;

* Set the home and work locations if they are an origin or a destionation;
data catiA; set OnBoard.cc_cati_purp; keep id orig_purp dest_purp;
data gisA; set OnBoard.cc_key_loc_taz;

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

data OnBoard.cc_key_loc_taz; set gisC; drop orig_purp dest_purp;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 3: Automobile sufficiency;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* q225 is number of workers, q226 is number of drivable vehicles;
data catiA; set OnBoard.cc_cati_purp;

proc freq data = catiA; tables q225 q225_other q226 q226_other;
run;

data catiB; set catiA;
   autoSuff = 'Missing                 ';

   if q226 = 16 then autoSuff = 'Zero autos';

   if q225 > q226 and q225<16 and q226<16 then autoSuff = 'Workers > autos';

   if q225 <= q226 and q225<16 and q226<16 then autoSuff = 'Workers <= autos';

   if q225 = 16 and q226<16 then autoSuff = 'Workers <= autos';

data catiB; set catiB;
   if autoSuff = 'Missing                 ' then autoSuff = .;

proc freq data = catiB; tables autoSuff;
run;

data OnBoard.cc_cati_purp_asuff; set catiB;
   label autoSuff = "Travel Model One Automobile Sufficiency";
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 4: Determine mode sequence and Travel Model One mode;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Create a macro to compute the Travel Model One mode from the survey questions, and use
  iteratively for the sequence questions;
%MACRO TM_MODE(LABEL,MODE_Q,AC_SEL_Q,AC_Q,MUNI_Q,TRANSFER);

   * AC Transit;
   if &MODE_Q = 1 and &AC_SEL_Q = 1 then &LABEL = 'local bus'; 
   if &MODE_Q = 1 and &AC_SEL_Q = 2 and (&AC_Q < 62 or &AC_Q > 94) then &LABEL = 'local bus';
   if &MODE_Q = 1 and &AC_SEL_Q = 2 and &AC_Q > 61 then &LABEL = 'express bus';
   if &MODE_Q = 1 and &TRANSFER = 0 then transfer_from = 'AC TRANSIT';
   if &MODE_Q = 1 and &TRANSFER = 1 then transfer_to   = 'AC TRANSIT';
   * Amtrak; 
   if &MODE_Q = 2 then &LABEL = 'commuter rail';
   if &MODE_Q = 2 and &TRANSFER = 0 then transfer_from = 'AMTRAK';
   if &MODE_Q = 2 and &TRANSFER = 1 then transfer_to   = 'AMTRAK';
   * BART;
   if &MODE_Q = 3 then &LABEL = 'heavy rail';
   if &MODE_Q = 3 and &TRANSFER = 0 then transfer_from = 'BART';
   if &MODE_Q = 3 and &TRANSFER = 1 then transfer_to   = 'BART';
   * County connection;
   if &MODE_Q = 4 then &LABEL = 'local bus';
   if &MODE_Q = 4 and &TRANSFER = 0 then transfer_from = 'COUNTY CONNECTION';
   if &MODE_Q = 4 and &TRANSFER = 1 then transfer_to   = 'COUNTY CONNECTION';
   * Golden gate transit;
   if &MODE_Q = 5 then &LABEL = 'express bus';
   if &MODE_Q = 5 and &TRANSFER = 0 then transfer_from = 'GOLDEN GATE TRANSIT';
   if &MODE_Q = 5 and &TRANSFER = 1 then transfer_to   = 'GOLDEN GATE TRANSIT';
   * Marin transit;
   if &MODE_Q = 6 then &LABEL = 'local bus';
   if &MODE_Q = 6 and &TRANSFER = 0 then transfer_from = 'MARIN TRANSIT';
   if &MODE_Q = 6 and &TRANSFER = 1 then transfer_to   = 'MARIN TRANSIT';
   * Muni;
   if &MODE_Q = 7 and &MUNI_Q<15  then &LABEL = 'light rail';
   if &MODE_Q = 7 and &MUNI_Q>=15 then &LABEL = 'local bus';
   if &MODE_Q = 7 and &TRANSFER = 0 then transfer_from = 'MUNI';
   if &MODE_Q = 7 and &TRANSFER = 1 then transfer_to   = 'MUNI';
   * Petaluma transit;
   if &MODE_Q = 8 then &LABEL = 'local bus';
   if &MODE_Q = 8 and &TRANSFER = 0 then transfer_from = 'PETALUMA TRANSIT';
   if &MODE_Q = 8 and &TRANSFER = 1 then transfer_to   = 'PETALUMA TRANSIT';
   * Santa Rosa City Bus;
   if &MODE_Q = 9 then &LABEL = 'local bus';
   if &MODE_Q = 9 and &TRANSFER = 0 then transfer_from = 'SANTA ROSA CITY BUS';
   if &MODE_Q = 9 and &TRANSFER = 1 then transfer_to   = 'SANTA ROSA CITY BUS';
   * Sonoma County transit;
   if &MODE_Q = 10 then &LABEL = 'local bus';
   if &MODE_Q = 10 and &TRANSFER = 0 then transfer_from = 'SONOMA COUNTY TRANSIT';
   if &MODE_Q = 10 and &TRANSFER = 1 then transfer_to   = 'SONOMA COUNTY TRANSIT';
   * Tri-delta;
   if &MODE_Q = 11 then &LABEL = 'local bus'; 
   if &MODE_Q = 11 and &TRANSFER = 0 then transfer_from = 'TRI-DELTA';
   if &MODE_Q = 11 and &TRANSFER = 1 then transfer_to   = 'TRI-DELTA';
   * Vallejo transit;
   if &MODE_Q = 12 then &LABEL = 'local bus';
   if &MODE_Q = 12 and &TRANSFER = 0 then transfer_from = 'VALLEJO TRANSIT';
   if &MODE_Q = 12 and &TRANSFER = 1 then transfer_to   = 'VALLEJO TRANSIT';
   * Westcat;
   if &MODE_Q = 13 then &LABEL = 'local bus';
   if &MODE_Q = 13 and &TRANSFER = 0 then transfer_from = 'WESTCAT';
   if &MODE_Q = 13 and &TRANSFER = 1 then transfer_to   = 'WESTCAT';
   * Other;
   if &MODE_Q = 14 then &LABEL = 'local bus';
   if &MODE_Q = 14 and &TRANSFER = 0 then transfer_from = 'OTHER';
   if &MODE_Q = 14 and &TRANSFER = 1 then transfer_to   = 'OTHER';
   
%MEND TM_MODE;
run;

* Set the access mode;
data catiA; set OnBoard.cc_cati_purp_asuff;

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

* Set the mode sequence and the transfer_from and transfer_to variables;
data catiB; set catiA;
   
   survey_mode = put('local bus',15.);
   transfer_from = 'None                      ';
   transfer_to   = 'None                      ';

   * Modes prior to the survey mode (overwrite the transfer variables each time);
   first_prior_mode = put('None',15.);
   %TM_MODE(first_prior_mode,q19,q20,q22,q38,0);

   second_prior_mode = put('None',15.);
   %TM_MODE(second_prior_mode,q42,q43,q45,q61,0);

   third_prior_mode = put('None',15.);
   %TM_MODE(third_prior_mode,q65,q66,q68,q84,0);

   fourth_prior_mode = put('None',15.);
   %TM_MODE(fourth_prior_mode,q88,q89,q91,q107,0);

   * Modes after the survey mode (first after is the transfer variable);
   first_after_mode = put('None',15.);
   %TM_MODE(first_after_mode,q111,q112,q114,q130,1);

   second_after_mode = put('None',15.);
   %TM_MODE(second_after_mode,q134,q135,q137,q153,-9);

   third_after_mode = put('None',15.);
   %TM_MODE(third_after_mode,q157,q158,q160,q176,-9);

   fourth_after_mode = put('None',15.);
   %TM_MODE(fourth_after_mode,q180,q181,q183,q199,-9);


run;

proc freq data = catiB; tables access_mode q19 first_prior_mode q42 second_prior_mode q65 third_prior_mode q88 fourth_prior_mode;
proc freq data = catiB; tables q111 first_after_mode q134 second_after_mode q157 third_after_mode q180 fourth_after_mode;
proc freq data = catiB; tables transfer_from transfer_to;
run;

* Set the egress mode;
data catiC; set catiB;

   * Egress mode;
   egress_mode = 'Missing                ';
   if q202 = 1 then egress_mode = 'walk';
   if q202 = 2 then egress_mode = 'bike';
   if q202 = 3 then egress_mode = 'pnr';
   if q202 = 4 then egress_mode = 'pnr';
   if q202 = 5 then egress_mode = 'knr';
   if q202 = 6 then egress_mode = 'pnr';
   if egress_mode = 'Missing                ' then egress_mode = .;

run;

proc freq data = catiC; tables q19 first_prior_mode q42 second_prior_mode q65 third_prior_mode q88 fourth_prior_mode;
proc freq data = catiC; tables q111 first_after_mode q134 second_after_mode q157 third_after_mode q180 fourth_after_mode;
proc freq data = catiC; tables q15 access_mode q202 egress_mode;
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

* Compare to Mark's trip_legs_code;
proc freq data = catiE; tables leg_categories boardings boardings*leg_categories;
run;

data OnBoard.cc_cati_purp_asuff_path; set catiE;
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

data catiA; set OnBoard.cc_cati_purp_asuff_path; 

   work_status = 'Missing                ';
   if q206 = 1 then work_status = 'full- or part-time';
   if q206 = 2 then work_status = 'non-worker';
   if orig_purp = 'work' or dest_purp = 'work' then work_status = 'full- or part-time';

   student_status = 'Missing             ';
   if q208 = 1 then student_status = 'full- or part-time';
   if q208 = 2 then student_status = 'non-student';
   if orig_purp = 'grade school' or dest_purp = 'grade school' then student_status = 'full- or part-time';
   if orig_purp = 'high school'  or dest_purp = 'high school'  then student_status = 'full- or part-time';
   if orig_purp = 'university'   or dest_purp = 'university'   then student_status = 'full- or part-time';

   age = 112 - q227;
   if q227 = 1 then age = -9;

   fare_medium = 'Missing                             ';
   if q218 = 1  then fare_medium = 'transfer (bart paper)';
   if q218 = 2  then fare_medium = 'cash (bills and coins)';
   if q218 = 3  then fare_medium = 'pass (monthly)';
   if q218 = 4  then fare_medium = 'pass (montly express)';
   if q218 = 5  then fare_medium = 'pass (12-ride punch)';
   if q218 = 6  then fare_medium = 'pass (12-ride express)';
   if q218 = 7  then fare_medium = 'pass (commuter card)';
   if q218 = 8  then fare_medium = 'pass (rtc card)';
   if q218 = 9  then fare_medium = 'transfer (county connection paper)';
   if q218 = 10 then fare_medium = 'other';

   fare_category = 'Missing                             ';
   if q219 = 1 then fare_category = 'adult';
   if q219 = 2 then fare_category = 'youth';
   if q219 = 3 then fare_category = 'senior';
   if q219 = 4 then fare_category = 'disabled';
   if q219 = 5 then fare_category = 'other discount';

   * Match AC Transit: NOT HISPANIC/LATINO OR OF SPANISH ORIGIN or HISPANIC/LATINO OR OF SPANISH ORIGIN;
   hispanic = 'Missing                                                    ';
   if q228 = 1 then hispanic = 'HISPANIC/LATINO OR OF SPANISH ORIGIN';
   if q228 = 2 then hispanic = 'NOT HISPANIC/LATINO OR OF SPANISH ORIGIN'; 
   
   * Match AC Transit: BLACK, WHITE, ASIAN, OTHER, MULTIRACIAL;
   race = 'Missing            ';
   if q229 = 1 then race = 'WHITE';
   if q229 = 2 then race = 'BLACK';
   if q229 = 3 then race = 'ASIAN';
   if q229 = 4 then race = 'OTHER';
   if q229 = 5 then race = 'OTHER';
   if q229 = 6 then race = 'OTHER';

   * Match AC Transit: ENGLISH ONLY, ...;
   language_at_home = 'Missing                 ';
   if q230 = 2 then language_at_home = 'ENGLISH ONLY';
   if q231 = 1  then language_at_home = 'SPANISH';
   if q231 = 2  then language_at_home = 'CHINESE';
   if q231 = 3  then language_at_home = 'VIETNAMESE';
   if q231 = 4  then language_at_home = 'KOREAN';
   if q231 = 5  then language_at_home = 'TAGALOG';
   if q231 = 6  then language_at_home = 'RUSSIAN';
   if q231 = 7  then language_at_home = 'PORTUGUESE';
   if q231 = 8  then language_at_home = 'FRENCH';
   if q231 = 9  then language_at_home = 'FRENCH CREOLE';
   if q231 = 10 then language_at_home = 'POLISH';
   if q231 = 11 then language_at_home = 'OTHER'; 

   household_income = 'Missing                             ';
   if q232 = 1 then household_income = 'under $35,000';
   if q232 = 2 then household_income = '$35,000 or higher';
   if q232 = 3 then household_income = 'refused';

   if q233 = 1 then household_income = 'under $10,000';
   if q233 = 2 then household_income = '$10,000 to $25,000';
   if q233 = 3 then household_income = '$25,000 to $35,000';

   if q234 = 1 then household_income = '$35,000 to $50,000';
   if q234 = 2 then household_income = '$50,000 to $75,000';
   if q234 = 3 then household_income = '$75,000 to $100,000';
   if q234 = 4 then household_income = '$100,000 to $150,000';
   if q234 = 5 then household_income = '$150,000 or higher';

   sex = 'Missing                             '; 
   if q235 = 1 then sex = 'male';
   if q235 = 2 then sex = 'female';

   * TODO: check that this is correct interpretation with Mark;
   interview_language = 'Missing                             ';
   if language = 1 then interview_language = 'English';
   if language = 2 then interview_language = 'Spanish';
   if language = 3 then interview_language = 'Cantonese Chinese';
   if language = 4 then interview_language = 'Mandarin Chinese';
 

run;

proc freq data = catiA; tables work_status student_status age fare_medium fare_category hispanic race language_at_home household_income sex interview_language;
run;

data OnBoard.cc_cati_purp_asuff_path_sd; set catiA;
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
run;
 

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 7: Other model information;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~; 

data catiA; set OnBoard.cc_cati_purp_asuff_path_sd;

  * Convert the character times to time variables;
  q214_time = input(q214,time12.);
  q216_time = input(q216,time12.);

   depart_hour = .;
   depart_hour = HOUR(q214_time);
   if q214_time = HMS(1,0,0) then depart_hour = .;
   if q215 = 1 then depart_hour = depart_hour;
   if q215 = 1 and HOUR(q216_time) = 12 then depart_hour = 0; 
   if q215 = 2 and HOUR(q214_time) < 12 then depart_hour = depart_hour + 12;

   return_hour = .;
   return_hour = HOUR(q216_time);
   if q216_time = HMS(1,0,0) then return_hour = .;
   if q217 = 1 then return_hour = return_hour;
   if q217 = 1 and HOUR(q216_time) = 12 then return_hour = 0;
   if q217 = 2 and HOUR(q216_time)<12 then return_hour = return_hour + 12;

run;

proc freq data = catiA; tables depart_hour return_hour;
run;

data catiB; set catiA; keep id weight orig_purp dest_purp tour_purp autoSuff access_mode survey_mode egress_mode
                            path_access path_egress path_line_haul path_label transfer_from transfer_to boardings 
							first_transit_mode last_transit_mode
							work_status student_status
                            age fare_medium fare_category hispanic race language_at_home household_income sex 
                            interview_language depart_hour return_hour;

   label depart_hour = 'Home-based tour hour of departure from home';
   label return_hour = 'Home-based tour hour of return to home';

run;


data OnBoard.cc_cati_ready; set catiB;
run;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Step 8: Extract useful data from Info file, build a combined data set, then segment weekend and weekday;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

data infoA; set OnBoard.rawCountyConnectInfo;

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
   if origin_code = 10 then orig_purp_field = "missing";
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
   if destination_code = 10 then dest_purp_field = "missing";
   if destination_code = 14 then dest_purp_field = "missing";

   field_language = language;
   id = survey_id;

run;

proc freq data = infoA; tables orig_purp_field dest_purp_field;
run;

* For the smaller operators, the boarding and alighting information is in the GIS file;
data gisA; set OnBoard.rawCountyConnectGis;
   if q = 7 then survey_boarding_x = point_x;
   if q = 7 then survey_boarding_y = point_y;

   if q = 9 then survey_alighting_x = point_x;
   if q = 9 then survey_alighting_y = point_y;

   if q = 39 then first_boarding_x = point_x;
   if q = 39 then first_boarding_y = point_y;

   if q = 132 then first_after_alighting_x = point_x;
   if q = 132 then first_after_alighting_y = point_y;

   if q = 155 then second_after_alighting_x = point_x;
   if q = 155 then second_after_alighting_y = point_y;

   if q = 178 then third_after_alighting_x = point_x;
   if q = 178 then third_after_alighting_y = point_y;

   if q = 201 then fourth_after_alighting_x = point_x;
   if q = 201 then fourth_after_alighting_y = point_y;

   id = surveyid;

proc summary data = gisA threads nway;
  class id;
  var survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;
  output out = gisB
  max = survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;

data gisC; set gisB; keep id survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y first_after_alighting_x first_after_alighting_y second_after_alighting_x second_after_alighting_y third_after_alighting_x third_after_alighting_y fourth_after_alighting_x fourth_after_alighting_y;

* Joint the info data with gis data;
proc sort data = gisC threads; by id;
proc sort data = infoA threads; by id;

data infoB; merge infoA gisC; by id;
run;

* Rectify the different x/y;
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

data OnBoard.cc_info_ready; set infoC; keep id route direction mtc_daypart field_language orig_purp_field dest_purp_field survey_boarding_x survey_boarding_y survey_alighting_x survey_alighting_y first_boarding_x first_boarding_y last_alighting_x last_alighting_y;

   label orig_purp_field = "Travel Model One Activity at origin from field"
         dest_purp_field = "Travel Model One Activity at destination from field";

  rename mtc_daypart = daypart;


run;

* Prepare day-of-week files;
data dowA; set OnBoard.rawCountyConnectDow; keep survey_id day_of_the_week;
   rename survey_id = id;

data OnBoard.cc_dow_ready; set dowA;
   label day_of_week = "Day of week of field survey";
run;



* Merge the data sets;
data catiA; set OnBoard.cc_cati_ready;
data locA;  set OnBoard.cc_key_loc_taz;
data infoA; set OnBoard.cc_info_ready;
data dowA;  set OnBoard.cc_dow_ready;

proc sort data = catiA threads; by id;
proc sort data = locA  threads; by id;
proc sort data = infoA threads; by id;
proc sort data = dowA  threads; by id;

data combo; merge catiA locA infoA dowA; by id;

* Add the operator, field start, and field end information (no stop geo-coding was done for county connection);
data comboA; set combo;
   operator       = 'County Connection                                 ';
*                   '12345678901234567890123456789012345678901234567890';
   field_start    = MDY(5,12,2012); 
   field_end      = MDY(5,19,2012);

data comboB; set comboA;
   survey_type = 'brief_cati';
   if weight = . then survey_type = 'brief';

data OnBoard.cc_ready; set comboB;
run;

* Build weekday and weekend files;
data rawA; set OnBoard.rawCountyConnectCati; keep id weekpart;
data readyA; set OnBoard.cc_ready;

proc sort data = rawA threads; by id;
proc sort data = readyA threads; by id;

data combo; merge rawA readyA; by id;

data OnBoard.cc_ready; set combo;

data comboA; set combo;
   if weekpart = 'WEEKDAY' then delete;

data OnBoard.cc_wend_ready; set comboA;
run;

data comboB; set combo;
   if weekpart = 'WEEKEND' then delete;
 
data OnBoard.cc_wday_ready; set comboB;
run;

* Clear working memory;
proc datasets lib = work kill; 
quit; run;
