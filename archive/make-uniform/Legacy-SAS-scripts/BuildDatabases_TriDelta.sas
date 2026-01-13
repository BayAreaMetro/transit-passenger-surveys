* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Tri Delta 
*          transit.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 07 07)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let inter_file  = 'M:\Data\OnBoard\Data and Reports\Tri Delta\As CSV\Tri Delta_OnBoard_InterceptSurvey_Aug 24_Submitted_20140826.csv';
%let on_off_file = 'M:\Data\OnBoard\Data and Reports\Tri Delta\As CSV\Tri Delta_On2OffSurvey_Data_July1_Submitted_v2.csv';
run;

* Read in the raw data file for the intercept survey;
proc import datafile = "&inter_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1250;
run;


data OnBoard.rawTriInter; set input;
   if ID = . then delete;
run;


* Read in the raw data file for the on off survey;
proc import datafile = "&on_off_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 7290;
run;


data OnBoard.rawTriOnOff; set input;
  if ID = . then delete;
run;

