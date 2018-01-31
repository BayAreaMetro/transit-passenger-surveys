* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to BART -- the 
* pilot survey to date.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 08 07)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let inter_file  = 'M:\Data\OnBoard\Data and Reports\BART\As CSV\BART Pilot Test Data_Reviewed_Aug23 DATE FIX.csv';
run;

* Read in the raw data file for the intercept survey;
proc import datafile = "&inter_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1495;
run;


data OnBoard.rawBart; set input;
   if ID = . then delete;
run;



