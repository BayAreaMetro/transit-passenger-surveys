* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Napa Vine transit.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 07 30)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let inter_file  = 'M:\Data\OnBoard\Data and Reports\Napa Vine\As CSV\Napa Vine Tranist OD Survey Data_Aug24_Submitted_20140826.csv';
run;

* Read in the raw data file for the intercept survey;
proc import datafile = "&inter_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 400;
run;


data OnBoard.rawNapa; set input;
   if ID = . then delete;
run;



