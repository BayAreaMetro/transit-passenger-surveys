* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Altamont Commuter 
*          Express (ACE).
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 06 17) WORK IN PROGRESS
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let data_file = 'M:\Data\OnBoard\Data and Reports\ACE\Redhill Data as CSV\ACE_2014 Final DataSet.csv';
run;

* Read in the raw cati file -- weekday;
proc import datafile = "&data_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 535;
run;


data OnBoard.rawAce; set input;
run;



