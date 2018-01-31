* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to County Connection.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 03 13)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file  = 'M:\Data\OnBoard\Data and Reports\County Connection\Redhill Data as CSV\County Connection CATI DATA Weekday-Weekend (With MTC Dayparts).csv';
%let gis_file   = 'M:\Data\OnBoard\Data and Reports\County Connection\Redhill Data as CSV\County Connection 2012 GIS Data.csv';
%let info_file  = 'M:\Data\OnBoard\Data and Reports\County Connection\Redhill Data as CSV\County Connection 2012 CATI Info (With MTC dayparts).csv';
%let dow_file   = 'M:\Data\OnBoard\Data and Reports\County Connection\Redhill Data as CSV\The County Connection DOW.csv';
run;

* Read in the raw cati file, which has weekday and weekend data;
proc import datafile = "&cati_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 800;
run;


data OnBoard.rawCountyConnectCati; set input;
run;

* Read in the raw gis file, which has weekday and weekend data;
proc import datafile = "&gis_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 4900;
run;

data OnBoard.rawCountyConnectGis; set input;
run;

* Read in the raw field info file, which has weekday and weekend data;
proc import datafile = "&info_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 2200;
run;

data OnBoard.rawCountyConnectInfo; set input;
run;

* Read in the raw day-of-week file, which has weekday and weekend data;
proc import datafile = "&dow_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 2200;
run;

data OnBoard.rawCountyConnectDow; set input;
run;


