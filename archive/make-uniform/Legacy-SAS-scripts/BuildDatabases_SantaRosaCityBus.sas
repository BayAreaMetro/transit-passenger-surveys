* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Santa Rosa City Bus.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2013 07 16)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Note: Santa Rosa City Bus data only collected on weekdays;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file = 'M:\Data\OnBoard\Data and Reports\Santa Rosa CityBus\Redhill Data as CSV\CityBus2012 CATI Data (updated).csv';
%let gis_file  = 'M:\Data\OnBoard\Data and Reports\Santa Rosa CityBus\Redhill Data as CSV\CityBus2012 GIS Data.csv';
%let info_file = 'M:\Data\OnBoard\Data and Reports\Santa Rosa CityBus\Redhill Data as CSV\CityBus2012 CATI Info (with mtc dayparts).csv';
%let dow_file  = 'M:\Data\OnBoard\Data and Reports\Santa Rosa CityBus\Redhill Data as CSV\Santa Rosa CityBus DOW.csv';
run;

* Read in the raw cati file, which has weekday and weekend data;
proc import datafile = "&cati_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 600;
run;


data OnBoard.rawCityBusCati; set input;
run;

* Read in the raw gis file, which has weekday and weekend data;
proc import datafile = "&gis_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 3500;
run;

data OnBoard.rawCityBusGis; set input;
run;
 
* Read in the raw field info file, which has weekday and weekend data;
proc import datafile = "&info_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1700;
run;

data OnBoard.rawCityBusInfo; set input;
run;

* Read in the raw day of the week file, which has weekday and weekend data;
proc import datafile = "&dow_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1700;
run;

data OnBoard.rawCityBusDow; set input;
run;



