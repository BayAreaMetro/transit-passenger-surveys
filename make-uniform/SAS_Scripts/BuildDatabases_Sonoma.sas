* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Sonoma county 
*          transit.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 03 13)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Note: Sonoma County transit data only collected on weekdays;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file = 'M:\Data\OnBoard\Data and Reports\Sonoma County\Redhill Data as CSV\Sonoma County Transit 2012 CATI Data.csv';
%let gis_file  = 'M:\Data\OnBoard\Data and Reports\Sonoma County\Redhill Data as CSV\Sonoma County Transit 2012 GIS Data.csv';
%let info_file = 'M:\Data\OnBoard\Data and Reports\Sonoma County\Redhill Data as CSV\Sonoma County CATI Info File.csv';
%let dow_file  = 'M:\Data\OnBoard\Data and Reports\Sonoma County\Redhill Data as CSV\Sonoma County Transit DOW.csv';
run;

* Read in the raw cati file, which has weekday and weekend data;
proc import datafile = "&cati_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 270;
run;


data OnBoard.rawSonomaCati; set input;
run;

data input; infile "&gis_file." delimiter = ',' missover scanover dsd
            lrecl = 32767 firstobs = 2;

			format id best12.;
			format q best12.;
			format qcode $20.;
			format location $120.;
			format location_details $120.;
			format point_x best24.;
			format point_y best24.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat location $120.;
			informat location_details $120.;
			informat point_x best32.;
			informat point_y best32.;

			input    id  
			         q  
			         qcode $
                     location $
                     location_details $ 
			         point_x  
			         point_y;

run;

data OnBoard.rawSonomaGis; set input;
run;
 
* Read in the raw field info file;
proc import datafile = "&info_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 180;
run;

data OnBoard.rawSonomaInfo; set input;
run;

* Read in the raw day of the week file;
proc import datafile = "&dow_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1000;
run;

data OnBoard.rawSonomaDow; set input;
run;

