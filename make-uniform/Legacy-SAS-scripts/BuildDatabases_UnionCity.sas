* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Union City.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2013 09 04)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Union City only surveyed weekday passengers;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file = 'M:\Data\OnBoard\Data and Reports\Union City\Redhill Data as CSV\Union City Transit CATI Data.csv';
%let gis_file  = 'M:\Data\OnBoard\Data and Reports\Union City\Redhill Data as CSV\Union City Transit GIS Data.csv';
%let info_file = 'M:\Data\OnBoard\Data and Reports\Union City\Redhill Data as CSV\Union City Transit Info File.csv';
%let dow_file  = 'M:\Data\OnBoard\Data and Reports\Union City\Redhill Data as CSV\Union City Transit DOW.csv';
run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 100;
run;

data OnBoard.rawUnionCati; set input;
run;


* Read in the raw gis file -- weekday;
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

data OnBoard.rawUnionGis; set input;
run;
 
* Read in the raw field info file;
proc import datafile = "&info_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 330;
run;

data OnBoard.rawUnionInfo; set input;
run;

* Read in the raw day of week file;
proc import datafile = "&dow_file."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 330;
run;

data OnBoard.rawUnionDow; set input;
run;




