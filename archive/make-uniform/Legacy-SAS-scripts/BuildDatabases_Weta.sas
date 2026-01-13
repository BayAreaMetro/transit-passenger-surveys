* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Water Emergency
*          Transportation Authority (WETA).
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
%let cati_file_wkday = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekday CATI Data.csv';
%let gis_file_wkday  = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekday GIS Data.csv';
%let info_file_wkday = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekday Info File.csv';
%let dow_file_wkday  = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA DOW Weekday.csv';

%let cati_file_wkend = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekend CATI Data.csv';
%let gis_file_wkend  = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekend GIS Data.csv';
%let info_file_wkend = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA Weekend Info File.csv';
%let dow_file_wkend  = 'M:\Data\OnBoard\Data and Reports\WETA\Redhill Data as CSV\WETA DOW Weekend.csv';
run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 290;
run;


data OnBoard.rawWetaCatiWkday; set input;
run;


* Read in the raw cati file -- weekend;
proc import datafile = "&cati_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 125;
run;


data OnBoard.rawWetaCatiWkend; set input;
run;

* Read in the raw gis file -- weekday;
data input; infile "&gis_file_wkday." delimiter = ',' missover scanover dsd
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

data OnBoard.rawWetaGisWkday; set input;
run;

* Read in the raw gis file -- weekend;
data input; infile "&gis_file_wkend." delimiter = ',' missover scanover dsd
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

data OnBoard.rawWetaGisWkend; set input;
run;
 
* Read in the raw field info file -- weekday;
proc import datafile = "&info_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1340;
run;

data OnBoard.rawWetaInfoWkday; set input;
run;

* Read in the raw field info file -- weekend;
proc import datafile = "&info_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 530;
run;

data OnBoard.rawWetaInfoWkend; set input;
run;

* Read in the raw day of the week file -- weekday;
proc import datafile = "&dow_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1340;
run;

data OnBoard.rawWetaDowWkday; set input;
run;


* Read in the raw day of the week file -- weekend;
proc import datafile = "&dow_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 530;
run;

data OnBoard.rawWetaDowWkend; set input;
run;

