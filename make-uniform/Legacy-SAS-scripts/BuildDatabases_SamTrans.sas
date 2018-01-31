* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to SamTrans.
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2013 08 25)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file_wkday = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekday CATI Data.csv';
%let gis_file_wkday  = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekday GIS Data.csv';
%let info_file_wkday = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekday Info File.csv';
%let dow_file_wkday  = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans DOW Weekday.csv';

%let cati_file_wkend = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekend CATI Data.csv';
%let gis_file_wkend  = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekend GIS Data.csv';
%let info_file_wkend = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans Weekend Info File.csv';
%let dow_file_wkend  = 'M:\Data\OnBoard\Data and Reports\SamTrans\Redhill Data as CSV\SamTrans DOW Weekend.csv';

run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 2600;
run;

data OnBoard.rawSamCatiWkday; set input;
run;

* Read in the raw cati file -- weekend;
proc import datafile = "&cati_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 320;
run;

data OnBoard.rawSamCatiWkend; set input;
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

data OnBoard.rawSamGisWkday; set input;
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

data OnBoard.rawSamGisWkend; set input;
run;
 
* Read in the raw field info file -- weekday;
proc import datafile = "&info_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 6800;
run;

data OnBoard.rawSamInfoWkday; set input;
run;

* Read in the raw field info file -- weekend;
proc import datafile = "&info_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 990;
run;

data OnBoard.rawSamInfoWkend; set input;
run;

* Read in the raw day of the week file -- weekday;
proc import datafile = "&dow_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 6800;
run;

data OnBoard.rawSamDowWkday; set input;
run;

* Read in the raw day of the week file -- weekend;
proc import datafile = "&dow_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 990;
run;

data OnBoard.rawSamDowWkend; set input;
run;


