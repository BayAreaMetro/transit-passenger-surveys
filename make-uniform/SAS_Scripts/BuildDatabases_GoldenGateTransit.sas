* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Golden Gate Transit 
*          (the bus service -- a separate file reads in the ferry data).
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
%let cati_file_wkday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekday CATI Data.csv';
%let gis_file_wkday  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekday GIS Data.csv';
%let info_file_wkday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekday Info File.csv';
%let dow_file_wkday  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit DOW Weekday.csv';

%let cati_file_wkend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekend CATI Data.csv';
%let gis_file_wkend  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekend GIS Data.csv';
%let info_file_wkend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit Weekend Info File.csv';
%let dow_file_wkend  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Transit DOW Weekend.csv';

run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 660;
run;


data OnBoard.rawGgtbCatiWkday; set input;
run;


* Read in the raw cati file -- weekend;
proc import datafile = "&cati_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 300;
run;


data OnBoard.rawGgtbCatiWkend; set input;
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
			format servtype $8.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat location $120.;
			informat location_details $120.;
			informat point_x best32.;
			informat point_y best32.;
			informat servtype $8.;

			input    id  
			         q  
			         qcode $
                     location $
                     location_details $ 
			         point_x  
			         point_y
					 servtype;

run;

data OnBoard.rawGgtbGisWkday; set input;
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
			format servtype $8.;

			informat id best32.;
			informat q best32.;
			informat qcode $20.;
			informat location $120.;
			informat location_details $120.;
			informat point_x best32.;
			informat point_y best32.;
			informat servtype $8.;

			input    id  
			         q  
			         qcode $
                     location $
                     location_details $ 
			         point_x  
			         point_y
					 servtype;

run;

data OnBoard.rawGgtbGisWkend; set input;
run;
 
* Read in the raw field info file -- weekday;
proc import datafile = "&info_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 2490;
run;

data OnBoard.rawGgtbInfoWkday; set input;
run;

* Read in the raw field info file -- weekend;
proc import datafile = "&info_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 960;
run;

data OnBoard.rawGgtbInfoWkend; set input;
run;

* Read in the raw day-of-week file -- weekday;
proc import datafile = "&dow_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 960;
run;

data OnBoard.rawGgtbDowWkday; set input;
run;

* Read in the raw day-of-week file -- weekend;
proc import datafile = "&dow_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 960;
run;

data OnBoard.rawGgtbDowWkend; set input;
run;
