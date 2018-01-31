* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Golden Gate Transit 
*          (the ferry service -- a separate file reads in the ferry data).
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 02 03)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file_wkday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekday CATI Data.csv';
%let gis_file_wkday  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekday GIS Data.csv';
%let info_file_wkday = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekday Info File.csv';
%let dow_file_wkday  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry DOW Weekday.csv';

%let cati_file_wkend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekend CATI Data.csv';
%let gis_file_wkend  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekend GIS Data.csv';
%let info_file_wkend = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry Weekend Info File.csv';
%let dow_file_wkend  = 'M:\Data\OnBoard\Data and Reports\Golden Gate Transit\Redhill Data as CSV\Golden Gate Ferry DOW Weekend.csv';

run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 370;
run;


data OnBoard.rawGgtfCatiWkday; set input;
run;


* Read in the raw cati file -- weekend;
proc import datafile = "&cati_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 180;
run;


data OnBoard.rawGgtfCatiWkend; set input;
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

data OnBoard.rawGgtfGisWkday; set input;
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

data OnBoard.rawGgtfGisWkend; set input;
run;
 
* Read in the raw field info file -- weekday;
proc import datafile = "&info_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1645;
run;

data OnBoard.rawGgtfInfoWkday; set input;
run;

* Read in the raw field info file -- weekend;
proc import datafile = "&info_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 600;
run;

data OnBoard.rawGgtfInfoWkend; set input;
run;

* Read in the raw day-of-week file -- weekday;
proc import datafile = "&dow_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 600;
run;

data OnBoard.rawGgtfDowWkday; set input;
run;

* Read in the raw day-of-week file -- weekend;
proc import datafile = "&dow_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 600;
run;

data OnBoard.rawGgtfDowWkend; set input;
run;
