* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* BuildDatabases.sas                                                    
*                                                                                            
* Purpose: Read in survey records and store in SAS database.  Specific to Livermore Amador 
*          Valley transit (LAVTA).
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 01 24)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Load the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set the name of the raw data files;
%let cati_file_wkday = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekday CATI Data.csv';
%let gis_file_wkday  = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekday GIS Data.csv';
%let info_file_wkday = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekday Info File.csv';
%let dow_file_wkday  = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA (WHEELS) DOW Weekday.csv';

%let cati_file_wkend = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekend CATI Data.csv';
%let gis_file_wkend  = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekend GIS Data.csv';
%let info_file_wkend = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA Weekend Info File.csv';
%let dow_file_wkend  = 'M:\Data\OnBoard\Data and Reports\LAVTA\Redhill Data as CSV\LAVTA (WHEELS) DOW Weekend.csv';

run;

* Read in the raw cati file -- weekday;
proc import datafile = "&cati_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 330;
run;


data OnBoard.rawLavtaCatiWkday; set input;
run;


* Read in the raw cati file -- weekend;
proc import datafile = "&cati_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 110;
run;


data OnBoard.rawLavtaCatiWkend; set input;
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

data OnBoard.rawLavtaGisWkday; set input;
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

data OnBoard.rawLavtaGisWkend; set input;
run;
 
* Read in the raw field info file -- weekday;
proc import datafile = "&info_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1240;
run;

data OnBoard.rawLavtaInfoWkday; set input;
run;

* Read in the raw field info file -- weekend;
proc import datafile = "&info_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 500;
run;

data OnBoard.rawLavtaInfoWkend; set input;
run;

* Read in the raw day-of-the-week file -- weekday;
proc import datafile = "&dow_file_wkday."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 1240;
run;

data OnBoard.rawLavtaDowWkday; set input;
run;

* Read in the raw day of the week file -- weekend;
proc import datafile = "&dow_file_wkend."
            dbms = dlm
			out = input replace;
			delimiter = ",";
			getnames = yes;
			guessingrows = 500;
run;

data OnBoard.rawLavtaDowWkend; set input;
run;

