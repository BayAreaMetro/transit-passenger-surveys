* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* LegacyDataToCSV.sas                                                    
*                                                                                            
* Purpose: Create a dataset that provides all of the legacy data in CSV format, so it can
*          then be merged with the on-going summaries being done in R.  
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 11 24)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set up the directories;
%let output_file = M:\Data\OnBoard\Data and Reports\SAS data\regional_ready.csv; 
run;

* Remove the brief surveys;
data write; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;
   format field_start mmddyy10.;
   format field_end mmddyy10.;
run;

* Write to disk;
proc export data = write
   outfile = "&output_file."
   replace;
run;




