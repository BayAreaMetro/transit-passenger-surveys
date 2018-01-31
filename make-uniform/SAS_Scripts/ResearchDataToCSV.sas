* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* ResearchDataToCSV.sas                                                    
*                                                                                            
* Purpose: Create a dataset that provides Census-tract geo-locations for use by researchers
*          and other public sector planning organizations.  
*
* Location: M:\Data\OnBoard\Data and Reports\
*
* Author(s): dto (2014 07 23)
*
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

* Establish the library;
libname OnBoard 'M:\Data\OnBoard\Data and Reports\SAS data';
run;

* Set up the directories;
%let output_directory = M:\Data\OnBoard\Data and Reports\Release Data\_working\2014 07 23 DRAFT Research Data.csv; 
run;

* Remove the brief surveys;
data readyA; set OnBoard.regional_ready;
   if survey_type = 'brief' then delete;
run;

* Remove detailed geographies and unneccesary variables;
data readyB; set readyA; drop id homeMAZ workMAZ schoolMAZ originMAZ destMAZ first_boarding_tap last_alighting_tap orig_purp_field dest_purp_field 
                              path_access path_egress path_line_haul path_label;
run;

* Replace ID with a unique, serial integer, and reformat date;
data write; set readyB; 
   id = _N_;
   format field_start DATE9. field_end DATE9.;
run;

* Write to disk;
proc export data = write
   outfile = "&output_directory."
   replace;
run;




