# Transit Passenger Survey Tableau Dashboards - 2024
## Overview
This Tableau workbook contains the dashboards and underlying worksheets for the Transit Passenger Survey Equity Dashboard, for the [Transit Passenger Survey](https://bayareametro.github.io/transit-passenger-surveys/) administered between 2012 - 2019. 
It will eventually be merged with a post-COVID transit rider dataset to compare changes in ridership between the two periods. 

**Key Links:**
- [Asana Task](https://app.asana.com/0/12291104512642/1206214494718647/f)
- [Internal Tableau Dashboard online](https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/views/TransitPassengerSurveyDraft7_22_DRAFT/LandingPage?:iid=1)
- [Tableau Workbook](https://github.com/BayAreaMetro/transit-passenger-surveys/blob/master/summaries/Kyler_Full%20TPS%20Dataset%20with%20Distances%20Appended.twb)
- [Script that built main TPS dataset](https://github.com/BayAreaMetro/transit-passenger-surveys/blob/master/make-uniform/production/Build_Full_Public_Database_with_Distances_Appended_from_Combined.R)
- [Folder](https://github.com/BayAreaMetro/transit-passenger-surveys/tree/master/summaries/Tableau%20Dashboards) with scripts to pull ACS & PUMS dbaseline demographic data

## Navigating the Workbook
These colors proceed in the workbook from left to right in order. 
Tabs underlined in green are the dashboards that will ultimately be live and publically viewable. 
Tabs underlined in blue are the underlying sheets that feed into the dashboards. 
Tabs underlined in yellow also feed into the dashboards, but are demographic profiles from the TPS dataset or public census surveys (ACS or PUMS).
Tabs underlined in red are other exploratory worksheets that do not appear in any current dashboards. 

## Key Data and Variable Notes
Trip counts are generally represented by the "Weight" variable. This is a weighted representation of boardings (unlinked transit trips), which is distinct from the variable for Boardings or Trips or Weighted Trips or Wtd Avg Boardings, and should be used for most cases. 

We have also grouped key variables with an unwieldy number of categories such as Fare Category, Destination Purpose (Dest Purp), Access Mode, and Language at Home. Other variables have been grouped as a renaming technique for clarity. Most of these new variables have (group) at the end of their name. Furthermore, some of these new group variables have an accompaning Set variable (i.e., Day Part (group) Set, Household Income (group) Set, etc) that filters out Missing and Null values for easier use in visualizations. 

## Data Sources
The data sources and R scripts that pull ACS and PUMS data are saved in the same folder on GitHub where this ReadMe file exists. The March 5 version with distances appended is the primary unified TPS survey results from all operators. Demographic data on race and gender (from the ACS) and income and vehicle ownership (from PUMS) are also in individual csv's in this folder. They are combined with the larger set in Tableau via a union in the workbook. Operator is set to "Bay Area Income Totals" (or any other demographc category), the corresponding column has the appropriate attributes (in this case, income categories), and the Weight column has corresponding weighted counts for each category. Note that these totals are reported at the person-level, not household. 

## Release checklist for `Snapshot_Survey_Dashboard.twb`
1. Check that both widgets work across multiple dashboards.  
2. Set them to their defaults (no operator, weekday).  
3. Make sure no category is selected on any of the pages (as this causes other categories to appear greyed out)  
4. Create data extract (if not already done, as Tableau Public requires it), and confirm that the extract doesn't include data filters.  
5. Commit the Tableau workbook to GitHub.  
6. Publish to Tableau Public.  
