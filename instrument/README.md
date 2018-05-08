
## Onboard-Surveys

Transit passenger data collection and analysis. This document summarizes the data collected in the transit passenger survey.


## Mandatory questions in every operator survey

[Geocoded location data](#geocoded-location-data)
[Access/egress modes](#Access)
[Transit transfers (intra- and inter-operator transfers)](#Transfers)
[Origin/destination trip purposes](#Origin)
[Time leaving and returning home](#Leaving)
[Fare payment](#Fare)
[Half-tour questions for work and school](#Half-Tour)
[Person demographics](#Demographics)
[Household demographics](#Demographics-1) 
[Survey type](#Type)
[Language survey conducted in (entered by surveyor)](#Language)
[Date survey collected (collected passively)](#Date)
[Time survey collected (collected passively)](#Time)
[Vehicle boarding time (may be continuous time or by day-part)](#Vehicle)
[Overall survey completion percentage](#Completion) 
 


### Geocoded location data

Trip origin
First transit boarding 
Last transit alighting
Trip destination
Bus/rail transfer location points (if transfers exist)
Home location
Work location (if appropriate)
School location (if appropriate)


### Access/egress modes

[Access mode from trip origin to first transit boarding](access.md)
[Egress mode from last transit alighting to trip destination](egress.md)

###Transit transfers (intra- and inter-operator transfers)
Up to three transfers to surveyed vehicle for this trip
Up to three transfers from surveyed vehicle for this trip



###Origin/destination trip purposes
Origin type
Destination type



###Time leaving and returning home
Time leaving home prior to this trip, by hour
Time returning home after this trip, by hour


###Fare payment
Payment method
Payment category

###Half-tour questions for work and school
Did/will passenger go to work before/after transit trip (see logic)
Did/will passenger go to school before/after transit trip (see logic)


###Person demographics

Hispanic/Latino status
Race
Age
Gender (may be observed)
Worker status
Student status
Ability to speak English


###Household demographics

Language spoken at home other than English
Number of persons in household
Number of workers in household
Number of household working vehicles
Household income

###Survey type

Tablet
Computer-assisted telephone interview (CATI)
Paper




 

A dyno-path assignment MUST include the following files:

Filename 			| Description										
----------			| -------------										
[`location`](/files/location.md)		| passenger paths that were chosen
[`chosen_links.csv`](/files/links.md)		| links for chosen paths

A dyno-path assignment MAY include the following files:

Filename 					| Description										
----------					| -------------		
[`pathset_paths.csv`](/files/paths.md)		| path-based information for pathsets
[`pathset_links.csv`](/files/links.md)		| link-based information for pathsets


