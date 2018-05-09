
## Onboard-Surveys

Transit passenger data collection and analysis. This document summarizes the data collected in the transit passenger survey.


## Mandatory questions in every operator survey

[Geocoded location data](#geocoded-location-data)  
[Access and egress modes](#access-and-egress-modes)  
[Transit transfers (intra- and inter-operator transfers)](#transit-transfers)  
[Origin and destination trip purposes](#origin-and-destination-trip-purposes)  
[Time leaving and returning home](#time-leaving-and-returning-home)  
[Fare payment](#fare-payment)  
[Half-tour questions for work and school](#half-tour-questions-for-work-and-school)  
[Person demographics](#person-demographics)  
[Household demographics](#household-demographics)  
[Data items entered by surveyor or passively collected](#data-items-entered-by-surveyor-or-passively-collected)  
 


### Geocoded location data (x,y format)

1. Trip origin  
2. First transit boarding  
3. Last transit alighting  
4. Trip destination  
5. Bus/rail transfer location points (if transfers exist)  
6. Home location  
7. Work location (if appropriate)  
8. School location (if appropriate)  


### Access and egress modes

[Access mode from trip origin to first transit boarding](access.md)  
[Egress mode from last transit alighting to trip destination](egress.md)  

### Transit transfers  
Up to three intra- and inter-operator transfers to surveyed vehicle for this trip  
Up to three intra- and inter-operator transfers from surveyed vehicle for this trip  


### Origin and destination trip purposes
Origin type  
Destination type  


### Time leaving and returning home
Time leaving home prior to this trip, by hour  
Time returning home after this trip, by hour  


### Fare payment
Payment method  
Payment category  

### Half tour questions for work and school
Did/will passenger go to work before/after transit trip (see [logic](./work_half-tour.md))    
Did/will passenger go to school before/after transit trip (see [logic](./school_half-tour.md))  


### Person demographics

Hispanic/Latino status  
Race  
Age  
Gender (may be observed)  
Worker status  
Student status  
Ability to speak English  


### Household demographics

Language spoken at home other than English  
Number of persons in household  
Number of workers in household  
Number of household working vehicles  
Household income  

### Data items entered by surveyor or passively collected

Survey type (tablet, two-step CATI, or paper)  
Language survey conducted in  
Date survey collected   
Time survey collected   
Vehicle boarding time (may be continuous time or by day-part)  
Overall survey completion percentage  

 

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


