
## Onboard-Surveys

Transit passenger data collection and analysis. This document summarizes the data collected in the transit passenger survey. A [data dictionary](data_dictionary.md) of survey variables found in MTC's public release data can be found here. An [example equivalency file](variable_dictionary.md) that converts vendor survey records to MTC's standard output variables can also be found here.


## Mandatory questions in every operator survey

1.  [Geocoded location data](#geocoded-location-data)  
2.  [Access and egress modes](#access-and-egress-modes)  
3.  [Transit transfers (intra- and inter-operator transfers)](#transit-transfers)  
4.  [Origin and destination trip purposes](#origin-and-destination-trip-purposes)  
5.  [Time leaving and returning home](#time-leaving-and-returning-home)  
6.  [Fare payment](#fare-payment)  
7.  [Half-tour questions for work and school](#half-tour-questions-for-work-and-school)  
8.  [Person demographics](#person-demographics)  
9.  [Household demographics](#household-demographics)  
10. [Data items entered by surveyor or passively collected](#data-items-entered-by-surveyor-or-passively-collected)  
 


### Geocoded location data (x,y format)
```
1. Trip origin  
2. First transit boarding  
3. Last transit alighting  
4. Trip destination  
5. Bus/rail transfer location points (if transfers exist)  
6. Home location  
7. Work location (if appropriate)  
8. School location (if appropriate)  
```

### Access and egress modes

[1. Access mode from trip origin to first transit boarding](access.md)  
[2. Egress mode from last transit alighting to trip destination](egress.md)  

### Transit transfers
```
1. Up to three intra- and inter-operator transfers to surveyed vehicle for this trip  
2. Up to three intra- and inter-operator transfers from surveyed vehicle for this trip  
```

### Origin and destination trip purposes

[1. Origin type](origin.md)  
[2. Destination type](destination.md)  


### Fare payment
[1. Payment method](fare.md/#payment-method)  
[2. Payment category](fare.md/#payment-category)  

### Half tour questions for work and school
[1. Did/will passenger go to work before/after transit trip](work_half-tour.md)   
[2. Did/will passenger go to school before/after transit trip](school_half-tour.md)  


### Person demographics

[1. Hispanic/Latino status](person.md)    
[2. Race](person.md/#race)    
[3. Age](person.md/#age)    
[4. Gender (may be observed)](person.md/#gender)    
[5. Worker status](person.md/#worker-status)  
[5. Days Commute to Non-Home Office](person.md/#commute)    
[7. Student status](person.md/#student-status)    
[8. Ability to speak English](person.md/#ability-to-speak-english)    


### Household demographics

[1. Language spoken at home other than English](household.md/#language-spoken-at-home)  
[2. Number of persons in household](household.md/#number-of-persons-in-household)    
[3. Number of workers in household](household.md/#number-of-workers-in-household)    
[4. Number of household working vehicles](household.md/#number-of-household-working-vehicles)    
[5. Household income](household.md/#household-income)    

### Data items entered by surveyor or passively collected
```
1. Survey type (tablet, two-step CATI, or paper)  
2. Language survey conducted in  
3. Date survey collected   
4. Time survey collected   
5. Vehicle boarding time (may be continuous time or by day-part)  
6. Overall survey completion percentage  
```
 

