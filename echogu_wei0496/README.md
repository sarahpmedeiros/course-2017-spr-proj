# Project #1: Data Retrieval, Storage, Provenance, and Transformations
Team: Lingyi Gu, Wei Wei

## Introduction
Browsing through [Boston Bikes](https://www.boston.gov/departments/boston-bikes)'s website,
we find out that the City of Boston has launched several projects and program to establish a bicycle network in Boston.
For example, The [Connect Historic Boston](https://www.boston.gov/transportation/connect-historic-boston) Project wants to connect historic sites in downtown Boston. 
And the [Youth Cycling Problem](https://www.boston.gov/departments/boston-bikes/youth-cycling-program) encourages students to ride by bringing biking curriculum to schools.

We're interested in finding the optimized locations to place bike racks of ride sharing stations to help with these programs.

## Data Sets
##### Boston Existing Bike Networks
Existing Biking Networks within the city of Boston (source: Boston Open Data)

##### Cambridge Bike Facilities
Biking facilities and network in Cambridge (source: Cambridge Open Data)

##### Hubway Stations
Station location data from Hubway, a bike-sharing program in Boston (source: Boston Open Data)

##### 311 Services Request
Open Services Request for city services, contains a "Boston Bikes" category (source: City of Boston Data Portal)

##### BLC Landmarks
Boston Landmark Commission Landmarks (source: Boston Open Data)

##### MA Schools
Public Schools in Massachusetts from Pre-K to 12th grade (source: MassGIS Open Data)

## Transformations and New Data Sets
#### Bike Network
we've found out that the Hubway Stations dataset include the locations of cambridge, thus we

#### Landmarks and Hubway Stations

#### Schools and Hubway Stations




Write a short narrative and justification (5-10 sentences) explaining how these data sets can be combined to answer an interesting question or solve a problem. You do not need to solve the actual problem in this project, and it is acceptable to merely combine data sets in a way that is likely to support one or more solutions involving the particular data you choose.
Include this narrative in a README.md file within your directory (along with any documentation you may write in that file).



## Setting Up
Follows the procedure in [Data-Mechanics/course-2017-spr-proj](https://github.com/Data-Mechanics/course-2017-spr-proj).

Required libraries:
```
pip3 install prov --upgrade --no-cache-dir
pip3 install dml --upgrade --no-cache-dir
pip3 install protoql --upgrade --no-cache-dir
pip3 install geopy
```