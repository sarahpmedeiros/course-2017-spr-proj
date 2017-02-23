# Project #1: Data Retrieval, Storage, Provenance, and Transformations
Team: Lingyi Gu, Wei Wei

## Introduction
Browsing through [Boston Bikes](https://www.boston.gov/departments/boston-bikes)'s website,
we find out that the City of Boston has launched several projects and program to establish a bicycle network in Boston.
For example, The [Connect Historic Boston](https://www.boston.gov/transportation/connect-historic-boston) Project wants to connect historic sites in downtown Boston. 
And the [Youth Cycling Problem](https://www.boston.gov/departments/boston-bikes/youth-cycling-program) encourages students to ride by bringing biking curriculum to schools.

In this project, we're interested in finding the optimized locations to place bike racks of ride sharing stations.
We found a data set that lists the locations and properties of existing Hubway Stations (a bike-sharing program) around Boston Area.
We also obtained data sets of Boston landmark locations and public school locations.
By examining the relationship between existing Hubway stations and bike networks,
we can determine if there's more Hubway stations near the existing bicycle lanes.
By analyzing the historic landmark locations and school locations, we can determine how to allocate resources
and provide suggestions for possible locations of more share-riding stations.

## Data Retrieval
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
This data set has a record of the city of Boston's and Cambridge's Bike Networks (street name, length and geographical information).
We've found out that the Hubway Stations has bike-sharing locations in both Boston and Cambridge,
thus we decide to merge these two data sets.
Since the two data sets have different schemas,
we first do a ```projection``` on both data sets to clean them up then we merge them using ```union```.

#### Landmarks and Hubway Stations
This data set has a record of Boston's historic landmarks and their distance to the nearest Hubway Station. We use the ```product``` operation to combine two sets and ```project``` on the resulting set.
Then we ```aggregate``` for each historic landmark, find the closest Hubway Station and get the distance.
The function is not implemented efficiently so that the aggregation process might takes 20 seconds or more.

#### Schools and Hubway Stations
This data set has a record of Boston's public schools and the number of Hubway Stations nearby.
First, we clean up the MA Schools data set by eliminating the schools outside Boston and the schools only offering lower grades (Pre-K to 2).
Then we use the ```product``` function to combine two data sets. Then we ```aggregate``` on Schools.
For each school, count the number of Hubway Stations within a 5-minute walking distance (we use 500 meters here).


## Setting Up
Follows the procedure in [Data-Mechanics/course-2017-spr-proj](https://github.com/Data-Mechanics/course-2017-spr-proj).

###### Required libraries
```
$ pip install prov --upgrade --no-cache-dir
$ pip install dml --upgrade --no-cache-dir
$ pip install protoql --upgrade --no-cache-dir
$ pip install geopy
```
###### Run
```
$ python execute.py echogu_wei0496
```
