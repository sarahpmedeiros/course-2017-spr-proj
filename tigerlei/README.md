# Project 1

## 2.a

This project focus on the safety of Boston city. All data resources are as below.
 - Two datasets from City of Boston Data Portal  
  - [Crime Incident Reports (July 2012 - August 2015) (Source: Legacy System)](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx)  
  - [Crime Incident Reports (August 2015 - To Date) (Source: New System)](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap) 
 - Two datasets from Boston Open Data 
  - [Boston Police Stations](http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6) 
  - [Colleges and Universities](http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2)
 - Five datasets from Boston Area Research Initiative Dataverse 
  - [Homicides in Boston(2012-2016)](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/1J0IBN). 

I union five years crime incidents datasets as all crime incidents dataset. Combine crime dataset with police station dataset could help me find out which districts of boston got high crime incidents (especially homicides) during past five years. And that reminds corresponding police stations to pay more attention on their areas.

Besides, university students are facing more and more crime incidents in their lifes now. Therefore, I take advantage of university dataset and police station dataset to find police stations which are within 2 kms distance of each university. The nearer the police stations are, the safer students are. For those don't have police stations within in 2kms, they may need spend more guardians on patrol.

Later I am going to cluster crime incidents on the map to find out accurate dangerous areas to help policemen to make our city safer!

## 2.b

The algorithm that retrieve these datasets automatically is in the file: ```retrieve.py```. To run it, uncomment this line first.

```shell
# retrieve.execute()
```
Then run
```python
python3 retrieve.py
```

## 2.c

Three non-trival transformations are as follows. They must be run in the name order. Dependency was written in ```reads[]``` and ```write[]``` parts in the codes.

### Transformation 1

The script is in ```transformation1.py```. This transformation mainly preprocesses all datasets. Porject needed part from different datasets and union crime incidents.  To run it,
```python
python3 transformation1.py
```

### Transformation 2

The script is in ```transformation2.py```. This transformation groups crime incidents & homicides by district ID. Then join with police on district ID to get zipcodes.

![crime_distribution](http://datamechanics.io/data/tigerlei/1.png) ![homicides_distribution](http://datamechanics.io/data/tigerlei/2.png) 

It's quite clear to see that B2/ B3/ C11/ D4 districts got both hight crime incidents and homicides in the past five years. These police stations need to pay more attention on patrol. To run it,
```python
python3 transformation2.py
```

### Transformation 3

The script is in ```transformation3.py```. This transformation first transforms university and police data from json into Geojson format. Then add geosphere index to find all police stations within 2kms for each university. 

![neighborhood of universities and police stations](http://datamechanics.io/data/tigerlei/3.png)

 From the result, most of universities are near police sations. However, a few of them still need to build some mechanics to deal with emergencies crime incidents. To run it,
```python
python3 transformation3.py
```
