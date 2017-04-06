# BU CS591L1 Spring 2017 Project 1

**Team Members:**
+ Yizhi Huang (billy108@bu.edu)
+ Yue Zhou (zhouy13@bu.edu)

##Narrative
<ol>
In this project, we aim to combine all data sets about aerobic and recreational places in Boston such as swimming pools , public parks and water play park, so that we could use the resulting new datasets for later developments of the Project 2 and to see how sportive Boston's neighborhoods are by counting how many recreational places there are in each neighborhood. By displaying these information, people could have one more perspective to view a neighborhood and decide whether they want to live in such neighborhood. Moreover, government officers could also reflect from these information to see whether a neighborhood need a improvement on the provision of recreational places. 
Since some open datasets we chose from online resources have no zipcode as an attribute and the coordinates are messy, we determined to attribute a specific park or pool to a neighborhood rather than a zipcode.
</ol>

##6 Datasets from 3 different portals
<ol>
<li>'seasonalSwimPools':'https://data.cityofboston.gov/resource/xw3e-c7pz.json'</li>
<li>'communityGardens':'https://data.cityofboston.gov/resource/rdqf-ter7.json'</li>
<li>'openSpaceCambridge':'https://data.cambridgema.gov/api/views/5ctr-ccas/rows.json?accessType=DOWNLOAD'</li>
<li>'waterplayCambridge':'https://data.cambridgema.gov/api/views/hv2t-vv6d/rows.json?accessType=DOWNLOAD'</li>
<li>'openSpaceBoston':'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'</li>
<li>'commCenterPools':'http://bostonopendata-boston.opendata.arcgis.com/datasets/5575f763dbb64effa36acd67085ef3a8_0.geojson'</li>
</ol>

##Transformation Processes
<ol>
<li>Retreive all data from the 6 open datasets and inserted them into 6 corresponding tables in mongoDB.</li>
<li>Do projection to each of the two datasets-'seasonalSwimPools' and 'commCenterPools', so that we get only the pool name and the corresponding neighborhood. And combine the two outcomes into one dataset that contains info of all pools in Boston.</li>
<li>Do projection to each of the three datasets-'openSpaceCambridge', 'openSpaceBoston' and 'communityGardens', so that we get only the place name and the corresponding neighborhood. And combine the three outcomes into one dataset that contains info of all open paces in Boston.</li>
<li>Do projection to the last open dataset 'waterplayCambridge' to get only the water play park name and the corresponding neighborhood. And combine the outcome with previous two output datasets from the above operations into one dataset that contains info of all recreational and aerobic locations in Boston.</li>
</ol>

##3 New Resulting Datasets
<ol>
<li>'allPoolsInBoston' - output of combineAllSwimmingPools.py</li>
<li>'allOpenSpacesInBoston' - output of combineAllOpenSpaces.py</li>
<li>'allRecreationalPlaces'- output of combineAll.py</li>
</ol>

##Code Executing Instructions:
To run to entire program:
```
python3 execute.py billy108_zhouy13_jw0208
```

**Project 2 Updates:**

**Team Members:**
+ Yizhi Huang (billy108@bu.edu)
+ Yue Zhou (zhouy13@bu.edu)
+ Wei Ji (jw0208@bu.edu)

##New Datasets
<ol>
<li>'hubwayStations':'http://bostonopendata-boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson'</li>
<li>'buildingPermits':'https://data.cityofboston.gov/Permitting/Approved-Building-Permits/msk6-43c6/data'</li>
</ol>


##Narrative
<ol>
We are trying to find out how sportive neighborhoods in Boston are and to see how quantities of different public recreational places or facilities are correlated.
+ Optimization: We could not find data about centroid coordinates of neighborhoods in Boston, therefore we calculated an estimation by ourselves. We retrieve new data about all building permits in Boston, and then we use K-means algorithm to estimate centroid coordinate for each neighborhood.
+ When we get all neighborhoods' centroids, we incorporate new recreational facility data about hubway(bicycle) stations in Boston. Since the hubway station data include coordinates, we map each hubway station to its closest neighborhood centroid.
+ We sum up the number of hubway stations, open spaces and swimming pools for each neighborhood.
+ Statistic Analysis: Using the stats collected above, we calculate correlation coefficient and p-value b/w number of open spaces and swimming pools, b/w number of open spaces and hubway stations and b/w number of swimming pools and hubway stations.
+ trial modes in mapHubwaysToClosestNeigh.py and estimateNeighCentroid.py
</ol>

