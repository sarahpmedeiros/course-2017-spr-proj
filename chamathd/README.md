# Using Neighborhood Population Data and Sea Level Information to Determine Flooding Severity in Different Parts of Boston

### Purpose / Narrative
  In late 2016, Boston experienced what is known as a 'king tide' - the highest point that the natural tide gets to during the course of the year. The tide in that particular year was surprisingly high; water came over the wharf at almost two feet higher than the yearly average. As sea levels around the world continue to rise, it becomes increasingly important to understand where potential flooding can become a problem and act accordingly ahead of time. In this project, I collect neighborhood census data and pair it with neighborhood geospatial polygon data, then test where that intersects geospatial data of estimated regions of coastal flooding at different sea levels. From this information, a severity index can be applied.
  
  The project's file structure consists of three files that fetch the intitial data to be placed in the MongoDB collections, and three files that represent the transformations on these data sets to form something more useful to the purpose above.
  
### APIs Used
The following APIs were applied for collecting data:

http://bostonopendata-boston.opendata.arcgis.com/ (Greater Boston-Area Sea Level Projections)
https://data.cityofboston.gov/ (Boston Neighborhood Polygon Data)
https://data.cambridgema.gov/ (Cambridge Neighborhood Polygon Data & Census Information)
http://datamechanics.io/ (Boston Neighborhood Census Information [collected from various online sources])

### Project Dependencies
This project requires no explicit API keys in order to collect information as most requests are simple JSON static content requests. Using a Socrata API token can help with throttling issues, but these are negligible.

There is one third-party Python library dependency, which is Shapely. This library is used to compare geometric information, which I use to check intersecting geospatial polygons in the context of this project. Shapely can be installed through pip, though it may have one additional dependency on 'libgeos' - both of these are easily accessible through most OS package managers.

https://pypi.python.org/pypi/Shapely

### Transformation #1 (Neighborhood Synchronization)
The first transformation consists of a simple unionization of the two neighborhood census data sets into one set. This requires a few projections in the last step, as the Boston census data does not explicitly match the Cambridge census data. We then use this set in applying the second transformation.

### Transformation #2 (Neighborhood Polygon and Centerpoint Data)
In the second transformation, we compare the master list of neighborhoods from the first transformation against the polygon data in both the Boston and Cambridge geospatial information; this allows us to correlate a set of coordinates to a neighborhood name and population. In addition, I add the calculated centerpoint of the neighborhood into the dataset, as such a value could come in handy later if applying a k-means sort of algorithm proves useful.

### Transformation #3 (Calculating Flood Ratios for 5 and 7.5 Feet Rise)
Finally, we use the Shapely python library to check for intersections between the neighborhood polygons and the sea level information. By comparing the intersection area to the size of the neighborhood, I can determine a ratio of flooded space and estimate how much of the population is affected. From there a severity index can be calculated for that neighborhood, which could later on be used in calculating a weighted k-means clustering algorithm which is biased towards area with large impact on humans. Note that this transformation could take a while (since it applies an expensive operation), but should take no longer than 5 minutes on most processors.
