# Using Neighborhood Population Data and Sea Level Information to Determine Safe Points in Different Parts of Boston and Cambridge

### Purpose / Narrative

Climate change remains one of the defining issues of the 21st century.  With sea levels rising around the world, coastal cities must be prepared to tackle new challenges. 

Boston is no exception. In late 2016, Boston experienced what is known as a 'king tide' - the highest point that the natural tide gets to during the course of the year. The tide in that particular year was surprisingly high; water came over the wharf at almost two feet higher than the yearly average. As sea levels around the world continue to rise, it becomes increasingly important to understand where potential flooding can become a problem and act accordingly ahead of time. In this project, I collect neighborhood census data and pair it with neighborhood geospatial polygon data, then test where that intersects geospatial data of estimated regions of coastal flooding at different sea levels. From this information, a severity index can be applied.

Our project aims to help city planners prepare for this scenario by identifying new locations outside the affected floodzone to build/move essential utilities such as electric stations or water pumps. We attempted to find locations that were close to large population centers so that they can service the greatest amount of people. By proactively and preemptively building in spots less effected by the increase in water levels, we can lessen the effect that climate change has on the city of Boston.

The project's file structure consists of three files that fetch the intitial data to be placed in the MongoDB collections, and five files that represent the transformations on these data sets to form something more useful to the purpose above.
  
### APIs Used
The following APIs were applied for collecting data:

http://bostonopendata-boston.opendata.arcgis.com/ (Greater Boston-Area Sea Level Projections)

https://data.cityofboston.gov/ (Boston Neighborhood Polygon Data)

https://data.cambridgema.gov/ (Cambridge Neighborhood Polygon Data & Census Information)

http://datamechanics.io/ (Boston Neighborhood Census Information [collected from various online sources])

### Project Dependencies
This project requires no explicit API keys in order to collect information as most requests are simple JSON static content requests. Using a Socrata API token can help with throttling issues, but these are negligible.

There are two third-party Python library dependencies, the first of which is Shapely. This library is used to compare geometric information, which I use to check intersecting geospatial polygons in the context of this project. Shapely can be installed through pip, though it may have one additional dependency on 'libgeos' - both of these are easily accessible through most OS package managers.

https://pypi.python.org/pypi/Shapely

The second dependency is NumPy, which is a package designed to assist in scientific computations. It offers efficient and convenient functions and data structures for manipulating large amounts of data. We use NumPy for running a weighted probability function needed in our K-Means++ function to select initial seeds for K-Means.

http://www.numpy.org/

### Transformation #1 (Neighborhood Synchronization)
The first transformation consists of a simple unionization of the two neighborhood census data sets into one set. This requires a few projections in the last step, as the Boston census data does not explicitly match the Cambridge census data. We then use this set in applying the second transformation.

### Transformation #2 (Neighborhood Polygon and Centerpoint Data)
In the second transformation, we compare the master list of neighborhoods from the first transformation against the polygon data in both the Boston and Cambridge geospatial information; this allows us to correlate a set of coordinates to a neighborhood name and population. In addition, I add the calculated centerpoint of the neighborhood into the dataset, as such a value could come in handy later if applying a k-means sort of algorithm proves useful.

### Transformation #3 (Calculating Flood Ratios for 5 and 7.5 Feet Rise)
Finally, we use the Shapely python library to check for intersections between the neighborhood polygons and the sea level information. By comparing the intersection area to the size of the neighborhood, I can determine a ratio of flooded space and estimate how much of the population is affected. From there a severity index can be calculated for that neighborhood, which could later on be used in calculating a weighted k-means clustering algorithm which is biased towards area with large impact on humans. Note that this transformation could take a while (since it applies an expensive operation), but should take no longer than 5 minutes on most processors.

### Transformation #4 (K-Means)
In the first transformation, we run K-Means on the neighborhoods, using the centerpoints of each neighborhood. This ensures that all the neighborhoods have a location that is relatively close. We use a seeding algorithm for K-Means known as K-Means++, which uses a weighted random walk over the original neighborhood centerpoints to find ideal seeds for K-Means.

### Transformation #5 (Calculating Safe Point)
In the second transformation, we take the locations from the first transformation and attempt to find safe points outside of the floodzone. We iterate over the means from the first transformation, the proposed safe points. Because we converted the floodzone into a multi-polygon, we check the proposed safe points by comparing each of the points to each of the polygons that make up the floodzone. If the mean is already safe, i.e not in the flood zone, we do nothing. If it is in the floodzone, we perform a calculation to find the closet point from that mean to the safe zone. 




