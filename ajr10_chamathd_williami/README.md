# Using Neighborhood Population Data and Sea Level Information to Determine Safe Points in Different Parts of Boston and Cambridge

### Purpose / Narrative
Climate change remains one of the defining issues of the 21st century.  With sea levels rising around the world, coastal cities must be prepared to tackle new challenges. They must become climate ready(?).

Boston is no exception. With an increase of just a couple feet, whole neighborhoods could become flooded, with thousands of people being displayed.

Our project aims to help city planners prepare for this scenario, by identifying new locations outside the affected slowzone to build/move essential utilities such as electric stations or water pumps. We attempted to find locations that were close to large population centers so that they can service the greatest amount of people. By proactively and preemptively building in spots less effected by the increase in water levels, we can lessen the effect that climate change has on the city of Boston.
  
### APIs Used
The following APIs were applied for collecting data:

http://bostonopendata-boston.opendata.arcgis.com/ (Greater Boston-Area Sea Level Projections)

https://data.cityofboston.gov/ (Boston Neighborhood Polygon Data)

https://data.cambridgema.gov/ (Cambridge Neighborhood Polygon Data & Census Information)

http://datamechanics.io/ (Boston Neighborhood Census Information [collected from various online sources])

### Project Dependencies
This project requires no explicit API keys in order to collect information as most requests are simple JSON static content requests. Using a Socrata API token can help with throttling issues, but these are negligible.

There is two third-party Python library dependency, the first of which is Shapely. This library is used to compare geometric information, which I use to check intersecting geospatial polygons in the context of this project. Shapely can be installed through pip, though it may have one additional dependency on 'libgeos' - both of these are easily accessible through most OS package managers.

https://pypi.python.org/pypi/Shapely

The second dependency is NumPy, which is a package designed to assist in scientific computations. It offers efficient and convenient functions and data structures for manipulating a large amount of data. We use NumPy to create a weighted probability function needed in our K-Means++ function to select seeds.

http://www.numpy.org/

### Transformation #1 (K-Means)
In the first transformation, we run K-Means on the neighborhoods, using the central x and y locations of each neighborhood. This ensures that all the neighborhoods have a location that is relatively close. We use a variation of K-Means known as K-Means++, which has a method of generating good seeds so that K-Means can create means that better represent the clustering of our neighbor hood information.

### Transformation #2 (Calculating Safe Point)
In the second transformation, we take the locations from the first transformation and attempt to find safe points outside of the floodzone. We iterate over the means from the first transformation, the proposed safe points. Because we converted the floodzone into a multi-polygon, we check the proposed safe points by comparing each of the points to each of the polygons that make up the floodzone. If the mean is already safe, i.e not in the flood zone, we do nothing. If it is in the floodzone, we perform a calculation to find the closet point from that mean to the safe zone. 




