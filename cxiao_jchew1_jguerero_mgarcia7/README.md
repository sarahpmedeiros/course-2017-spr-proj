# Project narrative

Our project seeks to explore the relationship between food accessibility, obesity and income in Boston's neighborhoods. We believe that we will find that a smaller income and a lack of close food sources in a neighborhood will correlate to a higher obesity percentage. In order to do this, we created a method to score each neighborhood based on how their accessible food sources are, and then we used this information to determine a correlation.
# Dataset descriptions

1. **Obesity Statistics in Massachusetts**: This dataset contains properties such as population over 20 and population with a BMI of 30 or higher in a certain area defined by geographic coordinates. (obesitystats.py)
2. **Boston Income information per neighborhood**: This dataset contains information concerning the median household income per neighborhood. (population.py)
3. **Farmers' Markets**: This dataset contains information about Farmers' Markets in Boston. (farmersmarkets.py)
4. **Corner Markets**: This dataset contains information about Corner Markets in Boston. (cornermarkets.py)
5. **Supermarkets**: This dataset contains information about the supermarkets in Boston. (supermarkets.py)
6. **Boston Neighborhood Shapefiles**: This dataset contains the geographic coordinates of the neighborhoods in Boston. (neighborhood.py)
7. **Master Address List**: [INSERT TEXT HERE] 

# Transformations

First, we combined the Farmers' Markets, Corner Markets, and Supermarkets datasets in order to create an overall Food Source dataset. In order to do this we needed to standardize the attribute names, as well as get rid of attributes that we deemed as unnecessary. Since not all of the datasets included latitude and longitude information, we used Google Maps Geocoding API in order to retrieve that information based on the address of the food source. (foodsources.py)

Second, we had to map the regions in the Obesity Statistics in Massachusetts to neighborhoods in Boston in order to calculate the percentage of people who had a BMI >= 30 per neighborhood. (obesityperneighborhood.py)

Third, we created a new dataset that combines information from all of the datasets to look at the relationships between information that will help us answer our overall project narrative. This new dataset includes the average income, total number of food sources and average obesity based on the neighborhood. (neighborhoodstatistics.py)

# Algorithms

### Calculating a Food Accessbility Score For Each Neighborhood: ###
[INSERT TEXT HERE]

### Determining Correlation: ###
[INSERT TEXT HERE]


# Required libraries and tools
The libraries pyshp and shapely will need to be installed before the program can be executed. They can be installed with these pip commands:
@EVERYONE: Make sure to include the libraries here you used that don't come packaged with Python
```
pip install pyshp
pip install shapely
```
