# Project narrative

Our project seeks to explore the relationship between food accessibility, obesity and income in Boston's neighborhoods. We believe that we will find that a smaller income and a lack of close food sources in a neighborhood will correlate to a higher obesity percentage. Later, we hope to more closely look at the accessibility of these food sources by train, car or walking distance to better understand the implications this has on obesity. 

# Dataset descriptions

1. **Obesity Statistics in Massachusetts**: This dataset contains properties such as population over 20 and population with a BMI of 30 or higher in a certain area defined by geographic coordinates.
2. **Boston Demographic information per neighborhood**: This dataset contains information such as the population of a neighborhood, the median household income in 2015 per neighborhood, and the median rent in 2015 per neighborhood. 
3. **Farmers' Markets**: This dataset contains information about Farmers' Markets in Boston.
4. **Corner Markets**: This dataset contains information about Corner Markets in Boston.
5. **Supermarkets**: This dataset contains information about the supermarkets in Boston.
6. **Boston Neighborhood Shapefiles**: This dataset contains the geographic coordinates of the neighborhoods in Boston.

# Transformations

First, we combined the Farmers' Markets, Corner Markets, and Supermarkets datasets in order to create an overall Food Source dataset. In order to do this we needed to standardize the attribute names, as well as get rid of attributes that we deemed as unnecessary.

Second, we had to map the regions in the Obesity Statistics in Massachusetts to neighborhoods in Boston in order to calculate the percentage of people who had a BMI >= 30 per neighborhood. Note: converting from shape file to JSON takes more than a few minutes. 

Third, we created a new dataset that combines information from all of the datasets to look at the relationships between information that will help us answer our overall project narrative. This new dataset includes the population size, average income, total number of food sources and average obesity based on the neighborhood. 

# Required libraries and tools
The libraries pyshp and shapely will need to be installed before the program can be executed. They can be installed with these pip commands:
```
pip install pyshp
pip install shapely
```
