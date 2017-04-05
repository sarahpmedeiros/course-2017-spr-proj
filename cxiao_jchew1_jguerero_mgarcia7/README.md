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

| Neighborhood                |   Avg # of Food Sources* |   Avg Distance To Closest Food Source (miles) |   Quality of Food Sources* |   Composite Z-score |
|:----------------------------|-------------------------:|----------------------------------------------:|---------------------------:|--------------------:|
| Roxbury                     |                     8.23 |                                          0.31 |                       0.44 |               -0.12 |
| Roslindale                  |                     4.66 |                                          0.39 |                       0.48 |                0.20 |
| South Boston                |                     6.57 |                                          0.31 |                       0.42 |                0.65 |
| Beacon Hill                 |                     3.00 |                                          0.22 |                       0.33 |                2.90 |
| Hyde Park                   |                     3.37 |                                          0.46 |                       0.45 |                1.05 |
| Mission Hill                |                     3.86 |                                          0.22 |                       0.58 |               -1.01 |
| North End                   |                     8.71 |                                          0.10 |                       0.41 |                0.17 |
| Bay Village                 |                     3.00 |                                          0.19 |                       0.33 |                2.90 |
| Back Bay                    |                     8.29 |                                          0.27 |                       0.53 |               -1.55 |
| West End                    |                     7.00 |                                          0.28 |                       0.38 |                1.05 |
| Allston/Brighton            |                     8.04 |                                          0.27 |                       0.41 |                0.39 |
| East Boston                 |                    13.96 |                                          0.16 |                       0.37 |               -0.73 |
| Financial District/Downtown |                    11.50 |                                          0.22 |                       0.39 |               -0.35 |
| Jamaica Plain               |                     5.22 |                                          0.37 |                       0.53 |               -0.67 |
| Fenway/Kenmore              |                    11.48 |                                          0.16 |                       0.49 |               -1.83 |
| Dorchester                  |                    11.30 |                                          0.29 |                       0.44 |               -1.08 |
| Mattapan                    |                     5.43 |                                          0.40 |                       0.52 |               -0.59 |
| Charlestown                 |                     6.65 |                                          0.26 |                       0.46 |                0.02 |
| South End                   |                    13.15 |                                          0.17 |                       0.46 |               -1.81 |
| West Roxbury                |                     1.92 |                                          0.69 |                       0.52 |                0.42 |

\* *Within walking distance of a residence*

### Determining Correlation: ###
[INSERT TEXT HERE]


# Required libraries and tools
The libraries pyshp and shapely will need to be installed before the program can be executed. They can be installed with these pip commands:
@EVERYONE: Make sure to include the libraries here you used that don't come packaged with Python
```
pip install pyshp
pip install shapely
```
