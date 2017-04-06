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

### Calculating a Food Accessibility Score For Each Neighborhood: ###
In order to calculate a food accessibility score for each neighborhood we looked at three important factors. 
* The average number of food sources in the neighboorhood. 
* The average distance to the closest food source.
* The average quality of the food source (based on how much of each type of food source there is)
These three items have a huge impact on what it means to have a "good" food accessibility food score. With neighborhoods being different sizes, the total number of food sources differs. Our main focus for the first metric was to get the average number of food sources that are within walking distance in the specified neighborhood. This is found by calculating the total number of food sources that are within walking distance per house and computing the mean. 

Next, we looked at what the average distance was to the closest food source per neighborhood. This was found by taking a list of closest food source distance per house for the specified neighborhood and calculating the mean. 

The last metric we look at, this one taking a bit more computation, was the average quality of food sources in the neighborhood. There were a few options that we considered when deciding how this number was going to be calculated. The one that seemed to make the most sense was to rank each type of food source and give it a 'weight'. So, a farmer's market had a weight of 1, a supermarket had a weight of 2/3, and a cornerstore had a weight of 1/3. Out of the total number of food sources in the neighborhood, we found how many of 




| Neighborhood                   |   Avg # of Food Sources* |   Avg Distance To Closest Food Source (km) |  Avg Quality of Food Sources* |   Composite Z-score |
|:-------------------------------|-------------------------:|-------------------------------------------:|---------------------------:|--------------------:|
| Bay Village                    |                     3.00 |                                       0.21 |                       0.33 |                2.72 |
| South End                      |                    13.60 |                                       0.17 |                       0.46 |               -1.80 |
| Roxbury                        |                     8.12 |                                       0.31 |                       0.44 |               -0.01 |
| Allston/Brighton               |                     8.67 |                                       0.26 |                       0.41 |                0.21 |
| Mattapan                       |                     5.76 |                                       0.39 |                       0.52 |               -0.41 |
| Roslindale                     |                     4.18 |                                       0.40 |                       0.48 |                0.52 |
| Financial District/Downtown    |                    11.29 |                                       0.19 |                       0.39 |               -0.32 |
| West End                       |                     7.00 |                                       0.26 |                       0.38 |                1.00 |
| North End                      |                     8.74 |                                       0.10 |                       0.41 |                0.18 |
| Back Bay                       |                     8.88 |                                       0.23 |                       0.53 |               -1.41 |
| East Boston                    |                    14.15 |                                       0.17 |                       0.37 |               -0.86 |
| South Boston                   |                     7.02 |                                       0.27 |                       0.42 |                0.55 |
| Mission Hill                   |                     3.69 |                                       0.33 |                       0.58 |               -0.55 |
| Jamaica Plain                  |                     5.12 |                                       0.40 |                       0.53 |               -0.34 |
| Government Center/Faneuil Hall |                     4.00 |                                       0.19 |                       0.67 |               -1.66 |
| Beacon Hill                    |                     3.00 |                                       0.23 |                       0.33 |                2.72 |
| Dorchester                     |                    11.19 |                                       0.29 |                       0.44 |               -0.95 |
| Fenway/Kenmore                 |                    11.22 |                                       0.15 |                       0.49 |               -1.55 |
| West Roxbury                   |                     1.92 |                                       0.71 |                       0.52 |                0.68 |
| Hyde Park                      |                     3.79 |                                       0.44 |                       0.45 |                1.03 |
| Charlestown                    |                     6.25 |                                       0.32 |                       0.46 |                0.26 |

\* *Within walking distance of a residence, which we defined to be < 1km*

### Determining Correlation: ###
[INSERT TEXT HERE]


# Required libraries and tools
The libraries pyshp, shapely, geopy, numpy will need to be installed before the program can be executed. They can be installed with these pip3 commands:
@EVERYONE: Make sure to include the libraries here you used that don't come packaged with Python
```
pip3 install pyshp
pip3 install shapely
pip3 install geopy
pip3 install numpy
```
