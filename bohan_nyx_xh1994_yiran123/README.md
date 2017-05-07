README

Running time: About 30-40 minutes.
From project1: transformation1-3
From project2: transformation4(correlation coefficient), transformation5(restaurant scoring system), transformation6(calculating avg restaurant scores around each airbnb), transformation7(airbnb scoring system)

Questions:
1. See if the datasets we created in project 1 are related using correlation coefficient.
2. Build a scoring system. Rank airbnb housings in Boston area, according to a criteria we designed.

In project 2 we first made some adjustments to datasets that we generated in project1. We use MBTA bus stops instead of traffic signals to more precisely describe transportations around each airbnb. 

We basically designed a scoring system for Airbnbs in Boston. We evaluate airbnbs in respect of ratings given by customers(obtain by calculating overall scores, cleanliness scores, noisy levels, accuracies), transportation convenience, surrounding entertainment, surrounding restaurant number and quality. We also created a restaurant scoring standard and obtained restaurant number & quality by calculating each restaurant's cleanliness level and safety level. We combined the two scoring system standards into one dataset to evaluate airbnbs.  

We normalized each field so that we could get a general evaluation. To determine which field to be used, we also calculated correlation coefficients of MBTA stops number around airbnb && entertainment number around airbnb and cleanness level of restaurants && crime numbers around the restaurants. We checked if the features of airbnb are in positive relation, if they are, they are similar traits to the scoring system of airbnb, so we can eliminate one of them to only have to use one to represent two features.