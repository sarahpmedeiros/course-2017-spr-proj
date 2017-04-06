README

Questions:
1. See if the datasets we created in project 1 are related or not. Build a scoring system.
2. Rank airbnb housings in Boston area, according to a criteria we designed


In project 2 we first made some adjustments to datasets that we generated in project1.
We basically designed a scoring system for Airbnbs in Boston. We evaluate airbnbs in respect of ratings given by customers(obtain by calculating overall scores, cleanliness scores, noisy levels, accuracies), transportation convenience, surrounding entertainment. We also created a restaurant scoring system. We obtain restaurant number and quality by calculating each restaurant's cleanliness level and safety level. In future, we will combine restaurant score and airbnb score to give a more convincing ranking of airbnbs in Boston.
We normalized each field so that we could get a general evaluation. To determine which filed to be used, we also calculated correlation coefficients of each two columns (pairs of columns: MBTA stops number around airbnb and entertainment number around airbnb, MBTA stops number and airbnb ratings, Entertainment number and airbnb ratings, cleanness level of restaurants and crime numbers around the restaurants) of the airbnb, to check if the features of airbnb are in positive relation, if they are, they are similar traits to the scoring system of airbnb, so we can eliminate one of them to only have to use one to represent two features.