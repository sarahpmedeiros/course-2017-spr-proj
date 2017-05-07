# Summary & Problem
We are using datasets of different healthy locations and a dataset of locations with high adult obesity. To get a dataset of healthy locations, we're combining several different datasets such as urban orchards, nutrition programs, and healthy corner stores. We'll be looking to see if there is any correlation between access to healthy locations and obesity. We will then use these information to determine whether these programs and shops are located in areas with higher levels of obesity, which need them the most.

# Transformations Performed
## Healthy Locations
We aggregated all of the health programs, stores and orchards into one dataset, with its type and location. We used select and project to clean and reformat the datasets.

## Closest MBTA stops to Obesity Locations
We calculated the closest stops to each obesity area. We defined close as equal or less than a mile away. We used product to map all possible stop and obese combinations and calculated the distance between both using project. Then we used select to filter out all the far away stops. We then aggregated each obesity area together to form a list of stops for each area.

## Closest MBTA stops to Healthy Locations
We calculated the closest stops to each healthy location (calculated in the first transformation). We defined close as equal or less than 2 miles because there were no locations within a mile away from an MBTA stop. We used product again to map all possible stop and healthy location combinations and calculated the distance between the two using project. We then used select to filter out far away stops and aggregated like the previous transformation.

## Closest Health Location to each Obesity Location (NEW)
For each obesity location, we calculated the closest healthy location. We put these in a dataset, with closest mbta data included as well (calculated above).

## Shortest Path
We use Dijkstra's algorithm and the MBTA's dataset to find the shortest path between each obesity location and its closest healthy location. To do this, we create a graph of the MBTA (subway only) using networkx's DiGraph class. 

## Linear Regression & Control Data
We use linear regression to see how strong the correlation between access to healthy locations and obesity is. We are using trash bins as a control to make sure that any correlation we find is specific to the obesity locations. We found that there was a positive corolation with a slope of 16 between obesity and distance from a healthy location. This means that if we decrease the distance a person has to go to get healthy food we can drasticaly decrease the levels of obesity in that area. 

We ran our algorithm on a Control data and found that there was also a corolation between obesity and distance from a trash bin. While this does mean that there may be external factors contributing to the corolations, the correlation between obesity and distance of healthy locations was much stronger. 

According to our data, the mean squared error between obesity and healthy location distance was only 8.98 where as our control data had a mean squared error of 14.15. This means that there is more signaficance to trying to predict obesity based on the distance from a healthy location. Both datasets ended up having similar rates of increase in obesity based on the distance from either trash bins or healthy locations.
