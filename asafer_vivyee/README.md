# Narrative (Project 1)
Using the data sets that we chose, we want to map the location of adult obesity and how that correlates to easy access to nutrition programs, healthy stores, etc. We will create a transportation map using the MBTA routes and stops data, which we combine. We will combine data on health related programs, stores and orchards to find areas with access to healthy goods and information. We will then use all of these datasets to find whether these programs and shops are located in areas that need it most (where obesity is dominant).

# Transformations Performed
## Healthy Locations
We aggregated all of the health programs, stores and orchards into one dataset, with its type and location. We used select and project to clean and reformat the datasets.

## Closest MBTA stops to Obesity Locations
We calculated the closest stops to each obesity area. We defined close as equal or less than a mile away. We used product to map all possible stop and obese combinations and calculated the distance between both using project. Then we used select to filter out all the far away stops. We then aggregated each obesity area together to form a list of stops for each area.

## Closest MBTA stops to Healthy Locations
We calculated the closest stops to each healthy location (calculated in the first transformation). We defined close as equal or less than 2 miles because there were no locations within a mile away from an MBTA stop. We used product again to map all possible stop and healthy location combinations and calculated the distance between the two using project. We then used select to filter out far away stops and aggregated like the previous transformation.