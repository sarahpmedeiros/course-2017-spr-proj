# CS591 Project #2 Proposal
#### Sean Zhang & Martin Yim

## The problem
  We are interested in discovering the feasibility of living in Boston in terms of walk time, commute time, and property/assessed value. 

  Let's assume that we have the cost of a property we currently live in, the walking time to get to the nearest public transit station, and the commute time via public transit. Given this information, we would like to find other potential places to live within the same zipcode that we would be indifferent towards, based on those location's respective costs, walking times, and commute times. These conditions are our constraints, and as such, we want to answer the question; does there exist a place(s) in which we are indifferent compared to our current location and its commuting attributes? 

  This kind of constraint satisfaction problem could actually be very applicable in various aspects. For example, say you live in Allston and take the T to commute to work at Kendall Square. Now, city planners have decided to finally renovate the entire block that you live on in Allston. Their claim is that you could move and live in Fenway with a different walk and commute time, but still be indifferent in terms of your overall tradeoffs for the times and costs needed at these two locations. Our script would be able to check and verify this: is this really true? Does there exist a person who would be indifferent towards living one place or another within a certain region?

  Once we found a feasible subset of places that satisfy our constraints, we thought that it would be interesting to see the relationship between various characteristics of these residential areas and the assessed value of the building. Namely, we took the data provided from the Boston Open Data portal, and created a multiple linear regression for various different factors, including number of rooms, number of bedrooms, number of bathrooms, building area, and year the building was built to predict the assessed value of building.

  

## How we did it
    
  From Project #1, we had already gotten data on assessed property data and MBTA location data, but to solve the constraint satisfaction problem, we actually needed to pull data from the Google Maps Places API Web Services. Given this information, we then want to iterate through our other possible residential locations and find the nearest transit stops to a given residential property. We then calculate walking time to the station, commute time to work via the T or bus using common averages. Finally, we find locations with which it is possible to find a set of preferences (weights) to our constraints (walking time, commute time, cost). We then return the weights and zip code region where we could find a new location that a person would be indifferent towards.
  
  With the help of Professor Lapets, our initial hope was to create a system of equations such that 
  
  (w * w_i + t * t_i + r * r_i) == 1 
  
  would be satisfiable for all i within a region. However, because this was such a strict requirement, we decided to loosen the constraints just slightly:
  
  (w * w_i + t * t_i + r * r_i) <= 1
  
  (w * w_i + t * t_i + r * r_i) > 0.99
  
  This allowed us to find at least five zip code regions where a person could satisfy the indifference constraint as opposed to zero.
  
  To conduct the multiple linear regression, we used the Property Assessments dataset from the Boston Data Portal. We wanted to create a model to predict a building's assessed value. Using number of rooms, number of bedrooms, number of bathrooms, building area, and the year built, we conducted the multiple linear regression using the statsmodels, a package for python. 


 
