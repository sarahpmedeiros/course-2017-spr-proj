# CS591 Project #2 Proposal
#### Sean Zhang & Martin Yim

## The problem
  We are interested in discovering the feasibility of living in Boston in terms of property/assessed value as well as other related factors. 
 
  Naturally, we first have to figure out where we can live. We begin by solving a constraint satisfaction problem; given that I have a budget of X dollars for a home, I work at a company at location Y, and want a commute time (both walking and public transportation) of less than Z, where can I live? This problem is nontrivial; there are multiple constraints that must be satisfied: walking time, transit time, and residential property cost. For our problem, we decided to use a budget of $blank, Kendall Square as the work location, a walk time of 7 minutes, and a transit time of 30 minutes.

  Assume that we have a property we currently live in and its cost, the walking time to get to the station, and the commute time via public transportation. Given this information, we would like to find other potential places to live within the same zipcode that we would be indifferent towards, based on those location's respective costs, walking times, and commute times. If there does exist a place(s) in which we are indifferent compared to our current location and its commuting attributes, we would return the "indifferent" locations, as well as our objective function's weights - our preferences.

  This kind of constraint satisfaction problem could actually be very applicable in various aspects. For example, say you live in Allston and take the T to commute to work at Kendall Square. Now, city planners have decided to finally renovate the entire block that you live on in Allston. Their claim is that you could move and live in Fenway with a different walk and commute time, but still be indifferent in terms of your overall tradeoffs for the times and costs needed at these two locations. Our script would be able to check and verify this: is this really true? Does there exist a person who would be indifferent towards living one place or another within a certain region?

  Once we found a feasible subset of places that satisfy our constraints, we thought that it would be interesting to see the relationship between various characteristics of these residential areas and the assessed value of the building. Namely, we took the data provided from the Boston Open Data portal, and created a multiple linear regression for various different factors, including number of rooms, number of bedrooms, number of bathrooms, building area, and year built the building was built to predict the assessed value of building.

  

## How we did it
    
  From Project #1, we had already gotten data on assessed property data and MBTA location data, but to solve the constraint satisfaction problem, we actually needed to pull data from the Google Maps Places API Web Services. Given this information, we then want to iterate through our other possible residential locations (within the same zipcode)with their attributes (walking time to station, commute time to work via T/Bus) and find locations with which it is possible to find a set of preferences (weights) to our constraints (walking time, commute time, cost). We then return the weights and locations we are indifferent towards
  
  To conduct the multiple linear regression, we used the Property Assessments dataset from the Boston Data Portal. We wanted to create a model to predict a building's assessed value. Using number of rooms, number of bedrooms, number of bathrooms, building area, and the year built, we conducted the multiple linear regression using the statsmodels, a package for python. 


 
