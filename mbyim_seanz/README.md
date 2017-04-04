# CS591 Project #2 Proposal
#### Sean Zhang & Martin Yim

## The problem
  We are interested in discovering the feasibility of living in Boston in terms of property/assessed value as well as other related factors. 
  
  Naturally, we first have to figure out where we can live. We begin by solving a constraint satisfaction problem; given that I have a budget of X dollars for a home, I work at a company at location Y, and want a commute time (both walking and public transportation) of less than Z, where can I live? This problem is nontrivial; there are multiple constraints that must be satisfied: walking time, transit time, and residential property cost. For our problem, we decided to use a budget of $blank, Kendall Square as the work location, a walk time of 7 minutes, and a transit time of 30 minutes.
  
  Once we found a feasible subset of places that satisfy our constraints, we thought that it'd be interesting to see the relationship between various characteristics of these residential areas and the assessed value of the building. Namely, we took the data provided from the Boston Open Data portal, and created a multiple linear regression for various different factors, including number of rooms, number of bedrooms, number of bathrooms, building area, and year built the building was built to predict the assessed value of building.

  

## How we did it
    
  From Project #1, we had already gotten data on assessed property data and MBTA location data, but to solve the constraint satisfaction problem, we actually needed to pull data from the Google Maps Places API Web Services. Originally, we thought that we could naively used the MBTA location data and the assesed property locations to determine the walking distance/time and then subsequently the transit distance/time to a given work location. However, the world is not mapped like a coordinate plane: there are various structures that prevent us from walking or traveling in a straight line from one point to the next. As a result, we used Google Maps to help us determine nearby transit stops for a given residential area, and estimated travel time from one point to another.
  
  To conduct the multiple linear regression, we used the Property Assessments dataset from the Boston Data Portal. We wanted to create a model to predict a building's assessed value. Using number of rooms, number of bedrooms, number of bathrooms, building area, and the year built, we conducted the multiple linear regression using the statsmodels, a package for python. 


 
