CS591 L1 Project 1
Name: Xi Chen, Yilin Feng

We chose the following 5 datasets to analyze how the value of real estate properties change in Boston due to factors such as access to the restaurant, schools and income per capita.

Our first  two datasets, Property Assessment 2014 and Property Assessment 2015, from the City of Boston, contains information about the property’s location and value.

Our third dataset, Boston public school, contains information about the name and location of Boston’s public elementary middle schools and high schools. Based on the location, we counted the number of schools near a particular property. 

Our fourth dataset, active food establishment license, contains information about the name and location of Boston’s restaurants. We want to know if access to food establishment adds value to properties.  

Our fifth dataset, individual Income Tax Statistics, contains information on total income, tax returns, zip codes and etc. 

These datasets combined together could explain if distance to food establishments  and schools combined with the average income based on zip code are correlated with the value of properties. Also, at the end of the project, we would give each property a score based on the evaluations of those dimensions.   


Project 2
Name: Chen Xi, Yilin Feng


We try to figure out if the value of Boston properties depends mainly on their location. We used property tax as a way to determine value. 
We used the K-means algorithms, with our modification, to find the means of clusters of restaurants, schools and properties. From these clusters, we can determine where centroids are located in the populated ares. 
We then used correlation analysis to see if the distance between each property to the nearest mean is negatively correlated with propert's value. We believed that a shorter distance would yield a larger value. 
We found that the correlation is actually a small positive value with a small P-value, which means the correlation is statistically significant.
Tax probably was not a good indication of value. Also, Boston's property's value could depend on othe factors besides distance to dining and education. Factors such as rent, environment and etc could have effects on value. 
