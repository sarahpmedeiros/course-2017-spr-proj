Group: johnt3_rsromero
Group Members: John Forrest Tokarowski and Ramon Sanchez

Data Sets:
1. Boston Police Department Crime Data
2. Boston Police Department Field Interrogation and Observation Data
3. Boston Police Department Locations Data
4. Cambridge Police Department Citation Data
5. Cambridge Police Department Crime Data
6. Cambridge Police Department Location Data

Currently our data sets come from three sources: Boston Data Portal, Cambridge Data Portal, and Google Maps. These data sets allow you to analyze the location of crimes relative to the police station locations in both Boston and Cambridge. These data sets when combined can allow you to look into a wide variety of problems: Where does crime most occur in Boston and Cambridge? Does the location of a Police Department close to an area heavily prevent/reduce crime in that area? Where would the optimal location be to place another police department? How are police districts drawn? How might they be redrawn to better service local crime?

Write a short narrative and justification (at least 7-10 sentences) explaining how the data sets and techniques you hope to employ can address the problem you chose. Depending on what techniques you are using and what problem you are trying to solve, you may need to provide a justification or evaluation methodology for the techniques you are using (i.e., why you believe the chosen techniques will solve the problem you are trying to address).

Question 1: Where are the optimal locations of the eleven police district stations?
Solution: In order to solve this problem we implemented the k-means algorithm onto our dataset. Our current data set consisted of all the crimes listed in our "Boston Crime Data" and "Boston Field Investogations and Operations Data" and their respective coordinates. Using this data we ran the k-means algorithm using eleven total means corresponding to the actual locations of the police district stations as our input means. The result gave us eleven new optimal locations based on the distribution of crimes across Boston.

Question 2: If we were to have to close a single police district station some time throughout the year which would be the best station to close and when?
Solution: In order to solve this problem we analyzed the data and focused on finding the amount of crimes that occured in every month of the year for each respective district. This resulted with a total of eleven arrays for each respective district police station in the following format: [#crimes in January, #crimes in February, ...., #crimes in December]. Once these arrays are obtained it is very simple to analyze the data and locate at what time of year in which district are there the least crimes.