Chloe Fortuna
Takumi House
Ka ram Yang
Shahrez Jan

### The Problem

Boston is a city where many traffic accidents occur. However, with thousands of car crashes every year and only 24 hospitals, it can be a problem to direct the victims of the car crashes to the closest hospitals when there are many external factors such as the time of day at the moment of the accident. Ambulances need to be better prepared for car accidents.

### The Datasets We Chose

#### Boston Hospitals
https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

#### Car Crashes in Boston
http://datamechanics.io/data/cfortuna_houset_karamy_snjan19/CarCrashData.json

## Statistical Analyses

### K-Means (OptimalHospitals.py)
We used the k-means algorithm to calculate the optimal locations for hospitals for the most common traffic accident locations in Boston. With this data, we can determine optimal hospital locations, such that the most common car crash sites can reach the hospital quickly.

### Linear Regression (carCrashTimes.py)
We calculated how many crashes occurred during certain time periods of the day throughout the span of a year. We created a line of best fit in order to predict the frequency of car crashes at these times in a year. With this information, hospitals can be more prepared for the victims of car accidents at particular time periods by anticipating the demand of care needed.
