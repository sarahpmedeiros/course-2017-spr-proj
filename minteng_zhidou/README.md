# Project1
### *by Minteng Xie, Zhi Dou*

# Narrative

We want to find a best office location for a new company. The first factor we consider is the rent of the location, so we find the data of average rent for different cities in Greater-Boston. The second factor is convenient transportation, then we fetch the MBTA data, and we assume that the location will be more convenient if there are more stops around this location. The third factor is food, we think the location is better if there are many restaurants around it. Similarly, the fourth factor is safety, we donot want too many crimes appeared near the location. The last factor is salary, we may add some fuctions related to different types of jobs.

# Datasets

1. [Average Rent](http://datamechanics.io/data/minteng_zhidou/rent.txt)
2. [MBTA Stops](http://datamechanics.io/data/minteng_zhidou/stops.txt)
3. [Food Data](https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq)
4. [Safety(Crime)](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap)
5. [Salary 2015](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2015/ah28-sywy)
6. [Salary 2014](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2014/4swk-wcg8)

# Transformations
1. Rent.....
2. Project MBTA, Food and Safety data, besides the needed infomation, for the value of key "location", we add tags such that (location, "transport") for MBTA data, (location, "restaurant") for Food data and (location, "crime") for Safety data. Then implement union of three datasets into the second new dataset.
3. After project the needed information, combine the two Salary data, then aggragate the dataset using the job title as the key, get the average salary for different jobs. The third new dataset contain the key: job title, values: average salary and other infomation such as company name, year and so on.
