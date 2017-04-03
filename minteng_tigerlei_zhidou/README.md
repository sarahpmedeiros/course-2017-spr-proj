# Project1
### *by Minteng Xie, Zhi Dou*

## Narrative

We want to find a best office location for a new company. The first factor we consider is the rent of the location, so we find the data of average rent for different cities in Greater-Boston. The second factor is convenient transportation, then we fetch the MBTA data, and we assume that the location will be more convenient if there are more stops around this location. The third factor is food, we think the location is better if there are many restaurants around it. Similarly, the fourth factor is safety, we do not want too many crimes appeared near the location. The last factor is salary, we may add some fuctions related to different types of jobs.

## Datasets

1. [Average Rent](http://datamechanics.io/data/minteng_zhidou/rent.txt)
2. [MBTA Stops](http://datamechanics.io/data/minteng_zhidou/stops.txt)
3. [Food Data](https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq)
4. [Safety(Crime)](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap)
5. [Salary 2015](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2015/ah28-sywy)
6. [Salary 2014](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2014/4swk-wcg8)
7. [Goolge Maps](https://www.google.com/maps)

## Transformations
1. In first transformations, we combine rent table with zipcode of accorsponding area. First we fetch longitude and latitude based on the name of area in rent table via google maps api and then using this location information as input to fetch zipcode also with the help of google maps api. Then combine location table with rent table and implement aggregation to get the final data set with rent and the zipcode.

2. Project MBTA, Food and Safety data, besides the needed infomation, for the value of key "location", we add tags such that (location, "transport") for MBTA data, (location, "food") for Food data and (location, "crime") for Safety data. Then implement union of three datasets into the second new dataset. After union, implement selection to remove data with invalid locations (for example, with longitude and latitude equal to 0).

3. After project the needed information, combine the two Salary data, then aggragate the dataset using the job title as the key, get the average salary for different jobs. The third new dataset contain the key: job title, values: average salary and other infomation such as company name, year and so on.

# Project 2
### *by Minteng Xie, Yue Lei, Zhi Dou*

## auth.json
This project use app token from boston data portal and googlemaps. To retrieve data automatically, app token should be added into `auth.json` file as follow format:
```
{
    "services": {
        "googlemapsportal": {
            "service": "https://developers.google.com/maps/",
            "username": "alice_bob@example.org",
            "key": "xxXxXXXXXXxxXXXxXXXXXXxxXxxxxXXxXxxX"
        },
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": "alice_bob@example.org",
            "token": "XxXXXXxXxXxXxxXXXXxxXxXxX",
            "key": "xxXxXXXXXXxxXXXxXXXXXXxxXxxxxXXxXxxX"
        }
    }
}
```

## Trial mode
In trial mode, the algorithm would complete its execution very quickly (in at most a few seconds) by operating on a very small portion of the input data set(s). 

Remember to uncomment last few lines of the file
```python
# if 'trial' in sys.argv:
#     <filename>.execute(True)
# else:
#     <filename>.execute()
```
To run it:
```python
python3 <filename>.py trial
```
## Problem 1: Optimization
Following the idea of project 1, our goal is to find a best office location for a new company. Since a detailed coordinate is meaningless, we try to find a suitable area. Project 1 has helped us gather all the licensed restaurants/ crime incidents/ MBTA stops/ rent price in boston area. We gonna find the best area that maximize ```#restaurant```, ```#MBTA stops``` and minimize ```#crime incidents``` and ```rent price```. 

We use googlemaps api to find the left bottom/ right top coordinates of boston area. With these coordinates, we could build a big rectangle containing boston area. Then we separate this rectangle into 10 x 10 grids(user could set this scale manually). Removing those blank grids which don't contain any useful data, we have 52 grids left. Each grid represents a possible target area which contains a suitable place for opening a company. 

![boston_grid](http://datamechanics.io/data/minteng_zhidou/Boston_grid.png)

Therefore, we could count the number of restaurant/ crime incidents/ MBTA stops(including buses and subway) in every grid and evaluate these numbers by mapping them into scores from 1-5. And according to the center coordinate of this grid, googlemaps api could help us to find it belongs to which area(like Allston/ Back Bay/ Fenway/...). 

![box_count](http://datamechanics.io/data/minteng_zhidou/map_with_label.png) 

Then our database would search and find matched rent price, which also should be mapped into reversed scores from 1-5. Then we get the tuple of ratings(The higher the better) for each grid in the format of 
```
(transport, food, safety, rent) 
```
User could customize rating due to their preference in ```Result_Example.ipynb```. We provide an example that our algorithm rank all 52 grid area according to user's requirement of grades: ```(3, 4, 3, 4)``` and provide top choices as well as its bound coordinates, area name & zipcode, average rent and ratings.
```
Top fitted areas:
Bound: [[42.2622102, -71.0835318], [42.2793753, -71.0566365]]
Area: Mattapan 02126    Avg rent: 1403
Grades: {'safety': 3, 'rent': 5, 'food': 4, 'transport': 4} 

Bound: [[42.2622102, -71.1642177], [42.2793753, -71.1373224]]
Area: West Roxbury 02132    Avg rent: 1476
Grades: {'safety': 3, 'rent': 4, 'food': 4, 'transport': 3} 

Bound: [[42.2793753, -71.0566365], [42.2965404, -71.0297412]]
Area: Dorchester 02122    Avg rent: 1524
Grades: {'safety': 2, 'rent': 4, 'food': 4, 'transport': 3} 

Bound: [[42.2622102, -71.1373224], [42.2793753, -71.11042710000001]]
Area: Roslindale 02131    Avg rent: 1685
Grades: {'safety': 2, 'rent': 3, 'food': 4, 'transport': 3} 

Bound: [[42.3137055, -71.1373224], [42.330870600000004, -71.11042710000001]]
Area: Jamaica Plain 02130    Avg rent: 2214
Grades: {'safety': 3, 'rent': 3, 'food': 5, 'transport': 2} 
```
## Problem 2: Statistical Analysis
After finding ideal area for a new company, we would like to dig deeper into those areas to explore the trend of crime incidents. 

For each block(grid), we could count the number of crime incidents of each month in different years(2013- 2016). We build a matrix with ```year``` x  ```#blocks``` x ```#month```(5 x 52 x 12). Then we calculate **correlation coefficient** and **p-value** for each block so that we could compare them in consistent years. We find that for some blocks, these **correlation coefficient** are close to 0 and **p-value** are close to 1, which means for these blocks, their crime incidents in consistent years are not related. However, we have the ability to do reasonable prediction for those blocks which have high **correlation coefficient** and low **p-value** year by year.  

|               | correlation coefficient |       p value       |
|:-------------:|:-----------------------:|:-------------------:|
|    12-13      |  0.836472540464         |  0.000695048480985  |
|    13-14      |  -0.038                 |  0.003              |
|    14-15      |  0.735134468067         |  0.00644758659544   |
|    15-16      |    |  0.001              |


![fitting](http://datamechanics.io/data/minteng_zhidou/fitting.png) 

![block27](http://datamechanics.io/data/minteng_zhidou/Block27.png) 
