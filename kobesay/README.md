# Project 1
## Intro
We are interested in the relation between personal income and amount of infrastructures in different regions of Bostion city. Do people with higher income share more infrastructure resource than others, or no significant relation of income and infrastructure resource?
To anwser the question, in this project, data of employee earnings, hospital and public/private school in Boston city are investigated to generate four result datasets, which may help figure out the question.

## Input datasets
### 3 portals
* City of Boston Data Portal
* Boston Wicked Open Data
* BostonMaps: Open Data

### 5 datasets
* Employee Earnings Report 2013: https://data.cityofboston.gov/resource/54s2-yxpg
* Employee Earnings Report 2014: https://data.cityofboston.gov/resource/4swk-wcg8
* Hospital Locations: https://data.cityofboston.gov/resource/46f7-2snz
* Public Schools: https://boston.opendatasoft.com/explore/dataset/public-schools
* NON PUBLIC SCHOOLS: http://bostonopendata.boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1

## Algorithm & Transformation
* get_data.py: fetch 5 datasets from web services and store them in mongodb
* trans_income.py: Process raw data of employee earnings(2013, 2014) and calculate average personal income for each region of Boston city
* trans_school.py: Process raw data of public/private schools and count amount of schools(public, private and all, respectively) in each region of Boston city
* trans_hospital.py: Process raw data of hospital locations and count amount of hospitals in each region of Boston city
* join_by_region.py: Join data of income, school, hospital by region to see relation between those factors

## Output datasets
* regionincome: average personal income for each region of Boston city
* regionschool: amount of schools(public, private and all, respectively) in each region of Boston city
* regionhospital: amount of hospitals in each region of Boston city
* income_infrastructure: relation between income and infrastructure resource in each region of Boston city

## Run scripts automatically
* python3 execute.py demo

## Run scripts manually
* 1. enter the directory of username
* 2. python3 get_data.py
* 3. python3 trans_income.py
* 4. python3 trans_school.py
* 5. python3 trans_hospital.py
* 6. python3 join_by_region.py