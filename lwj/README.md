CS591 Data Mechanics
Wenjing Lyu
Project-one

###About Project
This project intends to characterize neighborhoods in Greater Boston Area based on several measurement index. The key measurement would be the economic condition and accessibility. The economic condition in certain areas will be evaluated based on Property Assessment 2016 dataset, and the accessibility of a neighborhood will be evaluated based on the sources from following datasets: Hospital Locations, Colleges and Universities, Waze Jam Data, Community Gardens, Farmers Markets. These datasets will be merged to answer the question: Is this area fully developed already? If not, what part could be improved to increase the overall accessibility. 

###About Transformations
I defined a selection function to filter the Farmers Markets data first, which will help filter out the farmers markets in Masshachusetts but out of Boston area. Then I defined a union function to combine Hospital, Colleges and Universities, Gardens, and Markets datasets into one dataset based on a new schema and add a “Type” column to identify the place type. Since union.py reads the dataset from mongoldb instead of online source, the filtermarket.py should be run before the union.py. The third function is projection, which add a new column “average value” for the property assessment dataset by calculating total value/land area. Since the file of Property Assessment 2016 is too large (169,199 rows)to be handled from online source, I downloaded it first and then open the dataset from local. 

###Service Instructions
The filtermarket.py should be run before the union.py
You should download Property Assessment 2016 from City of Boston Data Portal before running propertyaccess.py
You can download the dataset here: https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5

###Future Plan
The Boston Neighborhood Shapefiles dataset would be later used to identify the boundaries for each neighborhood and the Waze Jam Data would probably be used to evaluate the traffic in a specific area.This project will help identity the overall characteristics of each neighborhood in Boston area and give some suggestions for the city development.
