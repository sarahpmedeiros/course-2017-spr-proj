In this project we gathered datasets on high school graduation rates, available technology in
schools, and income of the school's neighborhood. We hope to discover whether neighborhoods with 
higher average incomes and greater access to technology perform better academically. Our overarching goal 
is to identify which neighborhoods need more resources.
 
We chose the following datasets to focus on:

School Address and Organization Code:
http://profiles.doe.mass.edu/search/search.aspx?leftNavId=11238

Census Tract lookup by Address:
https://geocoding.geo.census.gov/geocoder/

Graduation Rates:
http://profiles.doe.mass.edu/state_report/gradrates.aspx

Income Distribution:
https://datausa.io/profile/geo/boston-ma/#income_geo

Technology Report:
http://profiles.doe.mass.edu/state_report/technology.aspx?mode=school&orderBy

First, we used the School Address dataset to retrieve all the 
Boston public high schools names, addresses, and org codes(i.e. a code identifying
the school and district). We then used their addresses to find and add
their according census tract(which is similar to a neighborhood). 
This will allow us to charactize neighborhoods as well as individual schools.

The Income Distribution dataset gives the average income of each census tract
in Boston. We collected this information so that we can connect graduation rates of schools
to the income of the school's neighborhood.

Next, we used information from the Technology Report dataset and the Graduation 
Rate dataset to combine technology availability rates and graduation rates for each Boston public
high school (by org code).

Finally, we added the census tract to each entry in our newly formed Technology/Graduation dataset.
We used the org code of each school to look up the census tract from intial dataset we created.
This will help with analysis of Technology/Graduation rates in relation to the Income Distribution dataset
(as this dataset is organized by census tract).

***NOTES ABOUT RUNNING SCRIPTS***: 
	the getSchools.py script takes approx 5 min to run









