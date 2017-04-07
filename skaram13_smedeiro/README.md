##Project 2

For project 2, we continued our focus on education in Boston but switched our problem to deal with the transportation issue plaguing our public schools. Currently transportations costs account for 11% of the districts budget. In this project our goal is to optimize where the bus stops for students that must walk to the corners.

This problem is too large to solve in one three week project so we chose to break it down by looking more closely at the Routing Challenge. We want to determine where the most effeective bus stops would be for students that are picked up at the corner. We chose to ignore the constraint that they cannot walk more than .5 miles.

The data sets that we chose are the students-simulated.geoson and the Boston, MA geojson (OSM2PGSQL) which we retrieved from https://mapzen.com/data/metro-extracts/metro/boston_massachusetts/.

The students-simulated file gives us simulated boston students, which school they currently attend, how far they are allowd to walk, their location in lat and long coords, the start and end time for their school, their grade, and much more information. But we chose to focus on their current school, their max walking distance, and the lat and long coordingates of where they live.

In order to pick the best bus stops, we employed the k-means algorithm. Fist we sepearated the students by schools and then ran k-means on these groups of students. We used k-means to find the best k bus stops for each school.

After finding the best places for the stops, we had to deal with another constraint problem given by the city of Boston. We cannot have the children walk to any random point, it has to be a corner. 

So next we transformed our results from k-means to corners in Boston. To do this we followed the theory behind rtrees but we wrote our own version so that we could minimize unnecessary work. 

For this step we had to use the city of boston geojson dataset in order to get all of the corners that could be possible stops for the buses. Then we created a box for each school so that we could minimize the amount of area that we have to cheak to place bus stops.

#Project 1
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











