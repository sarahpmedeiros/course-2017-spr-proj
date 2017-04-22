#Boston Public School Funding

Renzo Callejas and Hannah Schurman

##Update for Project 2

##Instructions to Run Files:
1. Run retrieve.py to import initial datasets (more information under Data Sets below)
2. Run funding_gradrates.py, funding_location.py, and funding_SAT.py in order to transform the data 
3. Run corr_gradrates.py, corr_location.py, and corr_SAT.py to find the appropriate correlations

##In project 2, we begin to answer two questions:
1. Does the funding a school recieves impact its average SAT score and graduation rates? In other words, is there a correlation between school funding and average SAT scores and graduation rates?
In order to answer this question, we took school funding, SAT score, and graduation rate data from 2008-2016 (it is dummy data for now because we are waiting on BPS to send us the full data). We then found the correlation between funding and SAT scores, and funding and graduation rates. Because we used dummy data, there is no correlation for now. For the next project, we expect to have a real result.

2. Does the total funding of a school's closest three schools impact the original school's average SAT score and graduation rate? 
To answer this question, we took a correlation, as before.

##Problem

Our goal for this project is to determine the correlation between Boston Public School funding, graduation rates, SAT scores, school location, and the number of students that continued on to higher education. We thought it would be interesting to see whether higher funding correlates to higher SAT scores, or if higher funding correlates to school location, etc. We want to learn if our assumptions about the Boston Public School system are in fact correct or if the correlation between funding and the other factors are unexpected, or even non-existant. We plan on taking data from about 4 consecutive years to see if there is also a correlation between these factors over time as well.   

Our first step was to find data sets relevant to these topics from the Boston Data Portals and the Boston Public School System, then create transformations to process these data sets in formats which we can then analyze their correlation


##Data Sets
Graduates Attending College (https://data.mass.gov/Education/Graduates-Attending-College-Research-File-by-schoo/grya-vhq5/data)

Boston Public Schools (https://data.cityofboston.gov/dataset/Boston-Public-Schools-School-Year-2012-2013-/e29s-ympv/data)

BPS SAT Scores

BPS Graduation Rates

BPS Funding

###retrieve.py
This file retrieves the data sets from our different data portals and sources

###funding_gradrates.py
This file uses the data from the Funding data set and the Gradrates data set, merges them, and then filters out their information to create a data set that includes 'School Name','School Funding', "graduation rates', and 'Number of grads that continued to higher education'
(Eventually we want to include data from a few consecutive years to see how that data changes over time)

###funding_location.py
This file uses the data from the Funding data set and the Location data set and uses transformations to create a data set containing 'School Name', 'School Funding', ['zip code', ['latitiude', 'longitude']]

###funding_SAT.py
This file uses the data from the Funding data set and the SAT data set and uses transformation to create a data set containing 'School Name', 'School Funding', 'School SAT Data'

##corr_gradrates.py
Returns a list of the correlation between funding and graduation rate for every school. Example: [{'SAT_Funding Correlation': None, 'School Name': 'Boston Adult Technical Academy'}, {}, ...]

##corr_SAT.py
Returns a list of the correlation between funding and average SAT score for every school. Example:
[{'School Name': 'Boston Latin Academy', 'SAT_Funding Correlation': None}, {'School Name': 'English High', 'SAT_Funding Correlation': None}, {}, ...]

##corr_location
Returns a list of the correlation between schools A's average SAT score and the total funding of school A's closest 3 neighboring schools. Also, returns correlation between school A's graduation rate and the total funding of school A's closest 3 neighbors. Example:
[{'Grad Rates Correlation': None, 'SAT_Neighbor Funding Correlation': None, 'School Name': 'Boston Latin Academy'}, {}, ...]