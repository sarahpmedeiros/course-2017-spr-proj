#Boston Public School Funding

Renzo Callejas and Hannah Schurman

##Problem

Our goal for this project is to determine the correlation between Boston Public School funding, graduation rates, SAT scores, school location, and the number of students that continued on to higher education

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
This file uses the data from the Funding data set and the Gradrates data set, merges them, and then filters out their information to create a data set that includes 'School Name' and 'School Funding'

(Eventually we want to include data from a few consecutive years to see how that data changes over time)
  
