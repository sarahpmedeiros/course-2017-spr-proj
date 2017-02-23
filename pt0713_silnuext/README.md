## Datasets we use

From City of Boston Data Portal:

Property Assessment 2014: https://data.cityofboston.gov/dataset/Property-Assessment-2014/qz7u-kb7x
Property Assessment 2015: https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
Crime Incident Reports: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx

From BostonMaps Open Data: 
Police Districts: http://bostonopendata-boston.opendata.arcgis.com/datasets/9a3a8c427add450eaf45a470245680fc_5?uiTab=table

From MassData:
FLD Complaints: https://data.mass.gov/dataset/FLD-Complaints/c5kv-hee8


## Short narrative of How our datasets are going to be used and What our project will be focus on

We are interested in “Boston crime incidents” in relation to “property values”, “police districts”, and  “fair labor division complaints”. Our main dataset will be Crime Incident Reports and we have come out three assumptions: First, there will be less crime incidents when “the total assessed value of property” rises. To do so, we look at data from Property Assessments from year 2014 and 2015 and compare if there are any correlations between the total assessed value for property and the number of crime incidents. Second, we assume that there will be less crime incidents within police districts. To do so, we look at the “district” column from Police Districts and check the proportion that the number of the districts where the crime incident equals to police district out of the total number of crimes. Last but not least, while there are many researches indicate that unemployment causes crime, we want to look that whether employees’ happiness also affects crime rate. We assume that there will be less crime rate when Fair Labor Division receives less complaints because people are happier and will not tend to behave silly. In this case, we compare data of FLD Complaints in Year 2015 in Boston area.


## Non-trivial transformations we implemented

(property_crime) The first non-trivial transformation we do is combining "Crime Incident Reports" with "Property Assessments" of 2014 and 2015, and we use projection to get columns of "av_total" and "location" from Property Assessments into property14_price_coordination and property15_price_coordination. We then extract the column of "location" from Crime Incident Reports into crime_14coordination and crime_15coordination, and see if there exists any relation between them. 
(police_crime) Second, we combine Police Districts and Crime Incident Reports and use projection to get the single "DISTRICT" column from Police Districts and "reptdistrict" from Crime Incident Reports to calculate the proportion mentioned earlier. 
(fld_crime) Finally, we combine Crime Incident Reports and FLD Complaints, then use projection to extract columns of "business city == Boston" and "date received" within Jan-Aug of 2015 from FLD Complaints, and also use projection to get "year == 15" and "month <= 8" from Crime Incident Reports and find the correlation of Jan-Feb, Mar-Apr, May-Jun, Jul-Aug between the columns we use.
