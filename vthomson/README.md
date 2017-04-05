## Project Part 1

There has always been a problem with city traffic, and this has led to an increase in mental health issues and outbursts of crime. I looked into both Boston and Cambridge’s traffic patterns and their correlation with crime and street complaints.
I separated my data into two categories: Boston, and Cambridge. For Boston, I looked at the street complaints and the traffic jams on those streets. Then I looked at the streets with said traffic and all the streets with reported crimes. Then I did the same thing with Cambridge’s data. 
With this data, I can see if there is a correlation between complaints, traffic jams, and crime per street in Boston and Cambridge.

In order to retrieve the data, you must run python3 retrieve.py
This will help get the data and store the information into the mongodb.


## Data Sets

Waze Jam
https://data.cityofboston.gov/resource/dih6-az4h.json

Boston Crime Incident Reports
https://data.cityofboston.gov/resource/crime.json

Commonwealth Connect Reports within Massachusetts State
https://data.mass.gov/resource/cb3u-xiib.json

Cambridge Crime Reports
https://data.cambridgema.gov/resource/dypy-nwuz.json

Daily Cambridge Traffic
https://data.cambridgema.gov/resource/aeey-t2nm.json


## Transformations

Transformation 1: The first transformation merges Boston’s detailed street complaints with its traffic patterns per street.

To run this script: $ python3 merge_BostonConnectTraffic.py

Transformation 2: The second transformation merges Cambridge’s detailed street complains with its traffic patterns per street.

To run this script: $ python3 merge_CambridgeConnectTraffic.py

Transformation 3: The third transformation merges Boston’s crime per street with its traffic patterns per street.

To run this script: $ python3 merge_BostonCrimeTraffic.py

