# jgrishey

For this project, I decided to use the MBTA Developer Portal, the City of Boston Data Portal,
the City of Cambridge Data Portal, and also the Geonames Data Portal. My first transformation
is to combine the MBTA's stationByRoute data and the Geonames nearbyStreets data. I acquired
all of the names of the stations along the Red Line and then added their nearby streets to the
dataset. My second transformation used the crime data from the City of Boston data portal and
the Red Line stations from the MBTA again. I was able to give the number of crime reports nearest
to each Red Line Station. My third and final transformation used the car accident and parking ticket
data from the City of Cambridge and also used the Geonames nearbyPlace data. I was able to get the nearest
landmark to each car accident and parking ticket and create a dataset containing each landmark's parking
ticket and car accident data.

MBTA - Need an account/token

City of Boston/Cambdrige - Socrata

Geonames - Need an account/username (No token)
