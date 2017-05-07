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

# Project 2

For this project, I am using the City of Boston Data Portal along with a JSON file that
has data about the major hospitals around Boston. I am getting the crime data from City of
Boston. I'm trying to solve the problem of where hospitals should be located and the expected
load of currently-placed hospitals over time. I am going to use the K-means
algorithm along with crime data to determine where hospitals should be placed. Obviously this
is a bit short-sighted, because crime isn't the only factor in placement of hospitals. Also, I am using
crime and currently-placed hospital data to determine the load of a certain hospital. I iterate through
the crimes and assign them to each hospital, giving each hospital a coefficient based off of its square footage.
This coefficient leads to larger hospitals getting more patients. This could potentially be used to guide a victim
of a crime or even just a person in-need to the best hospital that would have the least load and is closest.

City of Boston - Socrata

Hospital JSON - http://datamechanics.io/data/hospitalsgeo.json
