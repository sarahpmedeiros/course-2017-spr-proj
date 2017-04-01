# This is a helper function to calculate the direct distance between two geo points
# If possible, will substitute with a rotue finder to find the actual route distance
from geopy.distance import vincenty

def distance(point1, point2):
    return vincenty(point1, point2).miles
