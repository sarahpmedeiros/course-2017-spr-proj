# This is a helper function to calculate the centroid of all students belongs to
# one school
import transformData
import numpy
from geopy.distance import vincenty

def group(students, buses):
    projection_students = transformData.project(students, lambda t:(t['Assigned School'],[t['Latitude'],t['Longitude']]))
    # Now we have the centroid information of pick up location for every students
    # of each individual school
    Locations_students = transformData.aggregate(projection_students,cal_centroid)
    # Now we need to compute the distance between each bus yard and centroid and find the min
    closest_yard = []
    for i in range(0,len(Locations_students)):
        centroid = [Locations_students[i][1][0], Locations_students[i][1][1]]
        min_distance = 1000000
        min_yardid = ''
        for j in range(1,len(buses)):
            if(buses[j]['_id'] != buses[j-1]['_id']):
                yard = [buses[j]['Bus Yard Latitude'],buses[j]['Bus Yard Longitude']]
                distance = vincenty(centroid, yard).miles
                if(distance < min_distance):
                   min_distance = distance
                   min_yardid = buses[j]['_id']
        closest_yard.append({'_School':Locations_students[i][0],
                             '_Closestyard':min_yardid,
                             '_distance':min_distance})
    return closest_yard

def cal_centroid(*points):
    points = points[0]
    x_coords = [float(p[0]) for p in points]
    y_coords = [float(p[1]) for p in points]
    _len = len(points)
    centroid_x = sum(x_coords)/_len
    centroid_y = sum(y_coords)/_len
    return [centroid_x, centroid_y]
