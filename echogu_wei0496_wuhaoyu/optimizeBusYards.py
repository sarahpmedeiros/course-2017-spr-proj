# optimizeBusYards.py

import urllib.request
import json
import numpy
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty
from echogu_wei0496_wuhaoyu import transformData
from echogu_wei0496_wuhaoyu import MST

class optimizeBusYards(dml.Algorithm):
    contributor = 'echogu_wei0496_wuhaoyu'
    reads = ['echogu_wei0496_wuhaoyu.buses', 'echogu_wei0496_wuhaoyu.schools','echogu_wei0496_wuhaoyu.students']
    writes = ['echogu_wei0496_wuhaoyu.bus_yards']

    @staticmethod
    def execute(trial = False):
        ''' optimize school bus route
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496_wuhaoyu', 'echogu_wei0496_wuhaoyu')

        # loads the collection
        raw_buses = repo['echogu_wei0496_wuhaoyu.buses'].find()
        raw_schools = repo['echogu_wei0496_wuhaoyu.schools'].find()
        raw_students = repo['echogu_wei0496_wuhaoyu.students'].find()

        # projection
        buses = []
        for item in raw_buses:
            try:
                buses.append({'_id': item['_id'],
                              'Bus Yard Address': item['Bus Yard Address'],
                              'Bus Yard Latitude': item['Bus Yard Latitude'],
                              'Bus Yard Longitude': item['Bus Yard Longitude']})
            except:
                pass

        schools = []
        for item in raw_schools:
            try:
                schools.append({'_id': item['_id'],
                                'Address': item['Address'],
                                'Latitude': item['Latitude'],
                                'Longitude': item['Longitude']})
            except:
                pass

        students = []
        for item in raw_students:
            try:
                students.append({'_id': item['_id'],
                                  'Latitude': item['Latitude'],
                                  'Longitude': item['Longitude'],
                                  'School Latitude': item['School Latitude'],
                                  'Assigned School': item['Assigned School'],
                                  'School Longitude': item['School Longitude']})
            except:
                pass

        # group each student by their assigned schools
        bus_yards = optimizeRoute.__group(students, buses)
        repo.dropCollection('echogu_wei0496_wuhaoyu.bus_yards')
        repo.createCollection('echogu_wei0496_wuhaoyu.bus_yards')
        repo['echogu_wei0496_wuhaoyu.bus_yards'].insert_many(bus_yards)
        repo['echogu_wei0496_wuhaoyu.bus_yards'].metadata({'complete': True})
        print(repo['echogu_wei0496_wuhaoyu.buses'].metadata())
        #print(bus_yards[0])

        # fixied the capacity of the buses, and send the students' locations
        # as coordinates and convert them to a graph, find the MST.
        # First, group the student belongs to the same school
        projection_students = transformData.project(students, lambda t: (t['Assigned School'], [t['Latitude'], t['Longitude'], t['_id']]))
        result = MST.find_mst(projection_students)
        print(result[0])

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        ''' Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496_wuhaoyu', 'echogu_wei0496_wuhaoyu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:echogu_wei0496_wuhaoyu#mergeLandmarksHubway',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_BLCLandmarks = doc.entity('dat:echogu_wei0496_wuhaoyu#BLCLandmarks',
                                           {'prov:label': 'BLC Landmarks', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_HubwayStations = doc.entity('dat:echogu_wei0496_wuhaoyu#HubwayStations',
                                             {'prov:label': 'Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_LandmarksHubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_LandmarksHubway, this_script)
        doc.usage(get_LandmarksHubway, resource_BLCLandmarks, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_LandmarksHubway, resource_HubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        LandmarksHubway = doc.entity('dat:echogu_wei0496_wuhaoyu#LandmarksHubway',
                                     {prov.model.PROV_LABEL: 'Landmarks Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(LandmarksHubway, this_script)
        doc.wasGeneratedBy(LandmarksHubway, get_LandmarksHubway, endTime)
        doc.wasDerivedFrom(LandmarksHubway, resource_BLCLandmarks, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)
        doc.wasDerivedFrom(LandmarksHubway, resource_HubwayStations, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)

        repo.logout()

        return doc

    @staticmethod
    def __group(students, buses):
        projection_students = transformData.project(students, lambda t:(t['Assigned School'], [t['Latitude'], t['Longitude']]))
        # Now we have the centroid information of pick up location for every students
        # of each individual school
        student_locations = transformData.aggregate(projection_students, optimizeRoute.__cal_centroid)
        # Now we need to compute the distance between each bus yard and centroid and find the min
        closest_yard = []
        for i in range(0, len(student_locations)):
            centroid = [student_locations[i][1][0], student_locations[i][1][1]]
            min_distance = float('inf')
            min_yardid = ''
            for j in range(1, len(buses)):
                if(buses[j]['_id'] != buses[j-1]['_id']):
                    yard = [buses[j]['Bus Yard Latitude'], buses[j]['Bus Yard Longitude']]
                    distance = vincenty(centroid, yard).miles
                    if(distance < min_distance):
                       min_distance = distance
                       min_yardid = buses[j]['_id']
            closest_yard.append({'School': student_locations[i][0],
                                 'Closest Yard': min_yardid,
                                 'Distance': min_distance})
        return closest_yard

    @staticmethod
    def __cal_centroid(*points):
        points = points[0]
        x_coords = [float(p[0]) for p in points]
        y_coords = [float(p[1]) for p in points]
        _len = len(points)
        centroid_x = sum(x_coords)/_len
        centroid_y = sum(y_coords)/_len
        return [centroid_x, centroid_y]
