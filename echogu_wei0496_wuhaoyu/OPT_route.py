# mergeLandmarksHubway
# merge Boston Landmarks data set with Hubway Stations data set
# for each historic landmark, find the distance to the nearest Hubway

import urllib.request
import json
import numpy
import dml
import prov.model
import datetime
import uuid
import groupstudentbyschool
import transformData
import MST
from geopy.distance import vincenty

class OPT_route(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.buses', 'echogu_wei0496.schools','echogu_wei0496.students']
    writes = []

pychppppppp    @staticmethod
    def execute(trial = False):
        ''' Clean up and merge some data sets.
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # loads the collection
        Raw_Buses = repo['echogu_wei0496.buses'].find()
        Raw_Schools = repo['echogu_wei0496.schools'].find()
        Raw_Students = repo['echogu_wei0496.students'].find()

        # projection
        Buses = []
        for item in Raw_Buses:
            try:
                Buses.append({'_id': item['_id'],
                                  'Bus Yard Address': item['Bus Yard Address'],
                                  'Bus Yard Latitude': item['Bus Yard Latitude'],
                                  'Bus Yard Longitude': item['Bus Yard Longitude']})
            except:
                pass

        Schools = []
        for item in Raw_Schools:
            try:
                Schools.append({'_id': item['_id'],
                                'Address': item['Address'],
                                'Latitude': item['Latitude'],
                                'Longitude': item['Longitude']})
            except:
                pass

        Students = []
        for item in Raw_Students:
            try:
                Students.append({'_id': item['_id'],
                                  'Latitude': item['Latitude'],
                                  'Longitude': item['Longitude'],
                                  'School Latitude': item['School Latitude'],
                                  'Assigned School': item['Assigned School'],
                                  'School Longitude': item['School Longitude']})
            except:
                pass

        # Now we group each student by their assigned schools
        Location_yards = groupstudentbyschool.group(Students, Buses)
        print(Location_yards[0])
        # Now we fixied the capacity of the buses, and send the students' locations
        # as coordinates and convert them to a graph, find the MST.
        # First, group the student belongs to the same school
        projection_students = transformData.project(Students, lambda t:(t['Assigned School'],[t['Latitude'],t['Longitude'],t['_id']]))
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
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:echogu_wei0496#mergeLandmarksHubway',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_BLCLandmarks = doc.entity('dat:echogu_wei0496#BLCLandmarks',
                                           {'prov:label': 'BLC Landmarks', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_HubwayStations = doc.entity('dat:echogu_wei0496#HubwayStations',
                                             {'prov:label': 'Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_LandmarksHubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_LandmarksHubway, this_script)
        doc.usage(get_LandmarksHubway, resource_BLCLandmarks, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_LandmarksHubway, resource_HubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        LandmarksHubway = doc.entity('dat:echogu_wei0496#LandmarksHubway',
                                     {prov.model.PROV_LABEL: 'Landmarks Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(LandmarksHubway, this_script)
        doc.wasGeneratedBy(LandmarksHubway, get_LandmarksHubway, endTime)
        doc.wasDerivedFrom(LandmarksHubway, resource_BLCLandmarks, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)
        doc.wasDerivedFrom(LandmarksHubway, resource_HubwayStations, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)

        repo.logout()

        return doc
