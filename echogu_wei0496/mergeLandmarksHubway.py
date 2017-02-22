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
from geopy.distance import vincenty

class mergeLandmarksHubway(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.BLCLandmarks', 'echogu_wei0496.HubwayStations']
    writes = ['echogu_wei0496.LandmarksHubway']

    @staticmethod
    def execute(trial = False):
        ''' Clean up and merge some data sets.
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # loads the collection
        rawLandmarks = repo['echogu_wei0496.BLCLandmarks'].find()
        rawHubwayStations = repo['echogu_wei0496.HubwayStations'].find()

        # projection
        Landmarks = []
        for item in rawLandmarks:
            try:
                Landmarks.append({'_id': item['_id'],
                                  'properties': item['properties'],
                                  'geometry': item['geometry']})
            except:
                pass

        HubwayStations = []
        for item in rawHubwayStations:
            try:
                HubwayStations.append({'stations': item['geometry']['coordinates']})
            except:
                pass

        # product (id, properties, location, station)
        def product(R, S):
            return [(t, u) for t in R for u in S]

        p = product(Landmarks, HubwayStations)
        p = [{'_id': item[0]['_id'],
                    'properties': item[0]['properties'],
                    'geometry': item[0]['geometry'],                        # landmark locations
                    'stations': item[1]['stations']} for item in p]   # Hubway stations location coordinates

        # aggregation: for each historic landmark, find the closet Hubway station
        # This is not an efficient implementation, the process tabkes about 20 seconds
        LandmarksHubway = []
        keys = {item['_id'] for item in p}
        for key in keys:
            closest = float('inf')
            for item in p:
                if item['_id'] == key:
                    try:
                        polygon = [(c[1], c[0]) for c in item['geometry']['coordinates'][0]]
                        station = item['stations'][1], item['stations'][0]
                        min_d = min([vincenty(point, station).meters for point in polygon])
                        if min_d < closest:
                            closest = min_d
                    except:
                        pass
            LandmarksHubway.append({'_id': key,
                                    'properties': item['properties'],
                                    'geometry': item['geometry'],
                                    'distance': closest})

        repo.dropCollection("LandmarksHubway")
        repo.createCollection("LandmarksHubway")
        repo['echogu_wei0496.LandmarksHubway'].insert_many(LandmarksHubway)
        repo['echogu_wei0496.LandmarksHubway'].metadata({'complete': True})
        print("Saved LandmarksSubway", repo['echogu_wei0496.LandmarksHubway'].metadata())

        repo.logout()

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

mergeLandmarksHubway.execute()
doc = mergeLandmarksHubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof