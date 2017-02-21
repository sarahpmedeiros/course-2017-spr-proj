# mergeLandmarksHubway.py
# merge the landmark data set and the Hubway stations data set to determine how many existing Hubway stations
# is within a historic landmark

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from echogu_wei0496 import transformData

class mergeLandmarksHubway(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.BLCLandmarks', 'echogu_wei0496.HubwayStations']
    writes = ['echogu_wei0496.LandmarksHubway']

    @staticmethod
    def execute(trial=False):
        ''' Merge some datasets.
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # loads the collection
        rawLandmarks = repo['echogu_wei0496.BLCLandmarks']
        rawHubwayStations = repo['echogu_wei0496.HubwayStations']

        # projection
        Landmarks = [{'_id': item['_id'], 'properties': item['properties'], 'geometry': item['geometry']['coordinates']} for item in rawLandmarks.find()]
        HubwayStations = [{'coordinates': item['geometry']['coordinates']} for item in rawHubwayStations.find()]

        # Aggregation sum
        product = transformData.product(Landmarks, HubwayStations)
        product = [{'_id': item[0]['_id'],
                    'properties': item[0]['properties'],
                    'geometry': item[0]['geometry'],
                    'point': item[1]['coordinates']} for item in product]
        # select = []
        # for item in product:
        #     polygon = item[0]['geometry']
        #     lowerleft = polygon[0]
        #     lowerright = polygon[1]
        #     upper
        #     point = item[1]['coordinates']
        #     if point[x] < polygon[1][2] and >



        # repo.dropCollection('LandmarksHubway')
        # repo.createCollection('LandmarksHubway').insert_many(P)



        # url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("lost")
        # repo.createCollection("lost")
        # repo['alice_bob.lost'].insert_many(r)
        # repo['alice_bob.lost'].metadata({'complete': True})
        # print(repo['alice_bob.lost'].metadata())
        #
        # url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("found")
        # repo.createCollection("found")
        # repo['alice_bob.found'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        lost = doc.entity('dat:alice_bob#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

        return doc


mergeLandmarksHubway.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
