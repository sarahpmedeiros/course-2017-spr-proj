from urllib.request import urlopen, Request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class retrieveData(dml.Algorithm):
    contributor = 'alice_bob'
    reads = []
    writes = ['alice_bob.lost', 'alice_bob.found']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')

        # get Seasonal Swimming pools data
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("xw3e-c7pz", limit=10)
        print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("seasonalSwimPools")
        repo.createCollection("seasonalSwimPools")
        repo['alice_bob.seasonalSwimPools'].insert_many(r)
        repo['alice_bob.seasonalSwimPools'].metadata({'complete': True})
        print(repo['alice_bob.seasonalSwimPools'].metadata())

        # get community garden data
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("rdqf-ter7", limit=10)
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("communityGardens")
        repo.createCollection("communityGardens")
        repo['alice_bob.communityGardens'].insert_many(r)
        repo['alice_bob.communityGardens'].metadata({'complete': True})
        print(repo['alice_bob.communityGardens'].metadata())

        # get recreational open space data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas", limit=10)
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("RECREATION_OpenSpace")
        repo.createCollection("RECREATION_OpenSpace")
        repo['alice_bob.RECREATION_OpenSpace'].insert_many(r)
        repo['alice_bob.RECREATION_OpenSpace'].metadata({'complete': True})
        print(repo['alice_bob.RECREATION_OpenSpace'].metadata())

        # get recreational waterplay parks data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas", limit=10)
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("RECREATION_Waterplay")
        repo.createCollection("RECREATION_Waterplay")
        repo['alice_bob.RECREATION_Waterplay'].insert_many(r)
        repo['alice_bob.RECREATION_Waterplay'].metadata({'complete': True})
        print(repo['alice_bob.RECREATION_Waterplay'].metadata())


        # url = 'https://data.cityofboston.gov/resource/xw3e-c7pz.json'
        # response = urlopen(url).read().decode("utf-8")
        # print(json.dumps(json.loads(response), sort_keys=True, indent=2))
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


retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

