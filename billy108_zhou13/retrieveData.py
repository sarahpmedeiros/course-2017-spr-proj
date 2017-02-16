from urllib.request import urlopen, Request
import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class retrieveData(dml.Algorithm):
    contributor = 'billy108_zhou13'
    reads = []
    writes = ['billy108_zhou13.seasonalSwimPools', 'billy108_zhou13.communityGardens',
              'billy108_zhou13.openSpaceCambridge','billy108_zhou13.waterplayCambridge',
              'billy108_zhou13.openSpaceBoston', 'billy108_zhou13.commCenterPools']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')

        # get Seasonal Swimming pools data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("xw3e-c7pz")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("seasonalSwimPools")
        repo.createCollection("seasonalSwimPools")
        repo['billy108_zhou13.seasonalSwimPools'].insert_many(r)
        repo['billy108_zhou13.seasonalSwimPools'].metadata({'complete': True})
        print(repo['billy108_zhou13.seasonalSwimPools'].metadata())

        # get Community Gardens data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("rdqf-ter7")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("communityGardens")
        repo.createCollection("communityGardens")
        repo['billy108_zhou13.communityGardens'].insert_many(r)
        repo['billy108_zhou13.communityGardens'].metadata({'complete': True})
        print(repo['billy108_zhou13.communityGardens'].metadata())

        # get recreational open space data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("openSpaceCambridge")
        repo.createCollection("openSpaceCambridge")
        repo['billy108_zhou13.openSpaceCambridge'].insert_many(r)
        repo['billy108_zhou13.openSpaceCambridge'].metadata({'complete': True})
        print(repo['billy108_zhou13.openSpaceCambridge'].metadata())

        # get recreational waterplay parks data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("hv2t-vv6d")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("waterplayCambridge")
        repo.createCollection("waterplayCambridge")
        repo['billy108_zhou13.waterplayCambridge'].insert_many(r)
        repo['billy108_zhou13.waterplayCambridge'].metadata({'complete': True})
        print(repo['billy108_zhou13.waterplayCambridge'].metadata())

        # Get data of Open spaces of conservation and recreation interest in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        # print(json.dumps(json.loads(response), sort_keys=True, indent=2))
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("openSpaceBoston")
        repo.createCollection("openSpaceBoston")
        repo['billy108_zhou13.openSpaceBoston'].insert_many(r['features'])
        repo['billy108_zhou13.openSpaceBoston'].metadata({'complete': True})
        print(repo['billy108_zhou13.openSpaceBoston'].metadata())

        # Get data of Community Center Pools in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5575f763dbb64effa36acd67085ef3a8_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("commCenterPools")
        repo.createCollection("commCenterPools")
        repo['billy108_zhou13.commCenterPools'].insert_many(r['features'])
        repo['billy108_zhou13.commCenterPools'].metadata({'complete': True})
        print(repo['billy108_zhou13.commCenterPools'].metadata())

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
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:billy108_zhou13#example',
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

        lost = doc.entity('dat:billy108_zhou13#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:billy108_zhou13#found',
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

