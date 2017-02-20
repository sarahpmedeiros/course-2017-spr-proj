import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class retrieveData(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = []
    writes = ['echogu_wei0496.BikeNetwork', 'echogu_wei0496.HubwayStations', 'echogu_wei0496.ServicesRequest', 'echogu_wei0496.BLCLandmarks', 'echogu_wei0496.MASchools']

    @staticmethod
    def execute(trial=False):
        ''' Retrieve some data sets (not using the API here for the sake of simplicity).
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # City of Boston Boundary
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/142500a77e2a4dbeb94a86f7e0b568bc_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("CityBoundary")
        repo.createCollection("CityBoundary")
        repo['echogu_wei0496.CityBoundary'].insert_one(r)
        repo['echogu_wei0496.CityBoundary'].metadata({'complete': True})
        print(repo['echogu_wei0496.CityBoundary'].metadata())
        print("Retrieved CityBoundary", repo['echogu_wei0496.BikeNetwork'].metadata())

        # Existing Bike Network
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("BikeNetwork")
        repo.createCollection("BikeNetwork")
        repo['echogu_wei0496.BikeNetwork'].insert_one(r)
        repo['echogu_wei0496.BikeNetwork'].metadata({'complete': True})
        print("Retrieved BikeNetwork", repo['echogu_wei0496.BikeNetwork'].metadata())

        # Hubway Stations
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("HubwayStations")
        repo.createCollection("HubwayStations")
        repo['echogu_wei0496.HubwayStations'].insert_one(r)
        repo['echogu_wei0496.HubwayStations'].metadata({'complete': True})
        print("Retrieved HubwayStations", repo['echogu_wei0496.HubwayStations'].metadata())

        # 311 Open Services Request Map (JSON)
        client = sodapy.Socrata("data.cityofboston.gov", dml.auth['services']['cityofbostondataportal']['token'])
        r = client.get("j2a7-cdyk", limit=100, REASON="Boston Bikes")
        repo.dropCollection("ServicesRequest")
        repo.createCollection("ServicesRequest")
        repo['echogu_wei0496.ServicesRequest'].insert_many(r)
        repo['echogu_wei0496.ServicesRequest'].metadata({'complete': True})
        print("Retrieved ServicesRequest", repo['echogu_wei0496.ServicesRequest'].metadata())

        # Boston Landmarks Commission (BLC) Landmarks
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/7a7aca614ad740e99b060e0ee787a228_3.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("BLCLandmarks")
        repo.createCollection("BLCLandmarks")
        repo['echogu_wei0496.BLCLandmarks'].insert_one(r)
        repo['echogu_wei0496.BLCLandmarks'].metadata({'complete': True})
        print("Retrieved BLCLandmarks", repo['echogu_wei0496.BLCLandmarks'].metadata())

        # MA Schools (Pre-K through High School)
        url = 'http://maps-massgis.opendata.arcgis.com/datasets/359016a59a594e0088547235913c9168_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("MASchools")
        repo.createCollection("MASchools")
        repo['echogu_wei0496.MASchools'].insert_one(r)
        repo['echogu_wei0496.MASchools'].metadata({'complete': True})
        print("Retrieved MASchools", repo['echogu_wei0496.MASchools'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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

        # create document object and define namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # Boston Maps Open Data
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # City Of Boston Data Portal
        doc.add_namespace('mod', 'http://maps-massgis.opendata.arcgis.com/datasets/') #MassGIS Open Data

        # define entity to represent resources
        this_script = doc.agent('alg:echogu_wei0496#retrieveData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_CityBoundary = doc.entity('bod:142500a77e2a4dbeb94a86f7e0b568bc_0',
                                          {'prov:label': 'Boston City Boundary',
                                           prov.model.PROV_TYPE: 'ont:DataResource',
                                           'ont:Extension': 'geojson'})
        resource_BikeNetwork = doc.entity('bod:d02c9d2003af455fbc37f550cc53d3a4_0',
                              {'prov:label': 'Existing Bike Networks', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'geojson'})
        resource_HubwayStations = doc.entity('bod:ee7474e2a0aa45cbbdfe0b747a5eb032_0',
                                          {'prov:label': 'Hubway Stations',
                                           prov.model.PROV_TYPE: 'ont:DataResource',
                                           'ont:Extension': 'geojson'})
        resource_ServicesRequest = doc.entity('bdp:j2a7-cdyk',
                              {'prov:label': '311 Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource_BLCLandmarks = doc.entity('bod:7a7aca614ad740e99b060e0ee787a228_3',
                                             {'prov:label': 'BLC Landmarks',
                                              prov.model.PROV_TYPE: 'ont:DataResource',
                                              'ont:Extension': 'geojson'})
        resource_MASchools = doc.entity('mod:359016a59a594e0088547235913c9168_0',
                                           {'prov:label': 'BLC Landmarks',
                                            prov.model.PROV_TYPE: 'ont:DataResource',
                                            'ont:Extension': 'geojson'})

        # define activity to represent invocation of the script
        get_CityBoundary = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_BikeNetwork = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_HubwayStations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_ServicesRequest = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_BLCLandmarks = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_MASchools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        # associate the activity with the script
        doc.wasAssociatedWith(get_CityBoundary, this_script)
        doc.wasAssociatedWith(get_BikeNetwork, this_script)
        doc.wasAssociatedWith(get_HubwayStations, this_script)
        doc.wasAssociatedWith(get_ServicesRequest, this_script)
        doc.wasAssociatedWith(get_BLCLandmarks, this_script)
        doc.wasAssociatedWith(get_MASchools, this_script)

        # indicate that an activity used the entity
        doc.usage(get_BikeNetwork, resource_CityBoundary, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   # 'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_BikeNetwork, resource_BikeNetwork, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   #'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_HubwayStations, resource_HubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   #'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_ServicesRequest, resource_ServicesRequest, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   # 'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_BLCLandmarks, resource_BLCLandmarks, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   # 'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_MASchools, resource_MASchools, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   # 'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        CityBoundary = doc.entity('dat:echogu_wei0496#CityBoundary', {prov.model.PROV_LABEL: 'Boston City Boundary', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(CityBoundary, this_script)
        doc.wasGeneratedBy(CityBoundary, get_CityBoundary, endTime)
        doc.wasDerivedFrom(CityBoundary, resource_CityBoundary, resource_CityBoundary, resource_BikeNetwork, get_CityBoundary)

        BikeNetworks = doc.entity('dat:echogu_wei0496#BikeNetworks', {prov.model.PROV_LABEL: 'Existing Bike Networks', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(BikeNetworks, this_script)
        doc.wasGeneratedBy(BikeNetworks, get_BikeNetwork, endTime)
        doc.wasDerivedFrom(BikeNetworks, resource_BikeNetwork, get_BikeNetwork, get_BikeNetwork, get_BikeNetwork)

        HubwayStations = doc.entity('dat:echogu_wei0496#HubwayStations', {prov.model.PROV_LABEL: 'Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(HubwayStations, this_script)
        doc.wasGeneratedBy(HubwayStations, get_HubwayStations, endTime)
        doc.wasDerivedFrom(HubwayStations, resource_HubwayStations, get_HubwayStations, get_HubwayStations, get_HubwayStations)

        ServicesRequest = doc.entity('dat:echogu_wei0496#ServicesRequest', {prov.model.PROV_LABEL: '311 Services Request', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ServicesRequest, this_script)
        doc.wasGeneratedBy(ServicesRequest, get_ServicesRequest, endTime)
        doc.wasDerivedFrom(ServicesRequest, resource_ServicesRequest, get_ServicesRequest, get_ServicesRequest, get_ServicesRequest)

        BLCLandmarks = doc.entity('dat:echogu_wei0496#BLCLandmarks', {prov.model.PROV_LABEL: 'BLC Landmarks', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(BLCLandmarks, this_script)
        doc.wasGeneratedBy(BLCLandmarks, get_BLCLandmarks, endTime)
        doc.wasDerivedFrom(BLCLandmarks, resource_BLCLandmarks, get_BLCLandmarks, get_BLCLandmarks, get_BLCLandmarks)

        MASchools = doc.entity('dat:echogu_wei0496#MASchools', {prov.model.PROV_LABEL: 'MA Schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(MASchools, this_script)
        doc.wasGeneratedBy(MASchools, get_MASchools, endTime)
        doc.wasDerivedFrom(MASchools, resource_MASchools, get_MASchools, get_MASchools, get_MASchools)

        repo.logout()

        return doc

retrieveData.execute()
doc = retrieveData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof