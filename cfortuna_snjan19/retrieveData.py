import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import json
import requests 

class retrieveData(dml.Algorithm):
    contributor = 'cfortuna_snjan19'
    reads = []
    writes = ['cfortuna_snjan19.SnowRoutes','cfortuna_snjan19.BikeRoutes','cfortuna_snjan19.PotHoles','cfortuna_snjan19.Streets', 'cfortuna_snjan19.ParkingMeters']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_snjan19', 'cfortuna_snjan19')

        ###### Importing Datasets and putting them inside the mongoDB database #####

        # Snow emergency routes
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #for element in r['features']:
        #    del element['geometry']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("SnowRoutes")
        repo.createCollection("SnowRoutes")
        repo['cfortuna_snjan19.SnowRoutes'].insert_many(r['features'])
        #repo['cfortuna_snjan19.SnowRoutes'].metadeta({'complete':True})
        #print(repo['cfortuna_snjan19.SnowRoutes'].metadeta())

        # Bike Networks
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)
        repo.dropCollection("BikeRoutes")
        repo.createCollection("BikeRoutes")
        repo['cfortuna_snjan19.BikeRoutes'].insert_many(r['features'])

        # Potholes
        url = 'http://data.cityofboston.gov/resource/n65p-xaz7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)
        repo.dropCollection("PotHoles")
        repo.createCollection("PotHoles")
        repo['cfortuna_snjan19.PotHoles'].insert_many(r)

        # Streets of Boston
        url = 'http://data.mass.gov/resource/ms23-5ubn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)
        repo.dropCollection("Streets")
        repo.createCollection("Streets")
        repo['cfortuna_snjan19.Streets'].insert_many(r)

        # Parking Meters
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)
        repo.dropCollection("ParkingMeters")
        repo.createCollection("ParkingMeters")
        repo['cfortuna_snjan19.ParkingMeters'].insert_many(r['features'])

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_snjan19', 'cfortuna_snjan19')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

retrieveData.execute()
#doc = retrieveData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof