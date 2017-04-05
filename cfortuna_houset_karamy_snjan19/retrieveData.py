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
    contributor = 'cfortuna_houset_karamy_snjan19'
    reads = []
    writes = ['cfortuna_houset_karamy_snjan19.SnowRoutes','cfortuna_houset_karamy_snjan19.BikeRoutes','cfortuna_houset_karamy_snjan19.PotHoles','cfortuna_houset_karamy_snjan19.Streets', 'cfortuna_houset_karamy_snjan19.ParkingMeters']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_houset_karamy_snjan19', 'cfortuna_houset_karamy_snjan19')

        ###### Importing Datasets and putting them inside the mongoDB database #####
        # Waze Traffic Data
        url = 'https://data.cityofboston.gov/resource/dih6-az4h.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("WazeTrafficData")
        repo.createCollection("WazeTrafficData")
        repo['cfortuna_houset_karamy_snjan19.WazeTrafficData'].insert_many(r['features'])

        # Boston Hospitals
        url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonHospitals")
        repo.createCollection("BostonHospitals")
        repo['cfortuna_houset_karamy_snjan19.BostonHospitals'].insert_many(r['features'])

        # Potholes
        url = 'http://data.cityofboston.gov/resource/n65p-xaz7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("PotHoles")
        repo.createCollection("PotHoles")
        repo['cfortuna_houset_karamy_snjan19.PotHoles'].insert_many(r)

        # Streets of Boston
        url = 'http://data.mass.gov/resource/ms23-5ubn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Streets")
        repo.createCollection("Streets")
        repo['cfortuna_houset_karamy_snjan19.Streets'].insert_many(r)

        # Parking Meters
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("ParkingMeters")
        repo.createCollection("ParkingMeters")
        repo['cfortuna_houset_karamy_snjan19.ParkingMeters'].insert_many(r['features'])

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
        repo.authenticate('cfortuna_houset_karamy_snjan19', 'cfortuna_houset_karamy_snjan19')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('acw', 'cfortuna_houset_karamy_snjan19')

        this_script = doc.agent('alg:cfortuna_houset_karamy_snjan19#append_polygon_and_centerpoint', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_area_boston_res = doc.entity('acw:neighborhood_area_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_area_cambridge_res = doc.entity('acw:neighborhood_area_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_area_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood_area_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_area_boston, this_script)
        doc.wasAssociatedWith(get_neighborhood_area_cambridge, this_script)
        
        doc.usage(get_neighborhood_area_boston, neighborhood_area_boston_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Boston'
                  }
                  )
        doc.usage(get_neighborhood_area_cambridge, neighborhood_area_cambridge_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Cambridge'
                  }
                  )

        neighborhood_info = doc.entity('dat:cfortuna_houset_karamy_snjan19#neighborhood_info', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_info, this_script)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_boston, endTime)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_cambridge, endTime)
        doc.wasDerivedFrom(neighborhood_info, neighborhood_area_boston_res, get_neighborhood_area_boston, get_neighborhood_area_boston, get_neighborhood_area_boston)
        doc.wasDerivedFrom(neighborhood_info, neighborhood_area_cambridge_res, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge)

        repo.logout()
                  
        return doc

#retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
