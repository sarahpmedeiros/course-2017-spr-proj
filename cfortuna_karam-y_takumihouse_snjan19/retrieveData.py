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
    contributor = 'cfortuna_karam-y_takumihouse_snjan19'
    reads = []
    writes = ['cfortuna_karam-y_takumihouse_snjan19.SnowRoutes','cfortuna_karam-y_takumihouse_snjan19.BikeRoutes','cfortuna_karam-y_takumihouse_snjan19.PotHoles','cfortuna_karam-y_takumihouse_snjan19.Streets', 'cfortuna_karam-y_takumihouse_snjan19.ParkingMeters']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_karam-y_takumihouse_snjan19', 'cfortuna_karam-y_takumihouse_snjan19')

        ###### Importing Datasets and putting them inside the mongoDB database #####

        # Snow emergency routes
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("SnowRoutes")
        repo.createCollection("SnowRoutes")
        repo['cfortuna_karam-y_takumihouse_snjan19.SnowRoutes'].insert_many(r['features'])

        # Bike Networks
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BikeRoutes")
        repo.createCollection("BikeRoutes")
        repo['cfortuna_karam-y_takumihouse_snjan19.BikeRoutes'].insert_many(r['features'])

        # Potholes
        url = 'http://data.cityofboston.gov/resource/n65p-xaz7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("PotHoles")
        repo.createCollection("PotHoles")
        repo['cfortuna_karam-y_takumihouse_snjan19.PotHoles'].insert_many(r)

        # Streets of Boston
        url = 'http://data.mass.gov/resource/ms23-5ubn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Streets")
        repo.createCollection("Streets")
        repo['cfortuna_karam-y_takumihouse_snjan19.Streets'].insert_many(r)

        # Parking Meters
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("ParkingMeters")
        repo.createCollection("ParkingMeters")
        repo['cfortuna_karam-y_takumihouse_snjan19.ParkingMeters'].insert_many(r['features'])

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
        repo.authenticate('cfortuna_karam-y_takumihouse_snjan19', 'cfortuna_karam-y_takumihouse_snjan19')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('acw', 'cfortuna_karam-y_takumihouse_snjan19')

        this_script = doc.agent('alg:cfortuna_karam-y_takumihouse_snjan19#aggregate_sea_level_rise', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_info_res = doc.entity('acw:neighborhood_info', {'prov:label':'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        sea_level_five_res = doc.entity('acw:sea_level_five', {'prov:label':'Sea Level Rise Plus 5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        sea_level_seven_res = doc.entity('acw:sea_level_seven', {'prov:label':'Sea Level Rise Plus 7.5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_info = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_sea_level_five = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_sea_level_seven = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_info, this_script)
        doc.wasAssociatedWith(get_sea_level_five, this_script)
        doc.wasAssociatedWith(get_sea_level_seven, this_script)

        doc.usage(get_neighborhood_info, neighborhood_info_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Info'
                  }
                  )
        doc.usage(get_sea_level_five, sea_level_five_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Sea+Level+Five'
                  }
                  )
        doc.usage(get_sea_level_seven, sea_level_seven_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Sea+Level+Seven'
                  }
                  )

        neighborhood_sea_level_data = doc.entity('cfortuna_karam-y_takumihouse_snjan19#neighborhood_sea_level_data', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Sea Level Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_sea_level_data, this_script)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_neighborhood_info, endTime)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_sea_level_five, endTime)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_sea_level_seven, endTime)
        doc.wasDerivedFrom(neighborhood_sea_level_data, neighborhood_info_res, get_neighborhood_info, get_neighborhood_info, get_neighborhood_info)
        doc.wasDerivedFrom(neighborhood_sea_level_data, sea_level_five_res, get_sea_level_five, get_sea_level_five, get_sea_level_five)
        doc.wasDerivedFrom(neighborhood_sea_level_data, sea_level_seven_res, get_sea_level_seven, get_sea_level_seven, get_sea_level_seven)


        repo.logout()
                  
        return doc

#retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
