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
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("SnowRoutes")
        repo.createCollection("SnowRoutes")
        repo['cfortuna_snjan19.SnowRoutes'].insert_many(r['features'])

        # Bike Networks
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BikeRoutes")
        repo.createCollection("BikeRoutes")
        repo['cfortuna_snjan19.BikeRoutes'].insert_many(r['features'])

        # Potholes
        url = 'http://data.cityofboston.gov/resource/n65p-xaz7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("PotHoles")
        repo.createCollection("PotHoles")
        repo['cfortuna_snjan19.PotHoles'].insert_many(r)

        # Streets of Boston
        url = 'http://data.mass.gov/resource/ms23-5ubn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Streets")
        repo.createCollection("Streets")
        repo['cfortuna_snjan19.Streets'].insert_many(r)

        # Parking Meters
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
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

        #ADD EXTRA DATA SOURCES
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/') # portal to city of Boston Data
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # Boston Open Data
        doc.add_namespace('dmg', 'https://data.mass.gov/resource/') #Portal to Data Mass Gov 

        this_script = doc.agent('alg:cfortuna_snjan19#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Bikes_resource = doc.entity('bod:d02c9d2003af455fbc37f550cc53d3a4_0.geojson',{'prov:label':'Existing Bike Network, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Snow_resource = doc.entity('bod:4f3e4492e36f4907bcd307b131afe4a5_0.geojson',{'prov:label':'Snow Emergency Route, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Meters_resource = doc.entity('bod:962da9bb739f440ba33e746661921244_9.geojson',{'prov:label':'Parking Meters, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        PotHoles_resource = doc.entity('bdp:n65p-xaz7',{'prov:label':'PotHoles, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Street_resource = doc.entity('dmg:ms23-5ubn.json',{'prov:label':'Street, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_Bikes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Snow = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Meters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_PotHoles = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Street = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_Bikes, this_script)
        doc.wasAssociatedWith(get_Snow, this_script)
        doc.wasAssociatedWith(get_Meters, this_script)
        doc.wasAssociatedWith(get_PotHoles, this_script)
        doc.wasAssociatedWith(get_Street, this_script)

        doc.usage(get_Bikes,Bikes_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Existing+Bike+Network'})
        doc.usage(get_Snow,Snow_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Snow+Emergency+Route'})
        doc.usage(get_Meters,Meters_resource,startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Parking+Meters'})
        doc.usage(get_PotHoles,PotHoles_resource,startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Requests+for+Pothole+Repair'})
        doc.usage(get_Street,Street_resource,startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Massachusetts+Detailed+Streets+with+Labels'})

        Bikes = doc.entity('dat:cfortuna_snjan19#Bikes', {prov.model.PROV_LABEL:'Biking Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Bikes, this_script)
        doc.wasGeneratedBy(Bikes, get_Bikes, endTime)
        doc.wasDerivedFrom(Bikes, Bikes_resource, get_Bikes, get_Bikes, get_Bikes)

        Snow = doc.entity('dat:cfortuna_snjan19#Snow', {prov.model.PROV_LABEL:'Snow Emergency Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Snow, this_script)
        doc.wasGeneratedBy(Snow, get_Snow, endTime)
        doc.wasDerivedFrom(Snow, Snow_resource, get_Snow, get_Snow, get_Snow)

        Meters = doc.entity('dat:cfortuna_snjan19#Meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Meters, this_script)
        doc.wasGeneratedBy(Meters, get_Meters, endTime)
        doc.wasDerivedFrom(Meters, Meters_resource, get_Meters, get_Meters, get_Meters)

        PotHoles = doc.entity('dat:cfortuna_snjan19#PotHoles', {prov.model.PROV_LABEL:'PotHoles Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(PotHoles, this_script)
        doc.wasGeneratedBy(PotHoles, get_PotHoles, endTime)
        doc.wasDerivedFrom(PotHoles, PotHoles_resource, get_PotHoles, get_PotHoles, get_PotHoles)

        Street = doc.entity('dat:cfortuna_snjan19#Street', {prov.model.PROV_LABEL:'Street Names', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Street, this_script)
        doc.wasGeneratedBy(Street, get_Street, endTime)
        doc.wasDerivedFrom(Street, Street_resource, get_Street, get_Street, get_Street)

        repo.logout()
                  
        return doc

#retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof