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
    writes = ['cfortuna_houset_karamy_snjan19.WazeTrafficData','cfortuna_houset_karamy_snjan19.BostonHospitalsData','cfortuna_houset_karamy_snjan19.BostonStreetsData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_houset_karamy_snjan19', 'cfortuna_houset_karamy_snjan19')

        ###### Importing Datasets and putting them inside the mongoDB database #####

        # # Waze Traffic Data
        # url = 'http://data.cityofboston.gov/resource/dih6-az4h.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("WazeTrafficData")
        # repo.createCollection("WazeTrafficData")
        # repo['cfortuna_houset_karamy_snjan19.WazeTrafficData'].insert_many(r)

        # Boston Hospitals
        url = 'http://data.cityofboston.gov/resource/u6fv-m8v4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonHospitalsData")
        repo.createCollection("BostonHospitalsData")
        repo['cfortuna_houset_karamy_snjan19.BostonHospitalsData'].insert_many(r)

        # # Streets of Boston
        # url = 'http://data.mass.gov/resource/ms23-5ubn.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("BostonStreetsData")
        # repo.createCollection("BostonStreetsData")
        # repo['cfortuna_houset_karamy_snjan19.BostonStreetsData'].insert_many(r)

        # Car Crashes
        url = 'http://datamechanics.io/data/cfortuna_houset_karamy_snjan19/CarCrashData.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("CarCrashData")
        repo.createCollection("CarCrashData")
        repo['cfortuna_houset_karamy_snjan19.CarCrashData'].insert_many(r)

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mag', 'https://data.mass.gov/resource/')
        doc.add_namespace('car', 'http://datamechanics.io/data/cfortuna_houset_karamy_snjan19/')

        this_script = doc.agent('alg:cfortuna_houset_karamy_snjan19#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #wazeResource = doc.entity('bdp:dih6-az4h', {'prov:label':'Waze Traffic Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        hospitalsResource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Boston Hospitals Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #streetResource = doc.entity('mag:ms23-5ubn', {'prov:label':'Boston Streets Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        carCrashResource = doc.entity('car:CarCrashData', {'prov:label':'Car Crash Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #getWazeTrafficData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        getBostonHospitalsData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #getBostonStreetsData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        getCarCrashData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        #doc.wasAssociatedWith(getWazeTrafficData, this_script)
        doc.wasAssociatedWith(getBostonHospitalsData, this_script)
        #doc.wasAssociatedWith(getBostonStreetsData, this_script)
        doc.wasAssociatedWith(getCarCrashData, this_script)
        
        # doc.usage(getWazeTrafficData, wazeResource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           #'ont:Query':'?type=Neighborhood+Area+Boston'
        #           }
        #           )
        doc.usage(getBostonHospitalsData, hospitalsResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  #'ont:Query':'?type=Neighborhood+Area+Cambridge'
                  }
                  )
        # doc.usage(getBostonStreetsData, streetResource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           #'ont:Query':'?type=Neighborhood+Area+Cambridge'
        #           }
        #          )
        doc.usage(getCarCrashData, carCrashResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  #'ont:Query':'?type=Neighborhood+Area+Cambridge'
                  }
                  )

        # WazeTrafficData = doc.entity('dat:cfortuna_houset_karamy_snjan19#WazeTrafficData', {prov.model.PROV_LABEL:'Waze Traffic Data', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(WazeTrafficData, this_script)
        # doc.wasGeneratedBy(WazeTrafficData, getWazeTrafficData, endTime)
        # doc.wasDerivedFrom(WazeTrafficData, wazeResource, getWazeTrafficData, getWazeTrafficData, getWazeTrafficData)

        BostonHospitalsData = doc.entity('dat:cfortuna_houset_karamy_snjan19#BostonHospitalsData', {prov.model.PROV_LABEL:'Boston Hospitals Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonHospitalsData, this_script)
        doc.wasGeneratedBy(BostonHospitalsData, getBostonHospitalsData, endTime)
        doc.wasDerivedFrom(BostonHospitalsData, hospitalsResource, getBostonHospitalsData, getBostonHospitalsData, getBostonHospitalsData)

        # BostonStreetsData = doc.entity('dat:cfortuna_houset_karamy_snjan19#BostonStreetsData', {prov.model.PROV_LABEL:'Boston Streets Data', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(BostonStreetsData, this_script)
        # doc.wasGeneratedBy(BostonStreetsData, getBostonStreetsData, endTime)
        # doc.wasDerivedFrom(BostonStreetsData, streetResource, getBostonStreetsData, getBostonStreetsData, getBostonStreetsData)

        CarCrashData = doc.entity('dat:cfortuna_houset_karamy_snjan19#CarCrashData', {prov.model.PROV_LABEL:'Car Crash Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CarCrashData, this_script)
        doc.wasGeneratedBy(CarCrashData, getCarCrashData, endTime)
        doc.wasDerivedFrom(CarCrashData, carCrashResource, getCarCrashData, getCarCrashData, getCarCrashData)

        repo.logout()
                  
        return doc

retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
