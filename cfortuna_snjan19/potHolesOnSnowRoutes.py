import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import json
import requests 

class potHolesOnSnowRoutes(dml.Algorithm):
    contributor = 'cfortuna_snjan19'
    reads = ['cfortuna_snjan19.SnowRoutes', 'cfortuna_snjan19.PotHoles']
    writes = ['cfortuna_snjan19.PotHolesOnSnowRoutes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        def intersect(R, S):
            return [t for t in R if t in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k,v) in R if k == key])) for key in keys]

        def removeDuplicates(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if not (x in seen or seen_add(x)) and x != " "]

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_snjan19', 'cfortuna_snjan19')

        # Read the datasets from Mongo
        repo.dropCollection("PotHolesOnSnowRoutes")
        repo.createCollection("PotHolesOnSnowRoutes")

        snowRoutes = repo['cfortuna_snjan19.SnowRoutes'].find()
        potholes = repo['cfortuna_snjan19.PotHoles'].find()
   
        ##### Perform transformations here #####

        # Turns the datasets into lists
        snowData = []
        for element in snowRoutes:
            snowData.append(element["properties"]["FULL_NAME"])

        snowData = removeDuplicates(snowData)
        
        potHoleData = []
        for element in potholes:
            if "location_street_name" in element:
                potHoleData.append(element["location_street_name"])

        # Performs an intersection of the two datasets
        potHoleSnowRoute = []
        for snowRoute in snowData:
            for potholeRoute in potHoleData:
                if snowRoute in potholeRoute and snowRoute != " ":
                    potHoleSnowRoute.append(snowRoute)

        # Determines the number of potholes on each route
        counts = []
        for route in potHoleSnowRoute:
            counts.append((potHoleSnowRoute.count(route), route))

        counts = removeDuplicates(counts)

        # Stores the transormed database
        result = []
        for route in counts:
            result.append( {'route': route[1], 'count': route[0]} )

        repo['cfortuna_snjan19.PotHolesOnSnowRoutes'].insert_many(result)

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

        this_script = doc.agent('alg:cfortuna_snjan19#potHolesOnSnowRoutes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Snow_resource = doc.entity('bod:4f3e4492e36f4907bcd307b131afe4a5_0.geojson',{'prov:label':'Snow Emergency Route, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        PotHoles_resource = doc.entity('bdp:n65p-xaz7',{'prov:label':'PotHoles, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_Snow = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_PotHoles = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_Snow, this_script)
        doc.wasAssociatedWith(get_PotHoles, this_script)

        doc.usage(get_Snow,Snow_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Snow+Emergency+Route'})
        doc.usage(get_PotHoles,PotHoles_resource,startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Requests+for+Pothole+Repair'})

        Snow = doc.entity('dat:cfortuna_snjan19#Snow', {prov.model.PROV_LABEL:'Snow Emergency Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Snow, this_script)
        doc.wasGeneratedBy(Snow, get_Snow, endTime)
        doc.wasDerivedFrom(Snow, Snow_resource, get_Snow, get_Snow, get_Snow)


        PotHoles = doc.entity('dat:cfortuna_snjan19#PotHoles', {prov.model.PROV_LABEL:'PotHoles Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(PotHoles, this_script)
        doc.wasGeneratedBy(PotHoles, get_PotHoles, endTime)
        doc.wasDerivedFrom(PotHoles, PotHoles_resource, get_PotHoles, get_PotHoles, get_PotHoles)

        repo.logout()
                  
        return doc

#potHolesOnSnowRoutes.execute()
#doc = retrieveData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof