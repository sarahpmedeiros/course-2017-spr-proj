import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import json
import requests 

class potHolesOnBikeRoutes(dml.Algorithm):
    contributor = 'cfortuna_snjan19'
    reads = ['cfortuna_snjan19.BikeRoutes', 'cfortuna_snjan19.PotHoles']
    writes = ['cfortuna_snjan19.PotHolesOnBikeRoutes']

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

        ###### Importing Datasets and putting them inside the mongoDB database #####

        repo.dropCollection("PotHolesOnBikeRoutes")
        repo.createCollection("PotHolesOnBikeRoutes")

        bikeRoutes = repo['cfortuna_snjan19.BikeRoutes'].find()
        potholes = repo['cfortuna_snjan19.PotHoles'].find()
   
        ##### Perform transformations here #####

        # Need to shorten street to st, etc
        bikeData = []
        for element in bikeRoutes:
            route = element["properties"]["STREET_NAM"]
            if "Avenue" in route:
                route = route[:-3]
            elif "Street" in route:
                route = route[:-4]
            elif "Drive" in route:
                route = route[:-3]
            elif "Road" in route:
                route = route[:-3] + "d"
            elif "Highway" in route:
                route = route[:-6] + "wy"
            elif "Boulevard" in route:
                route = route[:-8] + "lvd"

            bikeData.append(route)
        
        bikeData = removeDuplicates(bikeData)

        potHoleData = []
        for element in potholes:
            if "location_street_name" in element:
                potHoleData.append(element["location_street_name"])

        potHoleBikeRoute = []
        for bikeRoute in bikeData:
            for potholeRoute in potHoleData:
                if bikeRoute in potholeRoute and bikeRoute != " ":
                    potHoleBikeRoute.append(bikeRoute)

        counts = []
        for route in potHoleBikeRoute:
            counts.append((route, potHoleBikeRoute.count(route)))

        counts = removeDuplicates(counts)

        result = []
        for route in counts:
            result.append( {'route': route[0], 'count': route[1]} )

        repo['cfortuna_snjan19.PotHolesOnBikeRoutes'].insert_many(result)

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


        this_script = doc.agent('alg:cfortuna_snjan19#potHolesOnBikeRoutes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Bikes_resource = doc.entity('bod:d02c9d2003af455fbc37f550cc53d3a4_0.geojson',{'prov:label':'Existing Bike Network, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        PotHoles_resource = doc.entity('bdp:n65p-xaz7',{'prov:label':'PotHoles, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_Bikes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_PotHoles = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_Bikes, this_script)
        doc.wasAssociatedWith(get_PotHoles, this_script)

        doc.usage(get_Bikes,Bikes_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Existing+Bike+Network'})
        doc.usage(get_PotHoles,PotHoles_resource,startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?type=Requests+for+Pothole+Repair'})


        Bikes = doc.entity('dat:cfortuna_snjan19#Bikes', {prov.model.PROV_LABEL:'Biking Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Bikes, this_script)
        doc.wasGeneratedBy(Bikes, get_Bikes, endTime)
        doc.wasDerivedFrom(Bikes, Bikes_resource, get_Bikes, get_Bikes, get_Bikes)

        PotHoles = doc.entity('dat:cfortuna_snjan19#PotHoles', {prov.model.PROV_LABEL:'PotHoles Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(PotHoles, this_script)
        doc.wasGeneratedBy(PotHoles, get_PotHoles, endTime)
        doc.wasDerivedFrom(PotHoles, PotHoles_resource, get_PotHoles, get_PotHoles, get_PotHoles)

        repo.logout()
                  
        return doc

#potHolesOnBikeRoutes.execute()
#doc = retrieveData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof