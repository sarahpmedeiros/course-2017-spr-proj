import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class healthyLocations(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = ['asafer_vivyee.orchards', 'asafer_vivyee.corner_stores', 'asafer_vivyee.nutrition_prog']
    writes = ['asafer_vivyee.healthy_locations']

    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def project(R, p):
        return [p(t) for t in R]

    @staticmethod
    def product(R, S):
        return [(t,u) for t in R for u in S]

    @staticmethod
    def location(loc):
        return {'location': [loc[1]['location'][0], loc[1]['location'][1]], 'type': loc[0]}

    @staticmethod
    def has_location(x):
        return 'location' in x
        # return True



    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee','asafer_vivyee')

        #loads
        orchards = repo['asafer_vivyee.orchards']
        corner_stores = repo['asafer_vivyee.corner_stores']
        nutrition_prog = repo['asafer_vivyee.nutrition_prog']


        repo.dropCollection('asafer_vivyee.healthy_locations')
        repo.createCollection('asafer_vivyee.healthy_locations')

        for i in orchards.find():
            if "location" in i:
                loc = i['location']
                area = {}
                area['orchard'] = loc
                repo['asafer_vivyee.healthy_locations'].insert(area)

        for i in corner_stores.find():
            if "location" in i:
                loc = i['location']
                area = {}
                area['store'] = loc
                repo['asafer_vivyee.healthy_locations'].insert(area)

        for i in nutrition_prog.find():
            if "location" in i:
                loc = i['location']
                area = {}
                area['prog'] = loc
                repo['asafer_vivyee.healthy_locations'].insert(area)        

        # repo['asafer_vivyee.healthy_locations'].insert_many(locations)
        repo['asafer_vivyee.healthy_locations'].metadata({'complete': True})

        print('all uploaded')

        endTime = datetime.datetime.now

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:asafer_vivyee#healthyLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # datamechanics.io data
        orchards_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'Urban Orchard Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        corner_stores_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'Healthy Corner Store Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        nutrition_prog_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'Community Culinary and Nutrition Programs', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_orchards = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_corner_stores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_nutrition_prog = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_orchards, this_script)
        doc.wasAssociatedWith(get_corner_stores, this_script)
        doc.wasAssociatedWith(get_nutrition_prog, this_script)

        doc.usage(get_orchards, orchards_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_corner_stores, corner_stores_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_nutrition_prog, nutrition_prog_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        orchards = doc.entity('dat:asafer_vivyee#orchards', {prov.model.PROV_LABEL:'Urban Orchard Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(orchards, this_script)
        doc.wasGeneratedBy(orchards, get_orchards, endTime)
        doc.wasDerivedFrom(orchards, orchards_resource, get_orchards, get_orchards, get_orchards)

        corner_stores = doc.entity('dat:asafer_vivyee#corner_stores', {prov.model.PROV_LABEL:'Healthy Corner Store Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corner_stores, this_script)
        doc.wasGeneratedBy(corner_stores, get_corner_stores, endTime)
        doc.wasDerivedFrom(corner_stores, corner_stores_resource, get_corner_stores, get_corner_stores, get_corner_stores)

        nutrition_prog = doc.entity('dat:asafer_vivyee#nutrition_prog', {prov.model.PROV_LABEL:'Community Culinary and Nutrition Programs', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(nutrition_prog, this_script)
        doc.wasGeneratedBy(nutrition_prog, get_nutrition_prog, endTime)
        doc.wasDerivedFrom(nutrition_prog, nutrition_prog_resource, get_nutrition_prog, get_nutrition_prog, get_nutrition_prog)

        repo.logout()

        return doc


healthyLocations.execute()
doc = healthyLocations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
        






