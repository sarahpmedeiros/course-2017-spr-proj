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

        # select data with location fields
        orchard_locs = healthyLocations.select(orchards.find(), lambda x: 'map_locations' in x and 'coordinates' in x['map_locations'])
        corner_stores_locs = healthyLocations.select(corner_stores.find(), lambda x: 'location' in x and 'coordinates' in x['location'])
        nutrition_prog_locs = healthyLocations.select(nutrition_prog.find(), lambda x: 'location' in x and 'coordinates' in x['location'])


        # reformat data using project
        orchard_locs = healthyLocations.project(orchard_locs, lambda x: {'type': 'orchard', 'location': x['map_locations']['coordinates']})
        corner_stores_locs = healthyLocations.project(corner_stores_locs, lambda x: {'type': 'store', 'location': x['location']['coordinates']})
        nutrition_prog_locs = healthyLocations.project(nutrition_prog_locs, lambda x: {'type': 'prog', 'location': x['location']['coordinates']})

        all_locs = orchard_locs + corner_stores_locs + nutrition_prog_locs

        repo['asafer_vivyee.healthy_locations'].insert_many(all_locs)
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
        






