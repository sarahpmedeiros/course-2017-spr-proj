import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class healthyLocations(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.orchards', 'asafer_asambors_maxzm_vivyee.corner_stores', 'asafer_asambors_maxzm_vivyee.nutrition_prog']
    writes = ['asafer_asambors_maxzm_vivyee.healthy_locations']

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
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')

        #loads
        orchards = repo['asafer_asambors_maxzm_vivyee.orchards']
        corner_stores = repo['asafer_asambors_maxzm_vivyee.corner_stores']
        nutrition_prog = repo['asafer_asambors_maxzm_vivyee.nutrition_prog']


        repo.dropCollection('asafer_asambors_maxzm_vivyee.healthy_locations')
        repo.createCollection('asafer_asambors_maxzm_vivyee.healthy_locations')

        # select data with location fields
        orchard_locs = healthyLocations.select(orchards.find(), lambda x: 'map_locations' in x and 'coordinates' in x['map_locations'])
        corner_stores_locs = healthyLocations.select(corner_stores.find(), lambda x: 'location' in x and 'coordinates' in x['location'])
        nutrition_prog_locs = healthyLocations.select(nutrition_prog.find(), lambda x: 'location' in x and 'coordinates' in x['location'])


        # reformat data using project
        orchard_locs = healthyLocations.project(orchard_locs, lambda x: {'type': 'orchard', 'location': x['map_locations']['coordinates']})
        corner_stores_locs = healthyLocations.project(corner_stores_locs, lambda x: {'type': 'store', 'location': x['location']['coordinates']})
        nutrition_prog_locs = healthyLocations.project(nutrition_prog_locs, lambda x: {'type': 'prog', 'location': x['location']['coordinates']})

        all_locs = orchard_locs + corner_stores_locs + nutrition_prog_locs

        repo['asafer_asambors_maxzm_vivyee.healthy_locations'].insert_many(all_locs)
        repo['asafer_asambors_maxzm_vivyee.healthy_locations'].metadata({'complete': True})

        print('all uploaded: healthyLocations')

        endTime = datetime.datetime.now

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee', 'asafer_asambors_maxzm_vivyee')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#healthyLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # datamechanics.io data
        get_healthy_locations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_healthy_locations, this_script)

        orchards = doc.entity('dat:asafer_asambors_maxzm_vivyee#orchards', {prov.model.PROV_LABEL:'Urban Orchard Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        corner_stores = doc.entity('dat:asafer_asambors_maxzm_vivyee#corner_stores', {prov.model.PROV_LABEL:'Healthy Corner Store Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        nutrition_prog = doc.entity('dat:asafer_asambors_maxzm_vivyee#nutrition_prog', {prov.model.PROV_LABEL:'Community Culinary and Nutrition Programs', prov.model.PROV_TYPE:'ont:DataSet'})
        
        healthy_locations = doc.entity('dat:asafer_asambors_maxzm_vivyee#healthy_locations', {prov.model.PROV_LABEL:'Aggregate of Health related locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(get_healthy_locations, orchards, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_healthy_locations, corner_stores, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_healthy_locations, nutrition_prog, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAttributedTo(healthy_locations, this_script)
        doc.wasGeneratedBy(healthy_locations, get_healthy_locations, endTime)
        doc.wasDerivedFrom(healthy_locations, orchards, get_healthy_locations, get_healthy_locations, get_healthy_locations)
        doc.wasDerivedFrom(healthy_locations, corner_stores, get_healthy_locations, get_healthy_locations, get_healthy_locations)
        doc.wasDerivedFrom(healthy_locations, nutrition_prog, get_healthy_locations, get_healthy_locations, get_healthy_locations)

        repo.logout()

        return doc
        






