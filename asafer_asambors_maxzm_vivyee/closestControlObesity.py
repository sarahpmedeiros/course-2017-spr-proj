import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import math
import numpy as np

class closestControlObesity(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.control_mbta', 'asafer_asambors_maxzm_vivyee.obesity_mbta']
    writes = ['asafer_asambors_maxzm_vivyee.control_obesity']

    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def aggregate(R, f):
        keys = [r[0] for r in R]
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def project(R, p):
        return [p(t) for t in R]

    @staticmethod
    def product(R, S):
        return [(t,u) for t in R for u in S]

    @staticmethod
    def calculate_distance(info):
        control, obesity = info
        print(control)
        control_lat = control['healthy_locations']['Location'][0]
        control_lon = control['healthy_locations']['Location'][1]

        obesity_lat = obesity['obesity']['geolocation']['latitude']
        obesity_lon = obesity['obesity']['geolocation']['longitude']

        # formula from: http://andrew.hedges.name/experiments/haversine/
        # used R = 3961 miles
        dlon = control_lon - obesity_lon
        dlat = control_lat - obesity_lat
        a = math.sin(dlat/2)**2 + (math.cos(obesity_lat) * math.cos(control_lat) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = 3961 * c
        return (obesity, (control, d))

    @staticmethod
    def closest(info):
        closest_control = min(info, key = lambda t: t[1])
        return closest_control

    @staticmethod
    def convert_to_dictionary(info):
        return {'obesity_locations': info[0], 'control_locations': info[1][0]}

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')

        #loads
        #control_locations = repo['asafer_asambors_maxzm_vivyee.control_locations']
        #obesity = repo['asafer_asambors_maxzm_vivyee.obesity']
        control_mbta = repo['asafer_asambors_maxzm_vivyee.control_mbta']
        obesity_mbta = repo['asafer_asambors_maxzm_vivyee.obesity_mbta']

        repo.dropCollection('asafer_asambors_maxzm_vivyee.control_obesity')
        repo.createCollection('asafer_asambors_maxzm_vivyee.control_obesity')

        # map all obesity locations with all control locations

        #all_combos = closestControlObesity.product(control_locations.find(), obesity.find())
        all_combos = closestControlObesity.product(control_mbta.find(), obesity_mbta.find())

        # calculate distance for control loc b/w every obesity location
        distances = closestControlObesity.project(all_combos, closestControlObesity.calculate_distance)

        # for each obesity location, keep only the closest control location
        obesity_by_closest = closestControlObesity.aggregate(distances, closestControlObesity.closest)

        # convert to dictionary format
        obesity_by_closest_dict = closestControlObesity.project(obesity_by_closest, closestControlObesity.convert_to_dictionary)

        repo['asafer_asambors_maxzm_vivyee.control_obesity'].insert_many(obesity_by_closest_dict)
        repo['asafer_asambors_maxzm_vivyee.control_obesity'].metadata({'complete': True})

        print('all uploaded: closestControlObesity')

        endTime = datetime.datetime.now

        return {"start":startTime, "end":endTime}

## not done yet from here ##
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

        this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#closestControlObesity', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_closest_control_obesity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_closest_control_obesity, this_script)

        control_mbta = doc.entity('dat:asafer_asambors_maxzm_vivyee#control_mbta', {prov.model.PROV_LABEL:'Closest MBTA stops to Control locations', prov.model.PROV_TYPE:'ont:DataSet'})
        obesity_mbta = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity_mbta', {prov.model.PROV_LABEL:'Closest MBTA stops to Obese areas', prov.model.PROV_TYPE:'ont:DataSet'})
        control_obesity = doc.entity('dat:asafer_asambors_maxzm_vivyee#control_obesity', {prov.model.PROV_LABEL:'Closest control location to an obese area', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(get_closest_control_obesity, control_mbta, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_closest_control_obesity, obesity_mbta, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAttributedTo(control_obesity, this_script)
        doc.wasGeneratedBy(control_obesity, get_closest_control_obesity, endTime)
        doc.wasDerivedFrom(control_obesity, control_mbta, get_closest_health_obesity, get_closest_health_obesity, get_closest_health_obesity)
        doc.wasDerivedFrom(control_obesity, obesity_mbta, get_closest_control_obesity, get_closest_health_obesity, get_closest_health_obesity)

        repo.logout()

        return doc
        

closestControlObesity.execute()

