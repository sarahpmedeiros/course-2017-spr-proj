import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import math

class closestHealthObesity(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.health_mbta', 'asafer_asambors_maxzm_vivyee.obesity_mbta']
    writes = ['asafer_asambors_maxzm_vivyee.health_obesity']

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
        healthy, obesity = info
        healthy_lat = float(healthy['healthy_locations']['location'][0])
        healthy_lon = float(healthy['healthy_locations']['location'][1])

        obesity_lat = float(obesity['obesity']['geolocation']['latitude'])
        obesity_lon = float(obesity['obesity']['geolocation']['longitude'])

        # formula from: http://andrew.hedges.name/experiments/haversine/
        # used R = 3961 miles
        dlon = healthy_lon - obesity_lon
        dlat = healthy_lat - obesity_lat
        a = math.sin(dlat/2)**2 + (math.cos(obesity_lat) * math.cos(healthy_lat) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = 3961 * c
        return (obesity, (healthy, d))

    @staticmethod
    def closest(info):
        closest_health = min(info, key = lambda t: t[1])
        return closest_health

    @staticmethod
    def convert_to_dictionary(info):
            return {'obesity_locations': info[0], 'healthy_locations': info[1]}

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')

        #loads
        #healthy_locations = repo['asafer_asambors_maxzm_vivyee.healthy_locations']
        #obesity = repo['asafer_asambors_maxzm_vivyee.obesity']
        health_mbta = repo['asafer_asambors_maxzm_vivyee.health_mbta']
        obesity_mbta = repo['asafer_asambors_maxzm_vivyee.obesity_mbta']

        repo.dropCollection('asafer_asambors_maxzm_vivyee.health_obesity')
        repo.createCollection('asafer_asambors_maxzm_vivyee.health_obesity')

        # map all obesity locations with all healthy locations

        #all_combos = closestHealthObesity.product(healthy_locations.find(), obesity.find())
        all_combos = closestHealthObesity.product(health_mbta.find(), obesity_mbta.find())

        # calculate distance for healthy loc b/w every obesity location
        distances = closestHealthObesity.project(all_combos, closestHealthObesity.calculate_distance)

        # for each obesity location, keep only the closest healthy location
        obesity_by_closest = closestHealthObesity.aggregate(distances, closestHealthObesity.closest)

        # convert to dictionary format
        obesity_by_closest_dict = closestHealthObesity.project(obesity_by_closest, closestHealthObesity.convert_to_dictionary)

        repo['asafer_asambors_maxzm_vivyee.health_obesity'].insert_many(obesity_by_closest_dict)
        repo['asafer_asambors_maxzm_vivyee.health_obesity'].metadata({'complete': True})

        print('all uploaded')

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

        repo.logout()

        return doc
        



