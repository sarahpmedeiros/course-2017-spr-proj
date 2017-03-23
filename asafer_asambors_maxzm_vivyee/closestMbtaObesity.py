import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import math

class closestMbtaObesity(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.obesity', 'asafer_asambors_maxzm_vivyee.mbta_routes']
    writes = ['asafer_asambors_maxzm_vivyee.obesity_mbta']

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
        obesity, stop = info
        obesity_lat = float(obesity['geolocation']['latitude'])
        obesity_lon = float(obesity['geolocation']['longitude'])

        stop_lat = float(stop['stop_lat'])
        stop_lon = float(stop['stop_lon'])

        # formula from: http://andrew.hedges.name/experiments/haversine/
        # used R = 3961 miles
        dlon = obesity_lon - stop_lon
        dlat = obesity_lat - stop_lat
        a = math.sin(dlat/2)**2 + (math.cos(stop_lat) * math.cos(obesity_lat) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = 3961 * c
        return (obesity, (stop, d))

    @staticmethod
    def close_stop(info):
        return info[1][1] <= 1

    @staticmethod
    def convert_to_dictionary(info):
        return {'obesity': info[0], 'stops': info[1]}

    @staticmethod
    def get_stops(info):
        stops = []
        for i in info['path']['direction']:
            for stop in i['stop']:
                stop['mode'] = info['mode']

            stops += i['stop']
        return stops

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')

        #loads
        obesity = repo['asafer_asambors_maxzm_vivyee.obesity']
        mbta_routes = repo['asafer_asambors_maxzm_vivyee.mbta_routes']

        repo.dropCollection('asafer_asambors_maxzm_vivyee.obesity_mbta')
        repo.createCollection('asafer_asambors_maxzm_vivyee.obesity_mbta')

        # get all stops by location
        stops = closestMbtaObesity.project(mbta_routes.find(), closestMbtaObesity.get_stops)
        all_stops = []
        for stop in stops:
            all_stops += stop

        # map all stops with all locations
        all_combos = closestMbtaObesity.product(obesity.find(), all_stops)

        # calculate distance for obesity b/w every stop
        distances = closestMbtaObesity.project(all_combos, closestMbtaObesity.calculate_distance)

        # find all places within a mile
        filtered_stops = closestMbtaObesity.select(distances, closestMbtaObesity.close_stop)

        # convert to dictionary format
        stops_by_location_dict = closestMbtaObesity.project(filtered_stops, closestMbtaObesity.convert_to_dictionary)

        repo['asafer_asambors_maxzm_vivyee.obesity_mbta'].insert_many(stops_by_location_dict)
        repo['asafer_asambors_maxzm_vivyee.obesity_mbta'].metadata({'complete': True})

        print('all uploaded')

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

        this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#obesity_mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # datamechanics.io data
        get_obesity_mbta = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_obesity_mbta, this_script)

        obesity = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity', {prov.model.PROV_LABEL:'Obesity Among Adults', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_routes = doc.entity('dat:asafer_asambors_maxzm_vivyee#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})

        obesity_mbta = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity_mbta', {prov.model.PROV_LABEL:'Closest MBTA stops to Obese areas', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(get_obesity_mbta, obesity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_obesity_mbta, mbta_routes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAttributedTo(obesity_mbta, this_script)
        doc.wasGeneratedBy(obesity_mbta, get_obesity_mbta, endTime)
        doc.wasDerivedFrom(obesity_mbta, obesity, get_obesity_mbta, get_obesity_mbta, get_obesity_mbta)
        doc.wasDerivedFrom(obesity_mbta, mbta_routes, get_obesity_mbta, get_obesity_mbta, get_obesity_mbta)

        repo.logout()

        return doc
        






