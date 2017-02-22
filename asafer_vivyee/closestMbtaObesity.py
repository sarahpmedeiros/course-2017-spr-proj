import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import math

class closestMbtaObesity(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = ['asafer_vivyee.obesity', 'asafer_vivyee.mbta_routes']
    writes = ['asafer_vivyee.obesity_mbta']

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
            stops += i['stop']
        return stops

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee','asafer_vivyee')

        #loads
        obesity = repo['asafer_vivyee.obesity']
        mbta_routes = repo['asafer_vivyee.mbta_routes']

        repo.dropCollection('asafer_vivyee.obesity_mbta')
        repo.createCollection('asafer_vivyee.obesity_mbta')

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

        # aggregate stops by location they're close to
        stops_by_location = closestMbtaObesity.aggregate(filtered_stops, lambda x: x)

        # convert to dictionary format
        stops_by_location_dict = closestMbtaObesity.project(stops_by_location, closestMbtaObesity.convert_to_dictionary)

        repo['asafer_vivyee.obesity_mbta'].insert_many(stops_by_location_dict)
        repo['asafer_vivyee.obesity_mbta'].metadata({'complete': True})

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

        this_script = doc.agent('alg:asafer_vivyee#obesity_mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # datamechanics.io data
        obesity_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'Obesity Among Adults', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        mbta_routes_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'MBTA Routes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        obesity_mbta_resource = doc.entity('dat:asafer_vivyee', {'prov:label': 'Closest MBTA stops to Obese areas', prov.model.PROV_TYPE:'ont:DataResource'})

        get_obesity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime) # LOL
        get_mbta_routes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_obesity_mbta = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_obesity, this_script)
        doc.wasAssociatedWith(get_mbta_routes, this_script)
        doc.wasAssociatedWith(get_obesity_mbta, this_script)

        doc.usage(get_obesity, obesity_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_mbta_routes, mbta_routes_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_obesity_mbta, obesity_mbta_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        obesity = doc.entity('dat:asafer_vivyee#obesity', {prov.model.PROV_LABEL:'Obesity Among Adults', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesity, this_script)
        doc.wasGeneratedBy(obesity, get_obesity, endTime)
        doc.wasDerivedFrom(obesity, obesity_resource, get_obesity, get_obesity, get_obesity)

        mbta_routes = doc.entity('dat:asafer_vivyee#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_routes, this_script)
        doc.wasGeneratedBy(mbta_routes, get_mbta_routes, endTime)
        doc.wasDerivedFrom(mbta_routes, mbta_routes_resource, get_mbta_routes, get_mbta_routes, get_mbta_routes)

        obesity_mbta = doc.entity('dat:asafer_vivyee#obesity_mbta', {prov.model.PROV_LABEL:'Closest MBTA stops to Obese areas', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesity_mbta, this_script)
        doc.wasGeneratedBy(obesity_mbta, get_obesity_mbta, endTime)
        doc.wasDerivedFrom(obesity_mbta, obesity_mbta_resource, get_obesity_mbta, get_obesity_mbta, get_obesity_mbta)

        repo.logout()

        return doc


closestMbtaObesity.execute()
doc = closestMbtaObesity.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
        






