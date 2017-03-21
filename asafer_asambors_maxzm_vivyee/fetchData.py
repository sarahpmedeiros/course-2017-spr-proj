import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class fetchData(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = []
    writes = ['asafer_vivyee.orchards', 'asafer_vivyee.corner_stores', 'asafer_vivyee.obesity', 'asafer_vivyee.nutrition_prog', 'asafer_vivyee.mbta_routes']

    @staticmethod
    def setup():
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')
        return repo

    @staticmethod
    def store(repo, url, collection):

        if (collection == 'asafer_vivyee.mbta_routes'):
            response = urllib.request.urlopen(url).read().decode("utf-8")
            response = json.loads(response)

            routes = [ mode for mode in response['mode'] if mode['mode_name'] == 'Subway' or mode['mode_name'] == 'Bus' ]
            routes = [ (mode['mode_name'], route['route_id']) for mode in routes for route in mode['route'] ]

            stop_url = 'http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=' + dml.auth['services']['mbtadeveloperportal']['key']
            stop_urls = {route:"{}&route={}&format=json".format(stop_url, route[1]) for route in routes}
            stop_responses = {route:urllib.request.urlopen(stop_urls[route]).read().decode("utf-8") for route in stop_urls}

            json_stops = []
            for route, response in stop_responses.items():
                stops_by_route = {}

                mode, route_id = route

                stops_by_route['name'] = route_id
                stops_by_route['mode'] = mode
                stops_by_route['path'] = json.loads(response)

                json_stops.append(stops_by_route)

            repo.dropPermanent(collection)
            repo.createPermanent(collection)
            repo[collection].insert_many(json_stops)

        elif (collection == 'asafer_vivyee.obesity'):
            response = requests.get(url)
            data = response.json()

            repo.dropPermanent(collection)
            repo.createPermanent(collection)
            repo[collection].insert_many(data)

        else:
            response = urllib.request.urlopen(url).read().decode("utf-8")
            response = json.loads(response)

            repo.dropPermanent(collection)
            repo.createPermanent(collection)
            repo[collection].insert_many(response)

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets'''
        startTime = datetime.datetime.now()

        repo = fetchData.setup()

        mbta_key = dml.auth['services']['mbtadeveloperportal']['key']
        cityofboston_token = dml.auth['services']['cityofbostondataportal']['token']

        datasets = {
            'asafer_vivyee.orchards': 'https://data.cityofboston.gov/resource/8tmm-wjbw.json?$$app_token=' + cityofboston_token,
            'asafer_vivyee.corner_stores': 'https://data.cityofboston.gov/resource/ybm6-m5qd.json?$$app_token=' + cityofboston_token,
            'asafer_vivyee.obesity': 'https://chronicdata.cdc.gov/resource/ahrt-wk9b.json?$offset=13908&$limit=177',
            'asafer_vivyee.nutrition_prog': 'https://data.cityofboston.gov/resource/ahjc-pw5e.json?$$app_token=' + cityofboston_token,
            'asafer_vivyee.mbta_routes': 'http://realtime.mbta.com/developer/api/v2/routes?api_key=' + mbta_key + '&format=json'
        }

        for collection, url in datasets.items():
            fetchData.store(repo, url, collection)

        repo.logout()

        endTime = datetime.datetime.now()

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/') # CDC API
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/r') # MBTA API

        this_script = doc.agent('alg:asafer_vivyee#data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        orchards_resource = doc.entity('bdp:8tmm-wjbw', {'prov:label': 'Urban Orchard Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        corner_stores_resource = doc.entity('bdp:ybm6-m5qd', {'prov:label': 'Healthy Corner Store Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        obesity_resource = doc.entity('cdc:a2ye-t2pa', {'prov:label': 'Obesity Among Adults', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        nutrition_prog_resource = doc.entity('bdp:ahjc-pw5e', {'prov:label': 'Community Culinary and Nutrition Programs', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension': 'json'})
        mbta_routes_resource = doc.entity('mbta:routes', {'prov:label': 'MBTA Routes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_orchards = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_corner_stores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_obesity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime) # LOL
        get_nutrition_prog = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_mbta_routes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_orchards, this_script)
        doc.wasAssociatedWith(get_corner_stores, this_script)
        doc.wasAssociatedWith(get_obesity, this_script)
        doc.wasAssociatedWith(get_nutrition_prog, this_script)
        doc.wasAssociatedWith(get_mbta_routes, this_script)

        doc.usage(get_orchards, orchards_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_corner_stores, corner_stores_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_obesity, obesity_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_nutrition_prog, nutrition_prog_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_mbta_routes, mbta_routes_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        orchards = doc.entity('dat:asafer_vivyee#orchards', {prov.model.PROV_LABEL:'Urban Orchard Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(orchards, this_script)
        doc.wasGeneratedBy(orchards, get_orchards, endTime)
        doc.wasDerivedFrom(orchards, orchards_resource, get_orchards, get_orchards, get_orchards)

        corner_stores = doc.entity('dat:asafer_vivyee#corner_stores', {prov.model.PROV_LABEL:'Healthy Corner Store Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corner_stores, this_script)
        doc.wasGeneratedBy(corner_stores, get_corner_stores, endTime)
        doc.wasDerivedFrom(corner_stores, corner_stores_resource, get_corner_stores, get_corner_stores, get_corner_stores)

        obesity = doc.entity('dat:asafer_vivyee#obesity', {prov.model.PROV_LABEL:'Obesity Among Adults', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesity, this_script)
        doc.wasGeneratedBy(obesity, get_obesity, endTime)
        doc.wasDerivedFrom(obesity, obesity_resource, get_obesity, get_obesity, get_obesity)

        nutrition_prog = doc.entity('dat:asafer_vivyee#nutrition_prog', {prov.model.PROV_LABEL:'Community Culinary and Nutrition Programs', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(nutrition_prog, this_script)
        doc.wasGeneratedBy(nutrition_prog, get_nutrition_prog, endTime)
        doc.wasDerivedFrom(nutrition_prog, nutrition_prog_resource, get_nutrition_prog, get_nutrition_prog, get_nutrition_prog)

        mbta_routes = doc.entity('dat:asafer_vivyee#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_routes, this_script)
        doc.wasGeneratedBy(mbta_routes, get_mbta_routes, endTime)
        doc.wasDerivedFrom(mbta_routes, mbta_routes_resource, get_mbta_routes, get_mbta_routes, get_mbta_routes)

        repo.logout()

        return doc

## eof