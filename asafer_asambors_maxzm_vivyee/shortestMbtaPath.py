import dml
import prov.model
import datetime
import networkx as nx
import uuid
import math
import sys
from requests import request as rq

class shortestMbtaPath(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.health_obesity', 'asafer_asambors_maxzm_vivyee.mbta_routes']
    writes = ['asafer_asambors_maxzm_vivyee.health_obesity', 'asafer_asambors_maxzm_vivyee.obesity_time']

    @staticmethod
    def project(R, p, G):
        # return [p(R[0])] + [R[i] for i in range(1,len(R))]
        return [p(t, G) for t in R]

    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def get_closest_path(info, G):
        obesity_stops = [ (stop['stop_id'], stop['rect_lat'], stop['rect_lon']) for stop in info['obesity_locations']['stops'] if stop['mode'] == 'Subway' ]
        healthy_stops = [ (stop['stop_id'], stop['rect_lat'], stop['rect_lon']) for stop in info['healthy_locations']['stops'] if stop['mode'] == 'Subway' ]
        
        origin_long = info['obesity_locations']['obesity']['geolocation']['rect_lon']
        origin_lat = info['obesity_locations']['obesity']['geolocation']['rect_lat']

        dest_lat = info['healthy_locations']['healthy_locations']['rect_location'][0]
        dest_long = info['healthy_locations']['healthy_locations']['rect_location'][1]

        google_key = dml.auth['services']['googlemaps']['key']

        base_link = "https://maps.googleapis.com/maps/api/directions/json?" 
        origin = "origin=" + str(origin_lat) + "," + str(origin_long) 
        destination = "&destination=" + str(dest_lat) + "," + str(dest_long) 
        bus_mode = "&mode=transit&transit_mode=bus"
        walk_mode = "&mode=walking"
        api_key = "&key=" + google_key

        # print('obesity stops length:', len(obesity_stops), 'healthy stops length:', len(obesity_stops))
        min_times = []
        for o_stop, o_stop_lat, o_stop_long in obesity_stops:
            for h_stop, h_stop_lat, h_stop_long in healthy_stops:
                try:
                    time = nx.dijkstra_path_length(G, o_stop, h_stop)

                    # Find walking distance/duration from start to o_stop
                    o_stop_coords = "&destination=" + str(o_stop_lat) + "," + str(o_stop_long)
                    walking_to_url = base_link + origin + o_stop_coords + walk_mode + api_key
                    response = rq(method="GET", url=walking_to_url)

                    raw = response.json()
                    total_added_time_for_walking = 0 

                    for route in raw['routes']:
                        tots = 0
                        for leg in route['legs']:
                            time_in_seconds = leg['duration']['value']
                            tots += time_in_seconds

                        tots /= 60.0
                        total_added_time_for_walking += tots
 

                    # Find walking distance/duration from h_stop to stop
                    h_stop_coords = "origin=" + str(h_stop_lat) + "," + str(h_stop_long)
                    walking_from_url = base_link + h_stop_coords + destination + walk_mode + api_key
                    response = rq(method="GET", url=walking_from_url)
                    raw = response.json()

                    # print("RESPONSE TEXT IS {}".format(response.text))
                    for route in raw['routes']:
                        tots = 0
                        for leg in route['legs']:
                            time_in_seconds = leg['duration']['value']
                            tots += time_in_seconds

                        tots /= 60.0
                        total_added_time_for_walking += tots
                        
                    # print('TIME IS {}'.format(time + total_added_time_for_walking))
                    min_times.append(time + total_added_time_for_walking)
                except nx.NetworkXNoPath: 
                    pass


        # obesity_bus_stops = [ stop['stop_id'] for stop in info['obesity_locations']['stops'] if stop['mode'] != 'Subway' ]
        # healthy_bus_stops = [ stop['stop_id'] for stop in info['healthy_locations']['stops'] if stop['mode'] != 'Subway' ]

        # print("Origin lat long {} {} and destination lat long {} {}".format(origin_lat, origin_long, dest_lat, dest_long))

        bus_link = base_link + origin + destination + bus_mode + api_key
        response = rq(method="GET", url=bus_link)
        raw_json = response.json()

        for route in raw_json['routes']:
            sum = 0
            for leg in route['legs']:
                time_in_seconds = leg['duration']['value']
                sum += time_in_seconds

            sum /= 60.0
            min_times.append(sum) 

        # # print("Origin lat long {} {} and destination lat long {} {}".format(origin_lat, origin_long, dest_lat, dest_long))
 
        if len(min_times) == 0:
            info['min_travel_time'] = sys.maxsize
        else:
            info['min_travel_time'] = min(min_times)
            # print("MIN TRAVEL TIME {}".format(info['min_travel_time'])) 
        
        # print(min_times)
        # print('info is\n' + str(info))
        return info

    @staticmethod
    def get_tuples(info, G):
        data = eval(info['obesity_locations']['obesity']['data_value'])
        time = info['min_travel_time']

        return {'data_value': data, 'time': time}


    @staticmethod
    def calculate_distance(lat_1, lon_1, lat_2, lon_2):
        # formula from: http://andrew.hedges.name/experiments/haversine/
        # used R = 3961 miles
        R = 3961.0
        dlon = lon_1 - lon_2
        dlat = lat_1 - lat_2
        a = math.sin(dlat/2)**2 + (math.cos(lat_2) * math.cos(lat_1) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')
        
        mbta_routes = repo['asafer_asambors_maxzm_vivyee.mbta_routes'].find()
        health_obesity = repo['asafer_asambors_maxzm_vivyee.health_obesity'].find()

        G = nx.DiGraph()

        transfers = {
            'Park Street': [],
            'Boyleston': [],
            'Government Center': [],
            'Haymarket': [],
            'North Station': [],
            'State Street': [],
            'Downtown Crossing': [],
            'Chinatown': [],
            'Tufts Medical Center': [],
            'South Station': []
        }

        # Add edges, will create nodes u and v if not already in the graph
        # weight = distance / mpm = total time it takes from point 1 to 2
        for route in mbta_routes:
            if route['mode'] == 'Subway':
                mpm = 30.0 / 60.0          # miles per minute

                for direction in route['path']['direction']:
                    prev_lat = 0
                    prev_lon = 0
                    prev_stop = ''
                    # print(direction['stop'])

                    for i in range(len(direction['stop'])):
                        stop = direction['stop'][i]

                        # find all transfer stations
                        for key in transfers.keys():
                            if key == stop['stop_name'][:len(key)]:
                                # add edge for transfer
                                for t in transfers[key]:
                                    G.add_edge(t, stop['stop_id'], weight=10)
                                    G.add_edge(stop['stop_id'], t, weight=10)

                                transfers[key].append(stop['stop_id'])

                        if i > 0:
                            d = shortestMbtaPath.calculate_distance(prev_lat, prev_lon, eval(stop['stop_lat']), eval(stop['stop_lon']))
                            w = d / mpm
                            G.add_edge(prev_stop, stop['stop_id'], weight=w)
                            G.add_edge(stop['stop_id'], prev_stop, weight=w)

                            # if w == 0:
                            #     print(stop['stop_id'], 'to', prev_stop)
                            # print('current stop:', stop['stop_id'], '; last_stop:', prev_stop, '; weight:', w)
                        prev_lon = eval(stop['stop_lon'])
                        prev_lat = eval(stop['stop_lat'])
                        prev_stop = stop['stop_id']
        
        # project

        health_obesity_times = shortestMbtaPath.project(health_obesity, shortestMbtaPath.get_closest_path, G)
        health_obesity_times_tuples = shortestMbtaPath.select(health_obesity_times, lambda x: 'data_value' in x['obesity_locations']['obesity'] and x['min_travel_time'] != sys.maxsize)
        health_obesity_times_tuples = shortestMbtaPath.project(health_obesity_times_tuples, shortestMbtaPath.get_tuples, G)
        # nx.dijkstra_path_length(G, source, target)
        repo.dropCollection('asafer_asambors_maxzm_vivyee.health_obesity')
        repo.createCollection('asafer_asambors_maxzm_vivyee.health_obesity')

        repo['asafer_asambors_maxzm_vivyee.health_obesity'].insert_many(health_obesity_times)
        repo['asafer_asambors_maxzm_vivyee.health_obesity'].metadata({'complete': True})

        repo.dropCollection('asafer_asambors_maxzm_vivyee.obesity_time')
        repo.createCollection('asafer_asambors_maxzm_vivyee.obesity_time')

        repo['asafer_asambors_maxzm_vivyee.obesity_time'].insert_many(health_obesity_times_tuples)
        repo['asafer_asambors_maxzm_vivyee.obesity_time'].metadata({'complete': True})

        print('all uploaded: shortestMbtaPath')

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

        this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#shortestMbtaPath', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_shortest_mbta_path = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_obesity_time = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_shortest_mbta_path, this_script)
        doc.wasAssociatedWith(get_obesity_time, this_script)

        health_obesity = doc.entity('dat:asafer_asambors_maxzm_vivyee#health_obesity', {prov.model.PROV_LABEL:'Closest healthy location to an obese area', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_routes = doc.entity('dat:asafer_asambors_maxzm_vivyee#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        obesity_time = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity_time', {prov.model.PROV_LABEL:'Time to get to a healthy location from an obese area (percentage)', prov.model.PROV_TYPE:'ont:DataSet'}) 

        doc.usage(get_shortest_mbta_path, health_obesity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_shortest_mbta_path, mbta_routes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_obesity_time, health_obesity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_obesity_time, mbta_routes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAttributedTo(health_obesity, this_script)
        doc.wasAttributedTo(obesity_time, this_script)

        doc.wasGeneratedBy(health_obesity, get_shortest_mbta_path, endTime)
        doc.wasGeneratedBy(obesity_time, get_obesity_time, endTime)

        doc.wasDerivedFrom(health_obesity, health_obesity, get_shortest_mbta_path, get_shortest_mbta_path, get_shortest_mbta_path)
        doc.wasDerivedFrom(health_obesity, mbta_routes, get_shortest_mbta_path, get_shortest_mbta_path, get_shortest_mbta_path)

        doc.wasDerivedFrom(obesity_time, health_obesity, get_obesity_time, get_obesity_time, get_obesity_time)

        repo.logout()

        return doc


shortestMbtaPath.execute()
