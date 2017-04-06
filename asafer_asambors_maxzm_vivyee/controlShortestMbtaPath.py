import dml
import prov.model
import datetime
import networkx as nx
import uuid
import math
import sys

class controlShortestMbtaPath(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.control_obesity', 'asafer_asambors_maxzm_vivyee.mbta_routes']
    writes = ['asafer_asambors_maxzm_vivyee.control_obesity', 'asafer_asambors_maxzm_vivyee.control_time']

    @staticmethod
    def project(R, p, G):
        return [p(t, G) for t in R]

    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def get_closest_path(info, G):
        obesity_stops = [ stop['stop_id'] for stop in info['obesity_locations']['stops'] if stop['mode'] == 'Subway' ]
        control_stops = [ stop['stop_id'] for stop in info['control_locations']['stops'] if stop['mode'] == 'Subway' ]
        
        # print('obesity stops length:', len(obesity_stops), 'healthy stops length:', len(obesity_stops))

        min_times = []
        for o_stop in obesity_stops:
            for h_stop in control_stops:
                try:
                    time = nx.dijkstra_path_length(G, o_stop, h_stop)
                    min_times.append(time)
                except nx.NetworkXNoPath:
                    # print('no path found')
                    pass

        if len(min_times) == 0:
            info['min_travel_time'] = sys.maxsize
        else:
            info['min_travel_time'] = min(min_times)
            # print(info)
        # print(min_times)
        # print('info is\n' + str(info))
        return info\

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
        control_obesity = repo['asafer_asambors_maxzm_vivyee.control_obesity'].find()

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
                            d = controlShortestMbtaPath.calculate_distance(prev_lat, prev_lon, eval(stop['stop_lat']), eval(stop['stop_lon']))
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

        control_obesity_times = controlShortestMbtaPath.project(control_obesity, controlShortestMbtaPath.get_closest_path, G)
        control_obesity_times_tuples = controlShortestMbtaPath.select(control_obesity_times, lambda x: 'data_value' in x['obesity_locations']['obesity'])
        control_obesity_times_tuples = controlShortestMbtaPath.project(control_obesity_times_tuples, controlShortestMbtaPath.get_tuples, G)
        # nx.dijkstra_path_length(G, source, target)


        repo.dropCollection('asafer_asambors_maxzm_vivyee.control_time')
        repo.createCollection('asafer_asambors_maxzm_vivyee.control_time')

        repo['asafer_asambors_maxzm_vivyee.control_time'].insert_many(control_obesity_times_tuples)
        repo['asafer_asambors_maxzm_vivyee.control_time'].metadata({'complete': True})

        print('all uploaded: controlShortestMbtaPath')

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

        this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#controlShortestMbtaPath', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_control_shortest_mbta_path = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_control_obesity_time = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_control_shortest_mbta_path, this_script)
        doc.wasAssociatedWith(get_control_obesity_time, this_script)

        control_obesity = doc.entity('dat:asafer_asambors_maxzm_vivyee#control_obesity', {prov.model.PROV_LABEL:'Closest control to an obese area', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_routes = doc.entity('dat:asafer_asambors_maxzm_vivyee#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        control_time = doc.entity('dat:asafer_asambors_maxzm_vivyee#control_time', {prov.model.PROV_LABEL:'Time to get to a control location from an obese area (percentage)', prov.model.PROV_TYPE:'ont:DataSet'}) 

        doc.usage(get_control_shortest_mbta_path, control_obesity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_control_shortest_mbta_path, mbta_routes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_control_obesity_time, control_obesity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_control_obesity_time, mbta_routes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAttributedTo(control_obesity, this_script)
        doc.wasAttributedTo(control_time, this_script)

        doc.wasGeneratedBy(control_obesity, get_control_shortest_mbta_path, endTime)
        doc.wasGeneratedBy(control_time, get_control_obesity_time, endTime)

        doc.wasDerivedFrom(control_obesity, control_obesity, get_control_shortest_mbta_path, get_control_shortest_mbta_path, get_control_shortest_mbta_path)
        doc.wasDerivedFrom(control_obesity, mbta_routes, get_control_shortest_mbta_path, get_control_shortest_mbta_path, get_control_shortest_mbta_path)

        doc.wasDerivedFrom(control_time, control_obesity, get_control_obesity_time, get_control_obesity_time, get_control_obesity_time)

        repo.logout()

        return doc


# shortestMbtaPath.execute()

