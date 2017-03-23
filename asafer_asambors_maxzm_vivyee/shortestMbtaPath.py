import dml
import prov.model
import datetime
import networkx as nx
import uuid
import math

class shortestMbtaPath(dml.Algorithm):
    contributor = 'asafer_asambors_maxzm_vivyee'
    reads = ['asafer_asambors_maxzm_vivyee.health_mbta', 'asafer_asambors_maxzm_vivyee.obesity_mbta', 'asafer_asambors_maxzm_vivyee.mbta_routes']
    writes = ['asafer_asambors_maxzm_vivyee.shortest_mbta_paths']

    @staticmethod
    def calculate_distance(lat_1, lon_1, lat_2, lon_2):
        # formula from: http://andrew.hedges.name/experiments/haversine/
        # used R = 3961 miles
        dlon = lon_1 - lon_2
        dlat = lat_1 - lat_2
        a = math.sin(dlat/2)**2 + (math.cos(lat_2) * math.cos(lat_1) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = 3961 * c
        return d

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #set up the datebase connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')
        
        mbta_routes = repo['asafer_asambors_maxzm_vivyee.mbta_routes'].find()

        repo.dropCollection('asafer_asambors_maxzm_vivyee.shortest_mbta_paths')
        repo.createCollection('asafer_asambors_maxzm_vivyee.shortest_mbta_paths')

        G = nx.DiGraph()

        # Add edges, will create nodes u and v if not already in the graph
        # weight = distance / mpm = total time it takes from point 1 to 2
        for route in mbta_routes:
            if route['mode'] == 'Subway':
                mpm = 30 / 60          # miles per minute
                for direction in route['path']['direction']:
                    prev_lat = 0
                    prev_lon = 0
                    prev_stop = ''

                    for i in range(len(direction['stop'])):
                        stop = direction['stop'][i]
                        if i > 0:
                            d = shortestMbtaPath.calculate_distance(prev_lat, prev_lon, eval(stop['stop_lat']), eval(stop['stop_lon']))
                            w = d / mpm
                            G.add_edge(prev_stop, stop['stop_id'], weight=w)
                            # print('current stop:', stop['stop_id'], '; last_stop:', prev_stop, '; weight:', w)
                        prev_lon = eval(stop['stop_lon'])
                        prev_lat = eval(stop['stop_lat'])
                        prev_stop = stop['stop_id']

                    break



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

        repo.logout()

        return doc

shortestMbtaPath.execute()