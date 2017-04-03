# optimizeBusRoute.py

# import urllib.request
# import json
# import numpy
import math
import random
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty
from heapq import heappush, heappop
from echogu_wei0496_wuhaoyu import transformData

class optimizeBusRoute(dml.Algorithm):
    contributor = 'echogu_wei0496_wuhaoyu'
    reads = ['echogu_wei0496_wuhaoyu.assigned_students']
    writes = ['echogu_wei0496_wuhaoyu.bus_routes']

    @staticmethod
    def execute(trial = False):
        ''' optimize school bus route
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496_wuhaoyu', 'echogu_wei0496_wuhaoyu')

        # loads the collection
        raw_assigned_students = repo['echogu_wei0496_wuhaoyu.assigned_students'].find()
        assigned_students = []
        for item in raw_assigned_students:
            try:
                assigned_students.append({'Aggregated_Points': item['Aggregated_Points'],
                                 'Points': item['Points']})
            except:
                pass

        # fixied the capacity of the buses, and send the students' locations
        # as coordinates and convert them to a graph, find the MST.
        # First, group the student belongs to the same school
        # projection_students = transformData.project(assigned_students, lambda t: (t['Assigned School'], [t['Latitude'], t['Longitude'], t['_id']]))
        result = optimizeBusRoute.__find_mst(assigned_students)

        repo.dropCollection('echogu_wei0496_wuhaoyu.final_route')
        repo.createCollection('echogu_wei0496_wuhaoyu.final_route')
        for i in result:
            print(i)
            repo['echogu_wei0496_wuhaoyu.final_route'].insert_one(i)
        repo['echogu_wei0496_wuhaoyu.final_route'].metadata({'complete': True})
        print(repo['echogu_wei0496_wuhaoyu.final_route'].metadata())
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        ''' Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496_wuhaoyu', 'echogu_wei0496_wuhaoyu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:echogu_wei0496_wuhaoyu#mergeLandmarksHubway',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_BLCLandmarks = doc.entity('dat:echogu_wei0496_wuhaoyu#BLCLandmarks',
                                           {'prov:label': 'BLC Landmarks', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_HubwayStations = doc.entity('dat:echogu_wei0496_wuhaoyu#HubwayStations',
                                             {'prov:label': 'Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_LandmarksHubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_LandmarksHubway, this_script)
        doc.usage(get_LandmarksHubway, resource_BLCLandmarks, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_LandmarksHubway, resource_HubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        LandmarksHubway = doc.entity('dat:echogu_wei0496_wuhaoyu#LandmarksHubway',
                                     {prov.model.PROV_LABEL: 'Landmarks Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(LandmarksHubway, this_script)
        doc.wasGeneratedBy(LandmarksHubway, get_LandmarksHubway, endTime)
        doc.wasDerivedFrom(LandmarksHubway, resource_BLCLandmarks, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)
        doc.wasDerivedFrom(LandmarksHubway, resource_HubwayStations, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)

        repo.logout()

        return doc

    # This is the helper function to find MST among all student pick up points
    # input: [coordinates] restricted by n(bus capacity, this is the maximum size of the tree)

    # convert the original input into format
    @staticmethod
    def __find_mst(assigned_students):
        final_res = []
        for i in assigned_students:
            Points = i['Points']
            K_points = i['Aggregated_Points'][0]
            final = []
            results = []
            for j in Points:
                results.append([j["Latitude"],j["Longitude"],j['Student_id']])
            #print("printing results")
            #print(results)
            res = optimizeBusRoute.__cal_MST(results)
            for k in res[0]:
                final.append({
                    'Student_id': results[k][2],
                    'Latitude': results[k][0],
                    'Longitude': results[k][1]})

            final_res.append({
                'Aggregated_Points':K_points,
                'Pickup_sequence': final})
        return final_res

    @staticmethod
    def __cal_MST(points):
        # construct a adjacency matrix
        # issue: when capacity contains only one student, there is a bug'''
        if(len(points) != 1):
            # construct a adjacency matrix
            adjacency_matrix = optimizeBusRoute.__generate_graph(points)
            result = optimizeBusRoute.__Prim(adjacency_matrix)
        return result

    # Initialization the adjacency matrix for the tree
    @staticmethod
    def __generate_graph(points):
        # initialize the adjacency matrix
        adjacency_matrix = [[100 for x in range(len(points))] for y in range(len(points))]
        for i in range(len(points)-1):
            for j in range(i+1, len(points)):
                adjacency_matrix[i][j] = optimizeBusRoute.distance(points[i][0:2], points[j][0:2])
                adjacency_matrix[j][i] = adjacency_matrix[i][j]
        return adjacency_matrix

    # Run MST Prim's Algorithm
    # Takes the random graph G as input and return the route and
    # the total weight of the MST
    @staticmethod
    def __Prim(G):
        # print(G)
        # Initialize the final weight to 0
        result = 0
        # Initialize keys of all vertices as infinite
        adjacency_matrix = [[2] * len(G)] * len(G)
        # Initialize an empty priority queue
        heap = []
        # Initialize an empty set of explored nodes S
        S = []
        # Insert source vertex into priority queue with key 0
        heappush(heap, [0, 1])
        # Initialize an empty array which each index equals 1 if the node has been explored and 0 if not
        Lookup = [0] * len(G)
        # Insert remaining vertex into priority queue with infinity key
        for i in range(1, len(G)):
            heappush(heap, [2, i])

        while sum(Lookup) != len(G):
            u = heappop(heap)

            # ignore all subsequent duplicates
            if Lookup[u[1]] == 0:
            # if u[1] not in S:
                S += [u[1]]
                Lookup[u[1]] = 1
                result += u[0]
                # for each edge e = (u,v)
                for v in range(len(G)):
                    # since in assumption it is a complete graph, we do not have to check if an edge exists
                    #if v not in S:
                    if Lookup[v] == 0:
                        # Since it's not efficient to lookup and modify existing tuples in the heap
                        # We can just insert the new tuple, upon removal we still have the lowest edge
                        heappush(heap,[G[u[1]][v],v])
        return S, result

    @staticmethod
    def distance(point1, point2):
        return vincenty(point1, point2).miles
