# Assign_Students.py

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

class assignStudents(dml.Algorithm):
    contributor = 'echogu_wei0496_wuhaoyu'
    reads = ['echogu_wei0496_wuhaoyu.bus_yards', 'echogu_wei0496_wuhaoyu.students', 'echogu_wei0496_wuhaoyu.schools']
    writes = ['echogu_wei0496_wuhaoyu.bus_routes']

    # set the bus capacity
    global bus_capacity
    bus_capacity = 20

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
        raw_students = repo['echogu_wei0496_wuhaoyu.students'].find()
        students = []
        for item in raw_students:
            try:
                students.append({'_id': item['_id'],
                                  'Latitude': item['Latitude'],
                                  'Longitude': item['Longitude'],
                                  'Assigned School': item['Assigned School']})
            except:
                pass

        # group the student belongs to the same school
        project_students = transformData.project(students, lambda t: (t['school'], [t['_id'], [t['latitude'], t['longitude']]]))
        group_students = transformData.aggregate(project_students, assignStudents.porj_students)

        results = []
        for i in group_students[0:10]:
            school = i[0]
            num_students = len(i[1][0])
            num_buses = math.ceil(num_students / bus_capacity)
            print("assignStudents points for " + str(school))
            print("points needed are " + str(num_buses))
            random_points = [(random.uniform(42.2, 42.4), random.uniform(-71.0, -71.2)) for x in range(num_buses)]
            students_points = [(float(x[1]),float(x[2])) for x in i[1][0]]
            assignStudents_points = assignStudents.k_means(random_points, students_points)
            print(assignStudents_points)
            print("finished!")

            print("Calling assgn_students")
            final = assignStudents.assign_students(assignStudents_points, students_points)
            results.append(final)

        # stores the collection
        repo.dropCollection('echogu_wei0496_wuhaoyu.assigned_students')
        repo.createCollection('echogu_wei0496_wuhaoyu.assigned_students')
        for i in results:
            repo['echogu_wei0496_wuhaoyu.assigned_students'].insert_many(i)
        repo['echogu_wei0496_wuhaoyu.assigned_students'].metadata({'complete': True})
        print(repo['echogu_wei0496_wuhaoyu.assigned_students'].metadata())

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

    def porj_students(*points):
        return points

    def dist(p, q):
        (x1, y1) = p
        (x2, y2) = q
        return (x1 - x2) ** 2 + (y1 - y2) ** 2

    def plus(args):
        p = [0, 0]
        for (x, y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x, y) = p
        return (x / c, y / c)

    def k_means(M, P):
        OLD = []
        count = 0
        while (OLD != M):
            OLD = M
            if(count == 100):
                break
            MPD = [(m, p, assignStudents.dist(m, p)) for (m, p2) in transformData.product(M, P)]
            PDs = [(p, assignStudents.dist(m, p)) for (m, p, d) in MPD]
            PD = transformData.aggregate(PDs, min)
            MP = [(m, p) for ((m, p, d), (p2, d2)) in transformData.product(MPD, PD) if p == p2 and d == d2]
            MT = transformData.aggregate(MP, assignStudents.plus)

            M1 = [(m, 1) for ((m, p, d), (p2, d2)) in transformData.product(MPD, PD) if p == p2 and d == d2]
            MC = transformData.aggregate(M1, sum)

            M = [assignStudents.scale(t, c) for ((m, t), (m2, c)) in transformData.product(MT, MC) if m == m2]
            count = count + 1
        return (sorted(M))

    def assign_students(M, P):
        MPD = [(m, p, assignStudents.dist(m, p[0:2])) for (m, p) in transformData.product(M, P)]
        PDs = [(p, assignStudents.dist(m, p[0:2])) for (m, p, d) in MPD]
        PD = transformData.aggregate(PDs, min)
        id = 0
        final = []
        for j in M:
            result = []
            count = 0
            for i in PD:
                if (count >= Bus_capacity):
                    break
                    print("count out of bound")
                if(i[1] == assignStudents.dist(j,i[0][0:2])):
                    result.append({
                        'Student_id':i[0][2],
                        'Latitude': i[0][0],
                        'Longitude': i[0][1]})
                    count = count + 1
            #print(j)
            final.append({
                'Aggregated_Points': [j],
                'Points': result})
        return final
