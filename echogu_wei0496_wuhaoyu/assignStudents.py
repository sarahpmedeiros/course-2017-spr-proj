# Assign_Students.py

# import urllib.request
# import json
import dml
import prov.model
import datetime
import uuid
import math
import random
#from geopy.distance import vincenty
#from echogu_wei0496_wuhaoyu import assignStudents

class assignStudents(dml.Algorithm):
    contributor = 'echogu_wei0496_wuhaoyu'
    reads = ['echogu_wei0496_wuhaoyu.bus_yards', 'echogu_wei0496_wuhaoyu.students', 'echogu_wei0496_wuhaoyu.schools']
    writes = ['echogu_wei0496_wuhaoyu.bus_routes']

    # set the bus capacity
    global bus_capacity
    bus_capacity = 50

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
            students.append({'_id': item['_id'],
                             'latitude': item['geometry']['coordinates'][0][1],
                             'longitude': item['geometry']['coordinates'][0][0],
                             'school': item['properties']['school']})

        # group the student belongs to the same school
        project_students = assignStudents.project(students, lambda t: (t['school'], [t['_id'], t['latitude'], t['longitude']]))
        school_students = assignStudents.aggregate(project_students, assignStudents.porj_students)

        results = []
        for item in school_students[7:15]:
            school = item[0]
            num_students = len(item[1][0])
            num_buses = math.ceil(num_students / bus_capacity)
            print(str(school) + ": ", num_students, "students,", num_buses, "buses")
            # k-means clustering algorithm for k = num_buses
            students_points = [(student[1], student[2], student[0]) for student in item[1][0]]
            random_points = [assignStudents.cal_centroid(students_points[i * num_buses: (i + 1) * num_buses]) for i in range(num_buses)]
            means = assignStudents.k_means(random_points, students_points)
            print(len(means))
            print("k-means:", means)

            final = assignStudents.assign_students(means, students_points)
            results.append(final)

        # stores the means and their corresponding students in the collection
        repo.dropCollection('echogu_wei0496_wuhaoyu.assigned_students')
        repo.createCollection('echogu_wei0496_wuhaoyu.assigned_students')
        for r in results:
            repo['echogu_wei0496_wuhaoyu.assigned_students'].insert_many(r)
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

        # create document object and define namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # define entity to represent resources
        this_script = doc.agent('alg:echogu_wei0496_wuhaoyu#assignStudents', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_students = doc.entity('dat:echogu_wei0496_wuhaoyu#students', {'prov:label': 'students', prov.model.PROV_TYPE: 'ont:DataSet'})

        # define activity to represent invocaton of the script
        
        # resource_HubwayStations = doc.entity('dat:echogu_wei0496_wuhaoyu#HubwayStations',
        #                                      {'prov:label': 'Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        #
        # get_LandmarksHubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # doc.wasAssociatedWith(get_LandmarksHubway, this_script)
        # doc.usage(get_LandmarksHubway, resource_BLCLandmarks, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Computation'})
        # doc.usage(get_LandmarksHubway, resource_HubwayStations, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Computation'})
        #
        # LandmarksHubway = doc.entity('dat:echogu_wei0496_wuhaoyu#LandmarksHubway',
        #                              {prov.model.PROV_LABEL: 'Landmarks Hubway Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(LandmarksHubway, this_script)
        # doc.wasGeneratedBy(LandmarksHubway, get_LandmarksHubway, endTime)
        # doc.wasDerivedFrom(LandmarksHubway, resource_BLCLandmarks, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)
        # doc.wasDerivedFrom(LandmarksHubway, resource_HubwayStations, get_LandmarksHubway, get_LandmarksHubway, get_LandmarksHubway)

        # # define activity to represent invocation of the script
        # get_buses = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # get_grade_safe_distance  = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # get_safety_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # get_schools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # get_students = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        # # associate the activity with the script
        # doc.wasAssociatedWith(get_buses, this_script)
        # doc.wasAssociatedWith(get_grade_safe_distance, this_script)
        # doc.wasAssociatedWith(get_safety_scores, this_script)
        # doc.wasAssociatedWith(get_schools, this_script)
        # doc.wasAssociatedWith(get_students, this_script)
        #
        # # indicate that an activity used the entity
        # doc.usage(get_buses, resource_buses, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query':'?type=buses&$select=properties,type,geometry'
        #            }
        #           )
        # doc.usage(get_grade_safe_distance, resource_grade_safe_distance, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query':'?type=grade_safe_distance&$select=K,1,2,3,4,5,6'
        #            }
        #           )
        # doc.usage(get_safety_scores, resource_safety_scores, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query':'?type=safety_scores$select=Safety+Score,Geocode'
        #            }
        #           )
        # doc.usage(get_schools, resource_schools, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query':'?type=schools&$select=properties,type,geometry'
        #            }
        #           )
        # doc.usage(get_students, resource_students, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query':'?type=students&$select=type,properties,geometry'
        #            }
        #           )
        #
        # # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        # buses = doc.entity('dat:echogu_wei0496_wuhaoyu#buses', {prov.model.PROV_LABEL: 'buses', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(buses, this_script)
        # doc.wasGeneratedBy(buses, get_buses, endTime)
        # doc.wasDerivedFrom(buses, resource_buses, get_buses, get_buses, get_buses)
        #
        # grade_safe_distance = doc.entity('dat:echogu_wei0496_wuhaoyu#grade_safe_distance', {prov.model.PROV_LABEL: 'grade_safe_distance', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(grade_safe_distance, this_script)
        # doc.wasGeneratedBy(grade_safe_distance, get_grade_safe_distance, endTime)
        # doc.wasDerivedFrom(grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance)
        #
        # safety_scores = doc.entity('dat:echogu_wei0496_wuhaoyu#safety_scores', {prov.model.PROV_LABEL: 'safety_scores', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(safety_scores, this_script)
        # doc.wasGeneratedBy(safety_scores, get_safety_scores, endTime)
        # doc.wasDerivedFrom(safety_scores, resource_safety_scores, get_safety_scores, get_safety_scores, get_safety_scores)
        #
        # schools = doc.entity('dat:echogu_wei0496_wuhaoyu#schools', {prov.model.PROV_LABEL: 'schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(schools, this_script)
        # doc.wasGeneratedBy(schools, get_schools, endTime)
        # doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)
        #
        # students = doc.entity('dat:echogu_wei0496_wuhaoyu#students', {prov.model.PROV_LABEL: 'students', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(students, this_script)
        # doc.wasGeneratedBy(students, get_students, endTime)
        # doc.wasDerivedFrom(students, resource_students, get_students, get_students, get_students)


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
        for (x, y, z) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x, y) = p
        return (x / c, y / c)

    def eq_tuples(a, b):
        # return a == b
        # return abs(a[0] - b[0] < 0.001) and abs(a[1] - b[1] < 0.001)
        return math.isclose(a[0], b[0]) and math.isclose(a[1], b[1])

    def eq_points(a, b):
        # return a == b
        # return abs(a - b < 0.001)
        return math.isclose(a, b)

    def k_means(M, P):
        OLD = []
        count = 0
        while (OLD != M):
            OLD = M
            # [Changes needed] check if diff b/t 2 pts < 0.001
            if(count == 10):
                break
            MPD = [(m, p, assignStudents.dist(m, p[0:2])) for (m, p) in assignStudents.product(M, P)]
            PDs = [(p, assignStudents.dist(m, p[0:2])) for (m, p, d) in MPD]
            PD = assignStudents.aggregate(PDs, min)
            MP = [(m, p) for ((m, p, d), (p2, d2)) in assignStudents.product(MPD, PD) if assignStudents.eq_tuples(p, p2) and assignStudents.eq_points(d, d2)]
            MT = assignStudents.aggregate(MP, assignStudents.plus)

            M1 = [(m, 1) for ((m, p, d), (p2, d2)) in assignStudents.product(MPD, PD) if assignStudents.eq_tuples(p, p2) and assignStudents.eq_points(d, d2)]
            MC = assignStudents.aggregate(M1, sum)

            M = [assignStudents.scale(t, c) for ((m, t), (m2, c)) in assignStudents.product(MT, MC) if m == m2]
            M = sorted(M)
            count += 1
        return sorted(M)

    def assign_students(M, P):
        MPD = [(m, p, assignStudents.dist(m, p[:2])) for (m, p) in assignStudents.product(M, P)]
        PDs = [(p, assignStudents.dist(m, p[:2])) for (m, p, d) in MPD]
        PD = assignStudents.aggregate(PDs, min)

        final = []
        for mean in M:
            result = []
            students_count = 0

            for i in PD:
                if (students_count > bus_capacity):
                    # print("Bus is full.")
                    # print(students_count)
                    break
                if(i[1] == assignStudents.dist(mean, i[0][:2])):
                    # print(i)
                    result.append({
                        'student_id': i[0][2],
                        'latitude': i[0][0],
                        'longitude': i[0][1]})
                    students_count += 1
            #print(j)
            final.append({
                'aggregated_points': [mean],
                'points': result})
        return final

    def union(R, S):
        return R + S

    def difference(R, S):
        return [t for t in R if t not in S]

    def intersect(R, S):
        return [t for t in R if t in S]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    def map(f, R):
        return [t for (k, v) in R for t in f(k, v)]

    def reduce(f, R):
        keys = {k for (k, v) in R}
        return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]

    def cal_centroid(*points):
        points = points[0]
        x_coords = [float(p[0]) for p in points]
        y_coords = [float(p[1]) for p in points]
        _len = len(points)
        centroid_x = sum(x_coords) / _len
        centroid_y = sum(y_coords) / _len
        return (centroid_x, centroid_y)

assignStudents.execute()
