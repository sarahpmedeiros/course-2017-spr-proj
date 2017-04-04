# Assign_Students.py

# import urllib.request
# import json
import dml
import prov.model
import datetime
import uuid
import math
import random
# from geopy.distance import vincenty

class assignStudents(dml.Algorithm):
    contributor = 'echogu_wei0496_wuhaoyu'
    reads = ['echogu_wei0496_wuhaoyu.students']
    writes = ['echogu_wei0496_wuhaoyu.assigned_students']

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
        for item in school_students[:10]:
            school = item[0]
            num_students = len(item[1][0])
            num_buses = math.ceil(num_students / bus_capacity)
            print(str(school) + ": ", num_students, "students,", num_buses, "buses")

            # initialize random points for k-means
            students_points = [(student[1], student[2], student[0]) for student in item[1][0]]
            # random_points = [assignStudents.cal_centroid(students_points[i * num_buses: (i + 1) * num_buses]) for i in range(num_buses)]
            lat = [x for (x, y, z) in students_points]
            lon = [y for (x, y, z) in students_points]
            lower_lat, upper_lat = min(lat), max(lat)
            lower_lon, upper_lon = min(lon), max(lon)
            random_points = [(random.uniform(lower_lat, upper_lat), random.uniform(lower_lon, upper_lon)) for i in range(num_buses)]

            # k-means clustering algorithm for k = num_buses
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
        run_kmeans = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # associate the activity with the script
        doc.wasAssociatedWith(run_kmeans, this_script)
        # indicate that an activity used the entity
        doc.usage(run_kmeans, resource_students, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        assigned_students = doc.entity('dat:echogu_wei0496_wuhaoyu#assigned_students', {prov.model.PROV_LABEL: 'assigned_students', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(assigned_students, this_script)
        doc.wasGeneratedBy(assigned_students, run_kmeans, endTime)
        doc.wasDerivedFrom(assigned_students, resource_students, run_kmeans, run_kmeans, run_kmeans)

        repo.logout()

        return doc

    @staticmethod
    def porj_students(*points):
        return points

    @staticmethod
    def dist(p, q):
        (x1, y1) = p
        (x2, y2) = q
        return (x1 - x2) ** 2 + (y1 - y2) ** 2

    @staticmethod
    def plus(args):
        p = [0, 0]
        for (x, y, z) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    @staticmethod
    def scale(p, c):
        (x, y) = p
        return (x / c, y / c)

    @staticmethod
    def eq_tuples(a, b):
        # return a == b
        # return abs(a[0] - b[0] < 0.001) and abs(a[1] - b[1] < 0.001)
        return math.isclose(a[0], b[0]) and math.isclose(a[1], b[1])

    @staticmethod
    def eq_points(a, b):
        # return a == b
        # return abs(a - b < 0.001)
        return math.isclose(a, b)

    @staticmethod
    def k_means(M, P):
        OLD = []
        count = 0
        while (OLD != M):
            OLD = M
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

    @staticmethod
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

    # def cal_centroid(*points):
    #     points = points[0]
    #     x_coords = [float(p[0]) for p in points]
    #     y_coords = [float(p[1]) for p in points]
    #     _len = len(points)
    #     centroid_x = sum(x_coords) / _len
    #     centroid_y = sum(y_coords) / _len
    #     return (centroid_x, centroid_y)

assignStudents.execute()
