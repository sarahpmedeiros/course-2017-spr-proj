# assignStudents.py

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import random
from geopy.distance import vincenty

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

        # Trial mode: randomly choose students from k schools
        if trial:
            school_students = random.choices(school_students, k = 1)

        results = []
        for item in school_students:
            school = item[0]
            num_students = len(item[1][0])
            num_buses = math.ceil(num_students / bus_capacity)
            students_points = [(student[1], student[2], student[0]) for student in item[1][0]]
            print(str(school) + ":", num_students, "students,", num_buses, "buses")

            if num_buses == 1:
                means = [assignStudents.cal_centroid(students_points)]
            else:
                # initialize random points for k-means
                # random_points = [assignStudents.cal_centroid(students_points[i * num_buses: (i + 1) * num_buses]) for i in range(num_buses)]
                lat = [x for (x, y, z) in students_points]
                lon = [y for (x, y, z) in students_points]
                lower_lat, upper_lat = min(lat), max(lat)
                lower_lon, upper_lon = min(lon), max(lon)
                random_points = [(random.uniform(lower_lat, upper_lat), random.uniform(lower_lon, upper_lon)) for i in range(num_buses)]

                # k-means clustering algorithm for k = num_buses
                means = assignStudents.k_means(random_points, students_points, lower_lat, upper_lat, lower_lon, upper_lon)
                # print("k-means:", means)
                # print("k-means size:", len(means))

            final = assignStudents.assign_students(means, students_points)
            results.append(final)

        # stores the means and their corresponding students in the collection
        repo.dropCollection('assigned_students')
        repo.createCollection('assigned_students')
        for r in results:
            repo['echogu_wei0496_wuhaoyu.assigned_students'].insert_many(r)
        repo['echogu_wei0496_wuhaoyu.assigned_students'].metadata({'complete': True})
        print(repo['echogu_wei0496_wuhaoyu.assigned_students'].metadata(), "Saved Assigned Students")

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
        run_assignStudents = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # associate the activity with the script
        doc.wasAssociatedWith(run_assignStudents, this_script)
        # indicate that an activity used the entity
        doc.usage(run_assignStudents, resource_students, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        assigned_students = doc.entity('dat:echogu_wei0496_wuhaoyu#assigned_students', {prov.model.PROV_LABEL: 'assigned_students', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(assigned_students, this_script)
        doc.wasGeneratedBy(assigned_students, run_assignStudents, endTime)
        doc.wasDerivedFrom(assigned_students, resource_students, run_assignStudents, run_assignStudents, run_assignStudents)

        repo.logout()

        return doc

    @staticmethod
    def porj_students(*points):
        return points

    @staticmethod
    def dist(p, q):
        return vincenty(p, q).miles

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
    def eq_values(a, b):
        return math.isclose(a, b)

    @staticmethod
    def converge(old, new):
        # k-means first iteration
        if len(old) == 0:
            return False

        for i in range(len(old)):
            if abs(old[i][0] - new[i][0] > 0.001) or abs(old[i][1] - new[i][1]) > 0.001:
                return False
        return True

    @staticmethod
    def k_means(M, P, lower_lat, upper_lat, lower_lon, upper_lon):
        OLD = []
        num_means = len(M)

        while assignStudents.converge(OLD, M) == False:
            OLD = M

            MPD = [(m, p, assignStudents.dist(m, p[0:2])) for (m, p) in assignStudents.product(M, P)]
            PDs = [(p, assignStudents.dist(m, p[0:2])) for (m, p, d) in MPD]
            PD = assignStudents.aggregate(PDs, min)

            MP = [(m, p) for ((m, p, d), (p2, d2)) in assignStudents.product(MPD, PD) if p == p2 and d == d2]
            MT = assignStudents.aggregate(MP, assignStudents.plus)
            M1 = [(m, 1) for ((m, p, d), (p2, d2)) in assignStudents.product(MPD, PD) if p == p2 and d == d2]
            MC = assignStudents.aggregate(M1, sum)
            M = [assignStudents.scale(t, c) for ((m, t), (m2, c)) in assignStudents.product(MT, MC) if m == m2]
            M = sorted(M)

            # If some mean points merge together, add random points into the list of mean points
            if (len(M) != num_means):
                # print("Mean point merged!")
                diff = num_means - len(M)
                for i in range(diff):
                    random_points=[(random.uniform(lower_lat, upper_lat), random.uniform(lower_lon, upper_lon))]
                    M = M + random_points
            # print("M:", M)
        return sorted(M)

    @staticmethod
    def assign_students(M, P):
        # [Issue] K-means algorithm does not assign students to each mean evenly.
        # Although we have set the bus capacity, the actual assignment of students might overflow the capacity

        MPD = [(m, p, assignStudents.dist(m, p[:2])) for (m, p) in assignStudents.product(M, P)]
        PDs = [(p, assignStudents.dist(m, p[:2])) for (m, p, d) in MPD]
        PD = assignStudents.aggregate(PDs, min)

        final = []
        for mean in M:
            result = []
            # students_count = 0
            for i in PD:
                if(assignStudents.eq_values(i[1], assignStudents.dist(mean, i[0][:2]))):
                    result.append({
                        'student_id': i[0][2],
                        'latitude': i[0][0],
                        'longitude': i[0][1]})
                    # students_count += 1
                    # print(students_count)
            final.append({
                'aggregated_points': [mean],
                'points': result})
        return final

    @staticmethod
    def union(R, S):
        return R + S

    @staticmethod
    def difference(R, S):
        return [t for t in R if t not in S]

    @staticmethod
    def intersect(R, S):
        return [t for t in R if t in S]

    @staticmethod
    def project(R, p):
        return [p(t) for t in R]

    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def product(R, S):
        return [(t, u) for t in R for u in S]

    @staticmethod
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    @staticmethod
    def map(f, R):
        return [t for (k, v) in R for t in f(k, v)]

    @staticmethod
    def reduce(f, R):
        keys = {k for (k, v) in R}
        return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]

    @staticmethod
    def cal_centroid(*points):
        points = points[0]
        x_coords = [float(p[0]) for p in points]
        y_coords = [float(p[1]) for p in points]
        size = len(points)
        centroid_x = sum(x_coords) / size
        centroid_y = sum(y_coords) / size
        return (centroid_x, centroid_y)

# assignStudents.execute()
# doc = assignStudents.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
