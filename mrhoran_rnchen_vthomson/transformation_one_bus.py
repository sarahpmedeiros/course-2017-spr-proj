# part 1:minimum number of
# bus trips would be needed for each school 
# get # student per school
#  {student, schoool, their_address(coordinates)


# part 2:
#

#from which yard those
#buses should come
# get the # bus yards and their coordinates

import json
import geojson
import dml
import prov.model
import datetime
import uuid
import ast
import sodapy
import time 
import rtree
from tqdm import tqdm
import shapely.geometry
from geopy.distance import vincenty
import numpy as np


# this transformation will check how many comm gardens and food pantries there are for each area
# we want to take (zipcode, #comm gardens) (zipcode, #food pantries) --> (area, #food pantries#comm gardens)

class transformation_one_bus(dml.Algorithm):

    contributor = 'mrhoran_rnchen'

    reads = ['mrhoran_rnchen_vthomson.students',
             'mrhoran_rnchen_vthomson.buses']

    writes = ['mrhoran_rnchen_vthomson.student_per_school',
              'mrhoran_rnchen_vthomson.buses_per_yard',
              'mrhoran_rnchen_vthomson.average_distance_students']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()

        repo = client.repo
        
        repo.authenticate('mrhoran_rnchen_vthomson', 'mrhoran_rnchen_vthomson')
        
        X = project([x for x in repo.mrhoran_rnchen_vthomson.students.find({})], get_students)

        X2 = project(X, lambda t: (t[0], 1))

        X3 = aggregate(X2, sum)

        # 72 is the average # of buses we think we need
        
        students_per_school = project(select(product(X,X3), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0],(t[0][1], t[1][1], (t[1][1]/72))))
                        
        repo.dropCollection('student_per_school')
        repo.createCollection('student_per_school')

        repo.mrhoran_rnchen_vthomson.student_per_school.insert(dict(students_per_school))

        # buses needed for school
        
############################
## FIND THE AVERAGE # CASE DISTANCE BETWEEN STUDENTS ** (MAYBE WORST CASE DISTANCE)

        student_locations = [(f, shapely.geometry.shape(f['geometry'])) for f in tqdm(geojson.loads(open('input_data/students-simulated.geojson').read())['features']) if f['geometry'] is not None]
#http://datamechanics.io/data/_bps_transportation_challenge/buses.geojson

        student_data = geojson.load(open('input_data/students-simulated.geojson'))


#         bus_locations = [(f, shapely.geometry.shape(f['geometry'])) for f in tqdm(geojson.loads(open('').read())['features']) if f['geometry'] is not None]


	#fill tree
        p = rtree.index.Property()
        student_tree = rtree.index.Index(properties=p)
        for i in tqdm(range(len(student_locations))):
              (f,s) = student_locations[i]
              student_tree.insert(i, s.bounds)

        
        avgs = []
        #average distance to nearest 10 other students for each student
        for i in tqdm(range(len(student_locations))):
              #print(student_locations[i][0]['geometry']['coordinates'])

              sv = student_locations[i][0]['geometry']['coordinates']
              m = (sv[0][0],sv[0][1])
              near = list(student_tree.nearest(m,1,True))
              n = [x.bbox for x in near]
              #print(len(n))

              #print(m,(n[0][0],n[0][1])) 
              #print([vincenty((m[0],m[1]),(c[0],c[1])) for c in n])
              #avgs.append(sum([vincenty((m[0],m[1]),(c[0],c[1])).miles for c in n])/len(n))
              d = [vincenty((m[0],m[1]),(c[0],c[1])).miles for c in n]
              d.sort()
              student_radius = [s for s in d if s < 0.5]
 
              avgs.append(np.sum([d[i] for i in range(min(10,len(d)))])/10)
              #print(avgs[i])
              #r = [(((m[0]-l[0])**2) + ((m[1]-l[1])**2)**(1/2)) for l in n]
              
 
        #print(avgs[0]) 
        

        #test
        #hits = student_tree.nearest((-71.2,42.2),10,True)
        #hits = [student_data[c] for c in hits]
        #for t in hits:
          #  print(t.bbox)
        #print(list(hits))


       # bounds = (-70,-72,41,43)
        
       # hits = student_tree.intersection(bounds)
       # print(hits)

        A = project([x for x in repo.mrhoran_rnchen_vthomson.students.find({})], find_location_students)
        """
        average_distance_student = []
	
        for feature in tqdm(student_data['features']):
            if 'geometry' in feature and 'coordinates' in feature['geometry']:
                coordinates = feature['geometry']['coordinates']
                if any([
                    shape.contains(shapely.geometry.Point(lon, lat))
                    for (lon, lat) in coordinates
                    for (feature, shape) in [student_locations[i]
                    for i in student_tree.nearest((lon,lat,lon,lat), 1)]
                    ]):
                    average_distance_student.append(feature)
                    continue
        """

        # now we have start_time , (lat,long) for every student now we want to preform average to find average distance per school start time
        # idea here is to see what conditions buses are like for differents start times
        
        # also want to keep track of the worst distance between a student (possibly)
<<<<<<< HEAD
#<<<<<<< HEAD
=======

>>>>>>> 48094adfb04a86614a36408dbaf8e6bf08d6d89a
       
      
       #vincentie geopy
<<<<<<< HEAD
#=======
        """ 
=======
        """
>>>>>>> 48094adfb04a86614a36408dbaf8e6bf08d6d89a
>>>>>>> 5f6b3c80055d5fed1c0fccdcdeb0d979025eae25
       
        
        b = select(product(A,A), lambda t: t[0][0][0] == t[1][0][0])

        c = select(project(b, lambda t: (t[0][0], dist(t[0][1],t[1][1]))), lambda t: t[1] > 0.0)

        d = project(b, lambda t: (t[0][0], 1))

        g = aggregate(c, sum)

        f = aggregate(d, sum)

        average_distance_students = project(select(product(f,g), lambda t: (t[0][0] == t[1][0])), lambda t: (t[0][0], (t[1][1]/t[0][1])))

        

<<<<<<< HEAD
=======

<<<<<<< HEAD
        """

        
#>>>>>>> 1e27ffee620800c204e86d42bfaba47567e022a5
=======
        """        
>>>>>>> 48094adfb04a86614a36408dbaf8e6bf08d6d89a

        repo.dropCollection('average_distance_students')
        repo.createCollection('average_distance_students')

       # repo.mrhoran_rnchen_vthomson.student_per_school.insert(dict(average_distance_students))
        
    
##########################

        ## number of buses per yard
        
        Y = project([p for p in repo.mrhoran_rnchen_vthomson.buses.find({})], get_buses)

        Y2 = project(Y, lambda t: (t[0], 1))

        Y3 = aggregate(Y2, sum)

        bus_per_school = project(select(product(Y,Y3), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0],(t[0][1], t[1][1])))

        #print(bus_per_school[0])
           
        repo.dropCollection('buses_per_yard')
        repo.createCollection('buses_per_yard')
        
        repo.mrhoran_rnchen_vthomson.buses_per_yard.insert(dict(bus_per_school))


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mrhoran_rnchen_vthomson', 'mrhoran_rnchen_vthomson')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('dat:mrhoran_rnchen_vthomson#transformation_one_bus', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource1 = doc.entity('dat:_bps_transportation_challenge/buses.json', {'prov:label':'Bus Yard Aggregation', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_buses_per_yard = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_buses_per_yard, this_script)

        doc.usage(get_buses_per_yard, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

           # label section might be wrong
        resource2 = doc.entity('dat:_bps_transportation_challenge/students.json', {'prov:label':'Student Aggregation', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_student_per_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_student_per_school, this_script)

        doc.usage(get_student_per_school, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        # Used resource2 --> average_distance_students uses students.json
        get_average_distance_students = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_average_distance_students, this_script)

        doc.usage(get_average_distance_students, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
    
        student_per_school = doc.entity('dat:mrhoran_rnchen_vthomson#student_per_school', {prov.model.PROV_LABEL:'Students per school', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(student_per_school, this_script)
        doc.wasGeneratedBy(student_per_school, get_student_per_school, endTime)
        doc.wasDerivedFrom(student_per_school, resource2, get_student_per_school, get_student_per_school, get_student_per_school)

    
        buses_per_yard = doc.entity('dat:mrhoran_rnchen_vthomson#buses_per_yard', {prov.model.PROV_LABEL:'Buses per yard', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(buses_per_yard, this_script)
        doc.wasGeneratedBy(buses_per_yard, get_buses_per_yard, endTime)
        doc.wasDerivedFrom(buses_per_yard, resource1, get_buses_per_yard, get_buses_per_yard, get_buses_per_yard)   


        average_distance_students = doc.entity('dat:mrhoran_rnchen_vthomson#average_distance_students', {prov.model.PROV_LABEL:'Distance between students', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(average_distance_students, this_script)
        doc.wasGeneratedBy(average_distance_students, get_average_distance_students, endTime)
        doc.wasDerivedFrom(average_distance_students, resource2, get_average_distance_students, get_average_distance_students, get_average_distance_students)       

        repo.logout()
                  
        return doc

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    
def select(R, s):
    return [t for t in R if s(t)]

def project(R, p):
    return [p(t) for t in R]

def product(R, S):
    return [(t,u) for t in R for u in S]

"""
#radixsort source: http://www.geekviewpoint.com/python/sorting/radixsort
def radixsort( aList ):
  RADIX = 10
  maxLength = False
  tmp , placement = -1, 1
 
  while not maxLength:
    maxLength = True
    # declare and initialize buckets
    buckets = [list() for _ in range( RADIX )]
 
    # split aList between lists
    for  i in aList:
      tmp = i / placement
      buckets[tmp % RADIX].append( i )
      if maxLength and tmp > 0:
        maxLength = False
 
    # empty lists into aList array
    a = 0
    for b in range( RADIX ):
      buck = buckets[b]
      for i in buck:
        aList[a] = i
        a += 1
 
    # move to next digit
    placement *= RADIX

"""

def find_location_students(student):

    lat = float(student["Latitude"])
    lon = float(student["Longitude"])
    school_start_time = ["Current School Start Time"]

<<<<<<< HEAD
#<<<<<<< HEAD
    return((school_start_time, (lat,long)))
#=======
    return([school_start_time, (lat,lon)])
#>>>>>>> 1e27ffee620800c204e86d42bfaba47567e022a5
=======
    return((school_start_time, (lat,long)))
>>>>>>> 48094adfb04a86614a36408dbaf8e6bf08d6d89a
    

def get_students(student): # want to return the coordinates of the towns in and around Boston

    name = student["Assigned School"]
    
    if(student["Assigned School"] == "Sr. Kennedy School"):

        name = "Sr Kennedy School"
        
    return((name, (student["School Longitude"], student["School Latitude"])))

def get_buses(bus): # want to return the coordinates of the towns in and around Boston

    lat = bus['Bus Yard Latitude']
    lon = bus['Bus Yard Longitude']
    name =  bus['Bus Yard']

<<<<<<< HEAD
#<<<<<<< HEAD
    return((name, (lat,long)))
#=======
    return([name, (lat,lon)])
#>>>>>>> 1e27ffee620800c204e86d42bfaba47567e022a5
=======
    return((name, (lat,lon)))
>>>>>>> 48094adfb04a86614a36408dbaf8e6bf08d6d89a

transformation_one_bus.execute()
doc = transformation_one_bus.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
