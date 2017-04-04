import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math
from sklearn.cluster import KMeans
import numpy as np

'''
    Helper Functions
'''

def product (R, S):
    return [(t, u) for t in R for u in S]

def aggregate (R, f):
    keys = [r[0] for r in R]
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]

def dist (p, q):
    (x1, y1) = p
    (x2, y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus (args):
    p = [0, 0]
    for (x, y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale (p, c):
    (x, y) = p
    return (x/c, y/c)

'''
    Main class
'''

class kMeansCrime(dml.Algorithm):
    contributor = 'jgrishey'
    reads = ['jgrishey.crime']
    writes = ['jgrishey.crimeMeans']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        crimes = list(repo['jgrishey.crime'].find(None, ['lat', 'long']))[:5]

        crimes = np.array([[crime['lat'], crime['long']] for crime in crimes])

        ''' K Means Algorithm '''

        res = KMeans(n_clusters = 5, random_state = 1).fit(crimes)

        centers = []

        ID = 0

        for center in res.cluster_centers_:
            centers.append({'_id': ID, 'lat': center[0], 'long': center[1]})
            ID += 1

        repo.dropCollection("hospitalLocations")
        repo.createCollection("hospitalLocations")

        for center in centers:
            repo['jgrishey.hospitalLocations'].insert(center)
            print(center)

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
        repo.authenticate('jgrishey', 'jgrishey')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jgrishey#getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_crime, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:DataSet',
                    'ont:Computation': 'Apply ID, get latitude, and get longitude'})

        crime = doc.entity('dat:jgrishey#crime', {prov.model.PROV_LABEL:'Boston Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc

kMeansCrime.execute()
## eof
