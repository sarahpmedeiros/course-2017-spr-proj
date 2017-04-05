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

        if trial:
            crimes = list(repo['jgrishey.crime'].find(None, ['lat', 'long']))[:20]
        else:
            crimes = list(repo['jgrishey.crime'].find(None, ['lat', 'long']))

        crimes = np.array([[crime['lat'], crime['long']] for crime in crimes])

        ''' K Means Algorithm '''

        res = KMeans(n_clusters = 7, random_state = 4).fit(crimes)

        centers = []

        ID = 0

        for center in res.cluster_centers_:
            centers.append({'_id': ID, 'lat': center[0], 'long': center[1]})
            ID += 1

        repo.dropCollection("hospitalLocations")
        repo.createCollection("hospitalLocations")

        for center in centers:
            repo['jgrishey.hospitalLocations'].insert(center)

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

        this_script = doc.agent('alg:jgrishey#hospitalLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crime = doc.entity('dat:jgrishey#crime', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})

        hospitalLocations = doc.entity('dat:jgrishey#hospitalLocations', {prov.model.PROV_LABEL:'Hospital Locations via Crime and K Means.', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitalLocations, this_script)
        doc.wasGeneratedBy(hospitalLocations, this_run, endTime)
        doc.wasDerivedFrom(hospitalLocations, crime, this_run, this_run, this_run)

        repo.logout()

        return doc

## eof
