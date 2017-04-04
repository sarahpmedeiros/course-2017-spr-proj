import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

'''
    Helper Functions
'''

def dist (p, q, coeff):
    (x1, y1) = p
    (x2, y2) = q
    return coeff*((x1-x2)**2 + (y1-y2)**2)

'''
    Main class
'''

class hospitalsLoad(dml.Algorithm):
    contributor = 'jgrishey'
    reads = ['jgrishey.hospitals', 'jgrishey.crime']
    writes = ['jgrishey.hospitalsLoad']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        hospitals = list(repo['jgrishey.hospitals'].find(None, ['lat', 'long', 'name', 'sqft']))

        aver = sum([float(hospital['sqft']) for hospital in hospitals]) / len(hospitals)

        for hospital in hospitals:
            hospital['bookings'] = 0
            hospital['coeff'] = 1 / (float(hospital['sqft']) / aver)

        if trial:
            crimes = list(repo['jgrishey.crime'].find(None, ['lat', 'long']))[:20]
        else:
            crimes = list(repo['jgrishey.crime'].find(None, ['lat', 'long']))

        '''
            Find closest hospital to crime, using coefficient based off sqft.
            Add booking to hospital. I know this isn't truly realistic, given
            hospitals aren't solely for crime, but this is just to get a visualization
            of hospital load just based on crime history.
        '''

        for crime in crimes:
            curr =  ("", float("inf"))
            for hospital in hospitals:
                distance = dist((float(crime['lat']), float(crime['long'])), (float(hospital['lat']), float(hospital['long'])), hospital['coeff'])
                if distance < curr[1]:
                    curr = (hospital['name'], distance)
            hosp = filter(lambda x: x['name'] == curr[0], hospitals)
            for h in hosp:
                h['bookings'] += 1

        for hospital in hospitals:
            hospital['load'] = float(hospital['bookings']) / (float(hospital['sqft']) / 100)

        repo.dropCollection("hospitalsLoad")
        repo.createCollection("hospitalsLoad")

        for hospital in hospitals:
            repo['jgrishey.hospitalsLoad'].insert(hospital)
            print(hospital)

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

        this_script = doc.agent('alg:jgrishey#hospitalsLoad', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        hospitals = doc.entity('dat:jgrishey#hospitals', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        crime = doc.entity('dat:jgrishey#crime', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, hospitals, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, hospitals, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})

        hospitalsLoad = doc.entity('dat:jgrishey#hospitalsLoad', {prov.model.PROV_LABEL:'Hospital Load Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitalsLoad, this_script)
        doc.wasGeneratedBy(hospitalsLoad, this_run, endTime)
        doc.wasDerivedFrom(hospitalsLoad, crime, this_run, this_run, this_run)
        doc.wasDerivedFrom(hospitalsLoad, hospitals, this_run, this_run, this_run)

        repo.logout()

        return doc

hospitalsLoad.execute()
## eof
