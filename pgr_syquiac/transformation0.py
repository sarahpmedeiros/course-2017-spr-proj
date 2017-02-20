'''
    Pauline Ramirez and Carlos Syquia
    transformation0.py
    Correlation between lack of doctor visits and distance from the hospitals
'''

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from geopy.distance import vincenty
from geopy.geocoders import Nominatim

class transformation0(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.hospitals', 'pgr_syquia.cdc']
    writes = ['pgr_syquiac.hospitals_doctor_visits']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        #start
        bosHospitalsRepo = repo.pgr_syquiac.hospitals
        cdcRepo = repo.pgr_syquiac.cdc

        # Look for health insurance rates near hospitals
        hospitals = bosHospitalsRepo.find()
        locations = []

        print("Gathering all the hospitals in Boston...")
        # For each hospital, collect doctor visiting rates in the vicinity
        for i in hospitals:
            # Create an empty list of doctor visit rates
            i['doctorVisits'] = []
            locations.append(i)

        #print(locations)

        cdc = cdcRepo.find()
        visits = []

        print("Collecting appropriate data from CDC data set...")
        for i in cdc:
            # Look to make sure its a census tract, for health insurance rates
            if (i['measure'] == 'Visits to doctor for routine checkup within the past Year among adults aged >=18 Years' or
                i['measureid'] == 'CHECKUP'):
                visits.append(i)

        #print(len(visits))
        print("Mapping rates to closest hospitals...")
        for i in visits:
            # We want to iterate through all the visits rates and map them to the
            # closest hospital
            distance = vincenty(i['geolocation']['coordinates'], locations[0]['location']['coordinates']).miles
            idx = 0

            for j in range(len(locations)):
                rate_distance = vincenty(i['geolocation']['coordinates'], locations[j]['location']['coordinates']).miles
                if rate_distance < distance:
                    distance = rate_distance
                    idx = j

            locations[idx]['doctorVisits'].append(i)


        repo.dropPermanent("hospitals_doctor_visits")
        repo.createPermanent("hospitals_doctor_visits")
        repo['pgr_syquiac.hospitals_doctor_visits'].insert_many(locations)
        print("Inserted new collection!")

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
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/')

        this_script = doc.agent('alg:pgr_syquiac#transformation0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        hospitals_doctor_visits = doc.entity('dat:pgr_syquiac#hospitals_doctor_visits', {prov.model.PROV_LABEL: 'Hospital locations and doctor visits', prov.model.PROV_TYPE:'ont:Dataset'})
        get_hospitals_doctors_visits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Hospital distance from people who have a lack of doctor visits'})
        doc.wasAssociatedWith(get_hospitals_doctors_visits, this_script)
        doc.used(getResource, hospitals_doctor_visits, startTime)
        doc.wasAttributedTo(hospitals_doctor_visits, this_script)
        doc.wasGeneratedBy(hospitals_doctor_visits, get_hospitals_doctors_visits, endTime)

        # repo.record(doc.serialize()) # Record the provenance document. <- doesn't work for some reason

        repo.logout()

        return doc


transformation0.execute()
doc = transformation0.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
