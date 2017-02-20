'''
    Pauline Ramirez and Carlos Syquia
    transformation1.py
    Sleep rates and correlation to university locations
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

class transformation1(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.cdc', 'pgr_syquiac.schools']
    writes = ['pgr_syquiac.sleep_rates_universities']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        #start
        cdcRepo = repo.pgr_syquiac.cdc
        schoolsRepo = repo.pgr_syquiac.schools

        all_schools = schoolsRepo.find()

        # Need to collect all the boston schools only
        print("Gathering schools in Boston....")
        boston_schools = []
        for i in all_schools:
            if i["FIELD4"] == "Boston" or i["FIELD4"] == "Cambridge":
                boston_schools.append(i)

        # Create a geolocator to get the coords
        geolocator = Nominatim()

        # Now then add another field for each school in the list of Boston Schools of their closest sleep rates
        # Also need to add a coordinates field
        for i in boston_schools:
            i['sleepRates'] = []
            i['coordinates'] = []
            
        # Add coordinates
        for i in boston_schools:
            i['coordinates'].append((i["FIELD65"], i["FIELD66"]))
 

        # Now filter by sleep issues for boston
        cdc = cdcRepo.find() 
        sleep = []
        print("Collecting appropriate data from CDC data set...")
        for i in cdc:
            # Look to make sure its a census tract, for health insurance rates
            if (i['measure'] == 'Sleeping less than 7 hours among adults aged >=18 Years' or 
                i['measureid'] == 'SLEEP'):
                sleep.append(i)
        

        # Map each sleep rates to the nearest campus
        print("Mapping sleep rates to closest universities...")
        for i in sleep:
            # We want to iterate through all the visits rates and map them to the
            # closest hospital

            distance = vincenty(i['geolocation']['coordinates'], boston_schools[0]['coordinates']).miles
            idx = 0

            for j in range(len(boston_schools)):
                rate_distance = vincenty(i['geolocation']['coordinates'], boston_schools[j]['coordinates'][0]).miles
                if rate_distance < distance:
                    distance = rate_distance
                    idx = j

            boston_schools[idx]['sleepRates'].append(i)

        # print(len(boston_schools))
        # print(boston_schools[0])

        # print("----")
        # for i in boston_schools:
        #     for j in range(len(i["sleepRates"])):
        #         if j == 0:
        #             pass
        #         else:
        #             print (i["FIELD2"] + " " + i["sleepRates"][j]['data_value'])

        repo.dropPermanent("sleep_rates_universities")
        repo.createPermanent("sleep_rates_universities")
        repo['pgr_syquiac.sleep_rates_universities'].insert_many(boston_schools)
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
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')

        this_script = doc.agent('alg:pgr_syquiac#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

transformation1.execute()