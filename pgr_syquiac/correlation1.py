'''
Pauline Ramirez & Carlos Syquia
correlation1.py

Calculates the correlation between x and y 

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
import scipy.stats

class correlation1(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.hospitals_doctor_visits']
    writes = ['pgr_syquiac.hospitals_doctor_correlation']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')
        print("hello")

        visits = repo.pgr_syquiac.hospitals_doctor_visits.find()

        # Append pairs of visit rates, distance to here
        visit_rate_distance = []

        # just prints how many data points each one has
        # for i in visits:
        # 	print(len(i['doctorVisits']))
        print(visits[0])

        # get the distance of each data point from the hospital
        # get the rate of people going to the doctor for a checkup
        # find that correlation 
        for i in visits:
        	for j in i['doctorVisits']:
        		rate_distance = vincenty(j['geolocation']['coordinates'], i['location']['coordinates']).miles
        		if 'data_value' in j:
        			visit_rate_distance.append((rate_distance, j['data_value']))

        print(visit_rate_distance)

        math = scipy.stats.pearsonr(visit_rate_distance[0], visit_rate_distance[1])
        print(math[0])

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


        repo.logout()

        return doc
correlation1.execute()