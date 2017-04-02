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
    writes = ['pgr_syquiac.visit_rate_distance']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        visits = repo.pgr_syquiac.hospitals_doctor_visits.find()

        # Append tuples of (distance_nearest_hospital, rate_of_checkup, name_of_hospital) to here
        rates = []

        # just prints how many data points each one has
        # for i in visits:
        # 	print(len(i['doctorVisits']))
        # print(visits[0])

        # get the distance of each data point from their closest hospital
        # get the rate of people going to the doctor for a checkup
        # Create a data structure of the form (distance, rate, name)
        for i in visits:
        	for j in i['doctorVisits']:
        		rate_distance = vincenty(j['geolocation']['coordinates'], i['location']['coordinates']).miles
        		if 'data_value' in j and rate_distance:
        			rates.append({'distance_nearest_hospital': rate_distance, 'rate_of_checkup': float(j['data_value']), 'name_of_hospital': i['name']})

        repo.dropPermanent("visit_rate_distance")
        repo.createPermanent("visit_rate_distance")
        repo['pgr_syquiac.visit_rate_distance'].insert_many(rates)
        print("Inserted new collection!")



        print("Calculating correlation coefficient and p-value...")
        x = []
        y = []
        for i in rates:
        	x.append(i['distance_nearest_hospital'])
        	y.append(i['rate_of_checkup'])

        math = scipy.stats.pearsonr(x, y)
        print("Correlation coefficient is " + str(math[0]))
        print("P-value is " + str(math[1]))


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