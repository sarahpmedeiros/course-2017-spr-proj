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
    reads = ['pgr_syquiac.sleep_rates_universities']
    writes = ['pgr_syquiac.sleep_universities']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        sleep = repo.pgr_syquiac.sleep_rates_universities.find()
        # print(sleep[2])
        # print(sleep[0])

      	
        # for i in sleep:
        # 	print(len(i['sleepRates']))
        


         # Append tuples of (dist_closest_uni, sleep_rate, name_of_closest_uni)
        rates = []

        # get the distance of each data point from their closest university
        # get the rate of people going to the doctor for a checkup
        # Create a data structure of the form (distance, sleep_rate, name)
        for i in sleep:
        	# Skip over schools that don't have data points
        	if len(i['sleepRates']) > 0:
	        	for j in i['sleepRates']:
	        		rate_distance = vincenty(j['geolocation']['coordinates'], i['coordinates']).miles
	        		if 'data_value' in j and rate_distance:
	        			rates.append({'distance_closest_uni': rate_distance, 'sleep_rate': float(j['data_value']), 'name_of_closest_uni': i['FIELD2']})

        repo.dropPermanent("sleep_universities")
        repo.createPermanent("sleep_universities")
        repo['pgr_syquiac.sleep_universities'].insert_many(rates)
        print("Inserted new collection!")
        

        print("Calculating correlation coefficient and p-value...")
        x = []
        y = []
        for i in rates:
        	x.append(i['distance_closest_uni'])
        	y.append(i['sleep_rate'])

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