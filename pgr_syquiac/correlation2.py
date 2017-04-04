'''
Pauline Ramirez & Carlos Syquia
correlation2.py

Calculates the correlation between sleep rates and proximity to a university

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

class correlation2(dml.Algorithm):
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


        # Let the user choose the max distance from each hospital
        distance = input("Please enter a distance in miles, or press enter to observe all data points: ")

        if not distance == '':
        	distance = float(distance)
        	print("Observing data points within a " + str(distance) + " mile distance of their nearest university...")

         # Append tuples of (dist_closest_uni, sleep_rate, name_of_closest_uni)
        rates = []
        # keep track of data points being added if trial is on
        count = 0
        # get the distance of each data point from their closest university
        # Create a data structure of the form (distance, sleep_rate, name)
        for i in sleep:
        	if trial and count > 1000:
        		break
        	else:
	        	# Skip over schools that don't have data points
	        	if len(i['sleepRates']) > 0:
		        	for j in i['sleepRates']:
		        		rate_distance = vincenty(j['geolocation']['coordinates'], i['coordinates']).miles
		        		if 'data_value' in j:
		        			if distance == '':
		        				count += 1
		        				rates.append({'distance_closest_uni': rate_distance,
		        			 		'sleep_rate': float(j['data_value']), 'name_of_closest_uni': i['FIELD2']})
		        			 	
		        			else: 
		        			 	if rate_distance < distance:
		        			 		rates.append({'distance_closest_uni': rate_distance,
		        			 			'sleep_rate': float(j['data_value']), 'name_of_closest_uni': i['FIELD2']})
		        			 		count += 1

        repo.dropPermanent("sleep_universities")
        repo.createPermanent("sleep_universities")
        repo['pgr_syquiac.sleep_universities'].insert_many(rates)
        print("Inserted new collection!")


        print("Calculating correlation coefficient and p-value...")
        # x = []
        # y = []
        # z = []
        # for i in rates:
        # 	x.append(i['distance_closest_uni'])
        # 	z.append((i['distance_closest_uni'], i['sleep_rate'], i['name_of_closest_uni']))
        # 	y.append(i['sleep_rate'])

        # for i in z:
        # 	print(i)
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

        this_script = doc.agent(
            'alg:pgr_syquiac#correlation2',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceSleepUniversities = doc.entity(
            'dat:pgr_syquiac#sleepUniversities',
            {'prov:label':'Sleep Universities', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceSleepUniversities, startTime)

        correlationSleepUniversities = doc.entity(
            'dat:pgr_syquiac#correlation2',
            {prov.model.PROV_LABEL:'Correlation Sleep Universities', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(correlationSleepUniversities, this_script)
        doc.wasGeneratedBy(correlationSleepUniversities, this_run, endTime)
        doc.wasDerivedFrom(correlationSleepUniversities, resourceSleepUniversities, this_run, this_run, this_run)

        repo.logout()

        return doc

correlation2.execute()
# doc = correlation2.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
