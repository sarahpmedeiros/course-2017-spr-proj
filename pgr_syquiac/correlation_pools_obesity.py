'''
Pauline Ramirez & Carlos Syquia
correlation_pools_obesity.py

Calculates the correlation between obesity rates and number of pools in proximity 

'''


import urllib.request
import json
import dml
import prov.model™
import datetime
import uuid
import sodapy
from geopy.distance import vincenty
from geopy.geocoders import Nominatim
import scipy.stats

class correlation_pools_obesity(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.obesity_pools_stores']
    writes = ['pgr_syquiac.pools_obesity']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        stores = repo.pgr_syquiac.obesity_pools_stores.find()
        # Append tuples of (rate_of_obesity, num_pools)
        rates = []

        # Let the user choose the max distance from each pool
        distance = input("Please enter a distance in miles, or press enter to observe all data points: ")

        if not distance == '':
        	distance = float(distance)
        	print("Observing data points within a " + str(distance) + " mile distance of their nearest pools...")


        # keep track of data points being added if trial is on
        count = 0
        # get the distance of each data point from their closest pool
        # Create a data structure of the form (num_pools, obesity rate)
        for i in stores:
            if trial and count > 1000:
                break
            else:
                count += 1
                # Skip over points that don't have obesity rates
                if len(i['obesity_rates']) > 0 and len(i['closest_pools']) > 0:
                    for x in i['obesity_rates']:
                        pools = 0
                        for y in i['closest_pools']:
                            # Find the distance between each data point and all the pools
                            d = vincenty(x['geolocation']['coordinates'], y['coordinates']).miles
                            if distance == '':
                                pools += 1

                            else:
                                # If user wants to look at pools less than the distance specified
                                if d < distance:
                                    pools += 1
                        if 'data_value' in x:
                            rates.append({'num_pools': pools, 'obesity_rate': float(x['data_value'])})



        # print(rates)

        repo.dropPermanent("pools_obesity")
        repo.createPermanent("pools_obesity")
        repo['pgr_syquiac.pools_obesity'].insert_many(rates)
        print("Inserted new collection!")


        print("Calculating correlation coefficient and p-value...")
        x = []
        y = []
        for i in rates:
        	x.append(i['obesity_rate'])
        	y.append(i['num_pools'])


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
            'alg:pgr_syquiac#correlation_pools_obesity',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceObesityPools = doc.entity(
            'dat:pgr_syquiac#obesityPools',
            {'prov:label':'Obesity Pools', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceObesityPools, startTime)

        correlationObesityPools = doc.entity(
            'dat:pgr_syquiac#correlation_pools_obesity',
            {prov.model.PROV_LABEL:'Correlation Obesity Pools', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(correlationObesityPools, this_script)
        doc.wasGeneratedBy(correlationObesityPools, this_run, endTime)
        doc.wasDerivedFrom(correlationObesityPools, resourceObesityPools, this_run, this_run, this_run)

        repo.logout()

        return doc

# correlation_pools_obesity.execute()
# doc = correlation_pools_obesity.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
