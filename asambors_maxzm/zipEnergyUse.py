import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import re 

__author__ = 'Ann Ming Samborski'

class zipEnergyUse(): #dml.Algorithm):
    contributor = 'asambors_maxzm'
    reads = ['asambors_maxzm.zipcodetolatlong',
             'asambors_maxzm.energywater',
             'asambors_maxzm.hospitals']
    writes = ['asambors_maxzm.zipenergyuse']

    @staticmethod
    def provides_care(property_type):
            recognized = ['Urgent Care/Clinic/Other Outpatient',
                          'Hospital (General Medical & Surgical)',
                          'Other - Specialty Hospital']
            return property_type['property_type'] in recognized

    @staticmethod
    def select(R, s):
    	return [t for t in R if s(t)] 


    @staticmethod
    def aggregate(R, f):
    	# (key, everything else)
    	keys = {r[0] for r in R}
    	return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def project(R, p):
    	return [p(t) for t in R]


    @staticmethod
    def filter_hospital_attr(hospital):
    	return {'name': hospital['name'],
    			'local':hospital['local']}

    @staticmethod
    def has_metrics(hospital):
    	if hospital['site_energy_use'] != 'Not Available':
    		return True


    @staticmethod
    def execute(): #trial = False):
    	startTime = datetime.datetime.now()

    	# Establish the date base connection
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate('asambors_maxzm','asambors_maxzm')

    	# Load data
    	zipcodetolatlong = repo['asambors_maxzm.zipcodetolatlong']
    	energywater = repo['asambors_maxzm.energywater']
    	hospitals = repo['asambors_maxzm.hospitals']

    	# Grab entries in energy data set
    	retrieveEnergyWater = energywater.find({},{'_id': False})

    	# Retrieve only properties that are care providers
    	properties = zipEnergyUse.select(retrieveEnergyWater, zipEnergyUse.provides_care)
    	with_metrics_properties = zipEnergyUse.select(properties, zipEnergyUse.has_metrics)

    	# for i in with_metrics_properties:
    	# 	print(i['site_energy_use'])
    	# 	print( int(re.sub("[^\d\.]", "", i['site_energy_use'])) ) 

    	# Transform so key is zip code 
    	projected_properties = zipEnergyUse.project(with_metrics_properties, lambda h: (h['zip'], int(re.sub("[^\d\.]", "", h['site_energy_use']))))

    	# Aggregate by zipcode to receive sum of energy used
    	agg_properties = zipEnergyUse.aggregate(projected_properties, sum)

    	print(agg_properties)

    	# Grab entries in hospitals data set
    	retrieveHospitals = hospitals.find({}, {'_id': False})

    	# Group by zip code
    	# Remove all entries except location_zip, name, neighborhood 
    	# filtered_hospitals = project(X, lambda h: (h['zipcode'], t[2]))

    	# Then an aggregate 



    	

zipEnergyUse.execute()

