import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import mapReduce

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

    	retrieveEnergyWater = energywater.find({},{'_id': False})

    	properties = zipEnergyUse.select(retrieveEnergyWater, zipEnergyUse.provides_care)

    	# for i in properties:
    	# 	print(i['property_type'])

    	

    	

zipEnergyUse.execute()

