# Algorithm to calculate a food accessibility score per neighborhood

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from collections import defaultdict

class calcfoodacc(dml.Algorithm):
	contributor = 'cxiao_jchew1_jguerero_mgarcia7'
	reads = ['cxiao_jchew1_jguerero_mgarcia7.foodsources', 'cxiao_jchew1_jguerero_mgarcia7.masteraddress']
	writes = []

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')

		# Data cursors
		foodsources_data_cursor = repo['cxiao_jchew1_jguerero_mgarcia7.foodsources'].find()
		add_data_cursor = repo['cxiao_jchew1_jguerero_mgarcia7.masteraddress'].find()

		fs = [(source['Neighborhood'], source['latitude'], source['longitude'], source['Type']) for source in foodsources_data_cursor]
		add = [(a['neighborhood'], a['latitude'], a['longitude'], 'Residential') for a in add_data_cursor]

		# Aggregate fs and add per neighborhood
		def aggregate(R):
			keys = {r[0] for r in R}
			return dict([(key, [(lat,lon,ty) for (k,lat,lon, ty) in R if k == key]) for key in keys])

		fs_per_nb = aggregate(fs)
		add_per_nb = aggregate(add)

		print(fs_per_nb)

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
		repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/jguerero_mgarcia7') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:cxiao_jchew1_jguerero_mgarcia7#calcfoodacc', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		foodsources_resource = doc.entity('dat:foodsources', {'prov:label':'Food Sources', prov.model.PROV_TYPE:'ont:DataSet'})
		masteraddresses_resource = doc.entity('dat:masteraddresses', {'prov:label':'Master Addresses', prov.model.PROV_TYPE:'ont:DataSet'})


		computeScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(computeScore, this_script)
		doc.usage(computeScore, foodsources_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
		doc.usage(computeScore, masteraddresses_resource, startTime, None,
		  {prov.model.PROV_TYPE:'ont:Computation'}
		  )
 
		'''
		foodsources = doc.entity('dat:cxiao_jchew1_jguerero_mgarcia7#calcfoodacc', {prov.model.PROV_LABEL:'Sources of food per neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(foodsources, this_script)
		doc.wasGeneratedBy(foodsources, computeScore, endTime)
		doc.wasDerivedFrom(foodsources, supermarkets_resource, computeScore, computeScore, computeScore)
		doc.wasDerivedFrom(foodsources, farmersmarkets_resource, computeScore, computeScore, computeScore)
		'''


		repo.logout()
				  
		return doc

def calc_distance():
	pass

calcfoodacc.execute()
## eof