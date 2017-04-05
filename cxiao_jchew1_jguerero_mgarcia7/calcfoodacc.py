# Algorithm to calculate a food accessibility score per neighborhood

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from collections import defaultdict
from geopy.distance import vincenty

class calcfoodacc(dml.Algorithm):
	contributor = 'cxiao_jchew1_jguerero_mgarcia7'
	reads = ['cxiao_jchew1_jguerero_mgarcia7.foodsources', 'cxiao_jchew1_jguerero_mgarcia7.masteraddress']
	writes = ['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection
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

		print(add_per_nb.keys())
		print(fs_per_nb.keys())

		def createDistanceMatrix(address,food):
			empty = [0] * len(food)
			mat = np.array([empty]*len(address), dtype=float)

			for row in range(len(address)):
				a = (address[row][0], address[row][1])
				for column in range(len(food)):
					f = (food[column][0], food[column][1])
					dist = vincenty(a, f).miles
					mat[row,column] = dist
			return mat
		def createMetricsMatrix(address, food, distance): #metrics = rows and then columns is food
			empty = [0.000] * len(address)
			mat = np.array([empty]*3)

			#walking distance metric 
			for row in range(len(distance)):
				mat[0][row] = sum([1 for i in range(len(distance[row])) if distance[row][i] < 0.5])

			#distance of closest
			for row in range(len(distance)):
				mat[1][row] = min(distance[row])

			#quality of food source
			fm = 0
			sm = 0
			cs = 0
			for i in range(len(food)):
				if food[i][2] == 'Farmers Market':
					fm += 1
				elif food[i][2] == 'Supermarkets':
					sm += 1
				elif food[i][2] == 'Cornerstores':
					cs += 1

			total = len(food)
			before = [1*(fm/total), (2/3)*(sm/total), (1/3)*(cs/total)]

			for row in range(len(distance)):
				mat[2][row] = sum(before)

			return mat



		# Get the average metric score per neighborhood
		nbs = set(fs_per_nb.keys()).intersection(set(add_per_nb.keys()))
		avg_metrics = np.zeros((len(nbs), 3), dtype=np.float64)

		for idx,nb in enumerate(nbs):
			distanceMat = createDistanceMatrix(add_per_nb[nb], fs_per_nb[nb])
			metricMatrix = createMetricsMatrix(add_per_nb[nb], fs_per_nb[nb], distanceMat)
			avg_metrics[idx] = np.mean(metricMatrix,axis=1, dtype=np.float64)

		# Compute the z-scores for each metric (to standardize)
		def computeZscore(arr):
			avg = np.mean(arr,dtype=np.float64)
			stdev = np.std(arr,dtype=np.float64)
			return (arr-avg)/stdev

		zscore_metrics = np.apply_along_axis(computeZscore,axis=0,arr=avg_metrics)

		# Compute a composite score for each neighborhood

		'''
		Result is the final metrics matrix for the specified neighborhood: 

		addresses above (same order as addresses in distance matrix)
		total in walking distance. # High  
		distance of closest fs	# Low
		quality of food source # High
		'''

		weights = np.array([-1,0,-1]) # Weight = -1 if it's a good thing to have a high value, Weight = 1 if it is not
		scores = np.sum(weights*zscore_metrics,axis=1)

		def scale_linear(rawpoints, high=100.0, low=0.0):
		    mins = np.min(rawpoints, axis=0)
		    maxs = np.max(rawpoints, axis=0)
		    rng = maxs - mins
		    return high - (((high - low) * (maxs - rawpoints)) / rng)

		scores = scale_linear(scores)

		for w,s in zip(nbs,scores):
			print(w,":",s)

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