# Algorithm to calculate a food accessibility score per neighborhood

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from collections import defaultdict
import pickle


class calcfoodacc(dml.Algorithm):
	contributor = 'cxiao_jchew1_jguerero_mgarcia7'
	reads = ['cxiao_jchew1_jguerero_mgarcia7.foodsources', 'cxiao_jchew1_jguerero_mgarcia7.masteraddress', 'cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics']
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

		fs_per_nb = defaultdict(list)
		add_per_nb = defaultdict(list)

		dummy_fs = [fs_per_nb[source['Neighborhood']].append((source['latitude'], source['longitude'], source['Type'])) for source in foodsources_data_cursor]
		dummy_add = [add_per_nb[a['neighborhood']].append((a['latitude'], a['longitude'], 'Residential')) for a in add_data_cursor if a.get('neighborhood') is not None] #[(a.get('neighborhood'),a['latitude'], a['longitude'], 'Residential') for a in add_data_cursor]

		'''
		# Aggregate fs and add per neighborhood
		def aggregate(R):
			keys = {r[0] for r in R if r[0] is not None}
			return dict([(key, [(lat,lon,ty) for (k,lat,lon, ty) in R if k == key]) for key in keys])

		fs_per_nb = aggregate(fs)
		add_per_nb = aggregate(add)
		'''


		def createDistanceMatrix(address,food):
			empty = [0] * len(food)
			mat = np.array([empty]*len(address), dtype=float)
			address = np.array(address)
			food = np.array(food)

			address = address[:,:2].astype(float)
			food = food[:,:2].astype(float)

			'''
			for row in range(len(address)):
				a = (address[row][0], address[row][1])
				for column in range(len(food)):
					f = (food[column][0], food[column][1])
					dist = vincenty(a, f).kilometers
					mat[row,column] = dist

			'''
			for idx,fs in enumerate(food):
				mat[:,idx] = np.apply_along_axis(distanceKm, 0, fs, address)
			return mat

		def createMetricsMatrix(address, food, distance): #metrics = rows and then columns is food
			empty = [0.000] * len(address)
			mat = np.array([empty]*3)

			e = [0] * 3
			m = np.array([e]*len(address))

			total = 0

			fm = 0
			sm = 0
			cs = 0
			#walking distance metric 
			for row in range(len(distance)):
				for i in range(len(distance[row])):
					if distance[row][i] < 1.0: 
						total += 1
						if food[i][2] == 'Farmers Market':
							m[row][0] += 1
						elif food[i][2] == 'Supermarkets':
							m[row][1] += 1
						elif food[i][2] == 'Cornerstores':
							m[row][2] += 1
						
				mat[0][row] = total

			#distance of closest
			for row in range(len(distance)):
				mat[1][row] = min(distance[row])

			#quality of food source
			t = len(food)	
			for row in range(len(m)):
				before = [1*((m[row][0])/t), (2/3)*((m[row][1])/t), (1/3)*((m[row][2])/t)]
				mat[2][row] = sum(before)

			return mat

		# Get the average metric score per neighborhood
		nbs = list(set(fs_per_nb.keys()).intersection(set(add_per_nb.keys())))
		print(nbs)

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
		weights = np.array([1,-1,1]) # Weight = 1 if it's a good thing to have a high value, -1 otherwise
		scores = np.sum(weights*zscore_metrics,axis=1)

		newd = {"Neighborhoods":nbs, "Scores":scores, "Zscore_metrics":zscore_metrics, "Avg_metrics":avg_metrics}
		pickle.dump(newd, open('info.p','wb'))


		# Create list of tuples that can be used to update a dictionary
		info = dict([(nb,score) for nb, score in zip(nbs,scores)])
		print(nbs)
		print(info)

		# Insert food accessbility score in the repo
		nstats = repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics'].find()
		nstats = [row for row in nstats]
		for item in nstats:
			nb = item['Neighborhood']
			item['FoodScore'] = info.get(nb)

		#print(nstats)

		repo.dropCollection("neighborhoodstatistics")
		repo.createCollection("neighborhoodstatistics")
		repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics'].insert_many(nstats)
		repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics'].metadata({'complete':True})
		print(repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics'].metadata())

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

def distanceKm(pt,add):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    #print("pt",pt)
    #print("add",add)

    pt = np.radians(pt)
    add = np.radians(add)

    lat1 = pt[0]
    lon1 = pt[1]

    lat2 = add[:,0]
    lon2 = add[:,1]

    '''

    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    '''

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

	

calcfoodacc.execute()
## eof