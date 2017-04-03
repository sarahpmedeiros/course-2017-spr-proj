# Combine all the data sets by neighborhood

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class neighborhoodstatistics(dml.Algorithm):
	contributor = 'cxiao_jchew1_jguerero_mgarcia7'
	reads = ['cxiao_jchew1_jguerero_mgarcia7.population', 'cxiao_jchew1_jguerero_mgarcia7.foodsources', 'cxiao_jchew1_jguerero_mgarcia7.obesityperneighborhood']
	writes = ['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')

		#create cursors to be able to go through the data
		population_cursor = repo['cxiao_jchew1_jguerero_mgarcia7.population'].find()
		foodsources_cursor = repo['cxiao_jchew1_jguerero_mgarcia7.foodsources'].find()
		obesity_cursor = repo['cxiao_jchew1_jguerero_mgarcia7.obesityperneighborhood'].find()

		#list for dictionarys (temp_stats) of neighborhood statistics
		nstats = []
		temp_stats = {'Neighborhood': 0, 'Population Size': 0, 'Average Income ($)': 0, 'Number of Food Sources': 0, 'Average Obesity (%)': 0}

		#all transformations used
		def union(R, S):
			return R + S

		def select(R, s):
			return [t for t in R if s(t)]

		def intersect(R, S):
			return [t for t in R if t in S]

		def project(R, p):
			return [p(t) for t in R]

		def aggregate(R, f):
			keys = {r[0] for r in R}
			return [(key, f([v for (k,v) in R if k == key])) for key in keys]

		#extracts necessary information from Population dataset to be used for new dataset
		p = project(population_cursor, lambda x: (x['Neighborhood'], x['Population'], x['Median Household Income in 2015 ($)']))

		#certain cities information were combined to better fit the obesity dataset
		FK = select(p, lambda a: a[0] == 'Fenway' or a[0] == 'Kenmore')
		FDD = select(p, lambda a: a[0] == 'Financial District' or a[0] == 'Downtown')
		GCFH = select(p, lambda a: a[0] == 'Government Center' or a[0] == 'Faneuil Hall')

		pop_info = select(p, lambda a: a[0] != 'Fenway' and a[0] != 'Kenmore' and a[0] != 'Financial District' and a[0] != 'Downtown' and a[0] != 'Government Center')

		def combine(tupl):
			'''
				This function combines two tuples together
			'''
			if (len(tupl) == 2):
				b = union(int(tupl[0][1].replace(",", "")), int(tupl[1][1].replace(",", "")))
				c = union(int(tupl[0][2].replace(",", "")), int(tupl[1][2].replace(",", "")))//2
				return (b,c)

		#adds the corrected tuple information to the population info list of tuples
		pop_info.append(('Fenway/Kenmore', combine(FK)[0], combine(FK)[1]))
		pop_info.append(('Financial District/Downtown', combine(FDD)[0], combine(FDD)[1]))
		pop_info.append(('Government Center/Faneuil Hall', GCFH[0][1], GCFH[0][2]))

		#get important info from food sources dataset 
		value = project(foodsources_cursor, lambda y: (y['Neighborhood'], y['Type']))
		food_info = dict(aggregate(value, lambda count: len(count)))

		#extract necessary info from obesity per neighboorhood statistics dataset
		obese_info = dict(project(obesity_cursor, lambda z: (z['neighborhood'], z['pctbmige30'])))

		#begins to create dictionary based on population information
		for i in pop_info:
			temp_stats = {key:0 for key in temp_stats}
			temp_stats['Neighborhood'] = i[0]
			temp_stats['Population Size'] = i[1]
			temp_stats['Average Income ($)'] = i[2]
			nstats.append(temp_stats)

		#appends food_info and obese_info information to dictionary so far based on neighborhood
		for k,Neighborhood in enumerate(n['Neighborhood'] for n in nstats):
			if food_info.get(Neighborhood) != None:
				nstats[k]['Number of Food Sources'] = food_info.get(Neighborhood)
			if obese_info.get(Neighborhood) != None:
				nstats[k]['Average Obesity (%)'] = obese_info.get(Neighborhood)

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
		doc.add_namespace('dat', 'http://datamechanics.io/data/cxiao_jchew1_jguerero_mgarcia7') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:cxiao_jchew1_jguerero_mgarcia7#neighborhoodstatistics', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		foodsources_resource = doc.entity('dat:foodsources', {'prov:label':'Food Sources', prov.model.PROV_TYPE:'ont:DataResource'})
		pop_resource = doc.entity('dat:population', {'prov:label':'Boston Demographics', prov.model.PROV_TYPE:'ont:DataResource'})
		obesity_resource = doc.entity('dat:obesityperneighborhood', {'prov:label':'Obesity Statistics per Neighborhood', prov.model.PROV_TYPE:'ont:DataResource'})

		get_neighborhoodstatistics = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_neighborhoodstatistics, this_script)
		doc.usage(get_neighborhoodstatistics, foodsources_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
		doc.usage(get_neighborhoodstatistics, obesity_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
		doc.usage(get_neighborhoodstatistics, pop_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )

		neighborhoodstatistics = doc.entity('dat:cxiao_jchew1_jguerero_mgarcia7#neighborhoodstatistics', {prov.model.PROV_LABEL:'Statistics per neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(neighborhoodstatistics, this_script)
		doc.wasGeneratedBy(neighborhoodstatistics, get_neighborhoodstatistics, endTime)

		doc.wasDerivedFrom(neighborhoodstatistics, obesity_resource, get_neighborhoodstatistics, get_neighborhoodstatistics, get_neighborhoodstatistics)
		doc.wasDerivedFrom(neighborhoodstatistics, pop_resource, get_neighborhoodstatistics, get_neighborhoodstatistics, get_neighborhoodstatistics)
		doc.wasDerivedFrom(neighborhoodstatistics, foodsources_resource, get_neighborhoodstatistics, get_neighborhoodstatistics, get_neighborhoodstatistics)


		repo.logout()
				  
		return doc


## eof

