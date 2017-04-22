# Statistical analysis

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from collections import defaultdict
import pickle
from sklearn import linear_model


class statisticsanalysis(dml.Algorithm):
	contributor = 'cxiao_jchew1_jguerero_mgarcia7'
	reads = ['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics']
	writes = []

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')

		# Data cursor
		nstats = repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoodstatistics'].find()

		foodscores = []
		income = []
		obesity = []

		for nb in nstats:
			foodscores.append(nb['FoodScore'])
			income.append(nb['Average Income ($)'])
			obesity.append(nb['Average Obesity (%)'])


		observations = np.vstack((foodscores,income,obesity))# rows = variables, x = obeservations

		idx_to_delete = None
		for idx, x in enumerate(foodscores):
			if x is None:
				idx_to_delete = idx

		n = np.delete(observations,(3),axis=1)

		# linear regression
		n = n.astype(float)
		x = n[:2,:].T
		y = n[2,:].T

		print(x.shape)
		print(y.shape)

		clf = linear_model.LinearRegression()
		clf.fit(x,y)

		print(clf.coef_)

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

		this_script = doc.agent('alg:cxiao_jchew1_jguerero_mgarcia7#statisticsanalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		neighborhoodstatistics_resource = doc.entity('dat:neighborhoodstatistics', {'prov:label':'Neighborhood Statistics', prov.model.PROV_TYPE:'ont:DataSet'})


		get_statistics = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_statistics, this_script)
		doc.usage(get_statistics, neighborhoodstatistics_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
 
		statisticsanalysis = doc.entity('dat:cxiao_jchew1_jguerero_mgarcia7#statisticsanalysis', {prov.model.PROV_LABEL:'Neighborhood statistical analysis', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(statisticsanalysis, this_script)
		doc.wasGeneratedBy(statisticsanalysis, get_statistics, endTime)
		doc.wasDerivedFrom(statisticsanalysis, neighborhoodstatistics_resource, get_statistics, get_statistics, get_statistics)


		repo.logout()
				  
		return doc

	

statisticsanalysis.execute()
## eof