import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests 
import time
import csv
import numpy as np
from sklearn.cluster import KMeans

class makeStudentsGraph(dml.Algorithm):
	contributor = 'skaram13_smedeiro'
	reads = []
	writes = ['skaram13_smedeiro.students']
	

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		# print ('here')
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

		results = repo['skaram13_smedeiro.students'].aggregate(
			[
     			{'$group':{'_id' : '$properties.school', 'students': { '$push': '$$ROOT' } } }
			]
		)

		for school in results:
			P = []
			walkLookup = {}
			for student in school['students']:
				walk =  student['properties']['walk']
				longitude = student['geometry']['coordinates'][0][0]
				latitude = student['geometry']['coordinates'][0][1]
				
				if walk is not None:
					key = str(longitude) + "," + str(latitude)
					
					#if someone else is at the same location,use the min walking distance
					if key in walkLookup: 
						walkLookup[key] = min(walk, walkLookup[key])
					else:
						walkLookup[key] = walk

					latLon = [str(longitude),str(latitude)]
					P.append(latLon)

			k = 10
			X = np.array(P)
			kmeans = KMeans(n_clusters=k).fit(X)

			labels = kmeans.labels_
			centers = kmeans.cluster_centers_
			pointsByMean = [X[np.where(labels==i)] for i in range(k)]
			
			meansAndPoints = {}

			for i in range(k):
				entries = []
				for item in pointsByMean[i]:
					walkKey = str(item[0]) + "," + str(item[1])
					walk = walkLookup[walkKey]

					entry = {'longitude':item[0],'latitude':item[1],'walk':walk}
					entries.append(entry)
				
				key = str(centers[i][0]) + "," + str(centers[i][1])
				meansAndPoints[key] = entries
			
			for centers in meansAndPoints:
				print()
				print("K MEAN: ", centers)
				print("STUDENTS FOR THAT MEAN: ")
				for each in meansAndPoints[centers]:
			 		print (each)

			# print(meansAndPoints)
			
			
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
			'''

		# Set up the database connection.
		# client = dml.pymongo.MongoClient()
		# repo = client.repo
		# doc.add_namespace('alg', 'http://datamechanics.io/algorithm/skaram13_smedeiro/') # The scripts are in <folder>#<filename> format.
		# doc.add_namespace('dat', 'http://datamechanics.io/data/skaram13_smedeiro/') # The data sets are in <user>#<collection> format.
		# doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		# doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		# doc.add_namespace('dmg', 'https://census.gov/resource/')


		# #Agents
		# # this script gets the tech rates for 2011 and the corresponding org codes
		# this_script = doc.agent('alg:skaram13_smedeiro#getGradRatesByOrgCodeFor2011', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		# # Entities
		# # this is the data set that we take the Grad reports from 
		# gradReport = doc.entity('dmg:skaram13_smedeiro#Graduation-Rate-Report-by-District-by-School',{'prov:label':'Graduation Report', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'CSV'})		
		# #this is the data set we create in this script
		# gradRates = doc.entity('dat:skaram13_smedeiro#GradRates',{'prov:label':'Percent graduated for 2011', prov.model.PROV_TYPE:'ont:DataSet'})

		# # Activities			
		# get_gradRates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		# doc.wasAssociatedWith(get_gradRates, this_script)
		# # usage(activity, entity=None, time=None, identifier=None, other_attributes=None)
		# doc.usage(get_gradRates,gradRates, startTime, None,
		# 		  {prov.model.PROV_TYPE:'ont:Retrieval',
		# 		  'ont:Query':'?type=reg4_r,org_code'
		# 		  }
		# 		  )
		# doc.wasAttributedTo(gradRates, this_script)
		# doc.wasGeneratedBy(gradRates, get_gradRates, endTime)
		# doc.wasDerivedFrom(gradRates, gradReport, get_gradRates, get_gradRates, get_gradRates)


		#repo.logout()
				  
		return doc

makeStudentsGraph.execute()
