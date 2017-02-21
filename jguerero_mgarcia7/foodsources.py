# Union of cornerstores, farmersmarkets, supermarkets to get food in general

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class foodsources(dml.Algorithm):
	contributor = 'jguerero_mgarcia7'
	reads = ['jguerero_mgarcia7.farmersmarkets', 'jguerero_mgarcia7.supermarkets', 'jguerero_mgarcia7.allcornerstores']
	writes = ['jguerero_mgarcia7.foodsources']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

		#cursors for all information in datasetss
		farmersmarkets_data_cursor = repo['jguerero_mgarcia7.farmersmarkets'].find()
		supermarkets_data_cursor = repo['jguerero_mgarcia7.supermarkets'].find()
		cornerstores_data_cursor = repo['jguerero_mgarcia7.allcornerstores'].find()

		combined_dataset = []
		temp = {'Neighborhood': 0, 'Type': 0, 'Name': 0, 'Address': 0, 'Zipcode': 0, 'Coordinates': 0} 

		#farmers market data extraction
		count = 0
		for i in farmersmarkets_data_cursor:
			temp = {key:0 for key in temp}
			if i.get('area') == 'South End/Chinatown':
				temp['Neighborhood'] = 'Chinatown'
			elif i.get('area') == 'Dorchester Ctr':
				temp['Neighborhood'] = 'Dorchester'
			else:
				temp['Neighborhood'] = i.get('area')
			temp['Type'] = 'Farmers Market'
			temp['Zipcode'] = i.get('zip_code')
			temp['Coordinates'] = i.get('coordinates')
			if (count < 29):
				temp['Name'] = i.get('name')
				temp['Address'] = i.get('location')
			else:
				temp['Name'] = i.get('location')
				temp['Address'] = i.get('location_1_location')

			combined_dataset.append(temp)
			count += 1

		#supermarkets data extraction
		for j in supermarkets_data_cursor:
			temp = {key:0 for key in temp}
			if j.get('neighborhood') == 'South End/Chinatown':
				temp['Neighborhood'] = 'Chinatown'
			elif j.get('neighborhood') == 'Dorchester Ctr':
				temp['Neighborhood'] = 'Dorchester'
			else:
				temp['Neighborhood'] = j.get('neighborhood')
			temp['Type'] = 'Supermarkets'
			temp['Name'] = j.get('store')
			temp['Address'] = j.get('address')
			temp['Zipcode'] = None
			temp['Coordinates'] = None

			combined_dataset.append(temp)

		#cornerstores data extraction
		for k in cornerstores_data_cursor:
			temp = {key:0 for key in temp}
			if k.get('area') == 'South End/Chinatown':
				temp['Neighborhood'] = 'Chinatown'
			elif k.get('area') == 'Dorchester Ctr':
				temp['Neighborhood'] = 'Dorchester'
			else:
				temp['Neighborhood'] = k.get('area')
			temp['Type'] = 'Cornerstores'
			temp['Name'] = k.get('site')
			temp['Address'] = k.get('address')
			temp['Zipcode'] = '0' + k.get('zip')
			temp['Coordinates'] = k.get('coordinates')

			combined_dataset.append(temp)


		repo.dropCollection("foodsources")
		repo.createCollection("foodsources")
		repo['jguerero_mgarcia7.foodsources'].insert_many(combined_dataset)
		repo['jguerero_mgarcia7.foodsources'].metadata({'complete':True})
		print(repo['jguerero_mgarcia7.foodsources'].metadata())

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
		repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/jguereo_mgarcia7') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:jguerero_mgarcia7#foodsources', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		allcornerstores_resource = doc.entity('dat:allcornerstores', {'prov:label':'Corner Stores', prov.model.PROV_TYPE:'ont:DataSet'})
		supermarkets_resource = doc.entity('dat:supermarkets', {'prov:label':'Supermarkets', prov.model.PROV_TYPE:'ont:DataSet'})
		farmersmarkets_resource = doc.entity('dat:farmersmarkets', {'prov:label':'Farmers Markets', prov.model.PROV_TYPE:'ont:DataSet'})


		get_foodsources = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_foodsources, this_script)
		doc.usage(get_foodsources, allcornerstores_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
		doc.usage(get_foodsources, supermarkets_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )
		doc.usage(get_foodsources, farmersmarkets_resource, startTime, None,
		  {prov.model.PROV_TYPE:'ont:Computation'}
		  )

		foodsources = doc.entity('dat:jguerero_mgarcia7#foodsources', {prov.model.PROV_LABEL:'Sources of food per neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(foodsources, this_script)
		doc.wasGeneratedBy(foodsources, get_foodsources, endTime)

		doc.wasDerivedFrom(foodsources, allcornerstores_resource, get_foodsources, get_foodsources, get_foodsources)
		doc.wasDerivedFrom(foodsources, supermarkets_resource, get_foodsources, get_foodsources, get_foodsources)
		doc.wasDerivedFrom(foodsources, farmersmarkets_resource, get_foodsources, get_foodsources, get_foodsources)




		repo.logout()
				  
		return doc


## eof

