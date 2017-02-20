#Transformation #1
#aggregates number of vehicles taxed by zip code in Boston
#i.e. 10 vehicles in 02215

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class agg_vehicle_tax(dml.Algorithm):
	contributor = 'mbyim_seanz'
	reads = ['mbyim_seanz.vehicle_tax']
	writes = ['mbyim_seanz.agg_vehicle_tax']

	@staticmethod
	def execute(trial = False):
		#S/O To Professor Lapetz
		def aggregate(R, f):
			keys = {r for r in R}
			return [{key: f([1 for k in R if k == key])} for key in keys]

		#list of boston zipcodes from:
		#http://zipcode.org/city/MA/BOSTON
		boston_zips = ["02108", "02109", "02110", "02111", "02112", "02117", "02118", "02127", "02113", "02114", "02115", "02116", "02123", 
				"02128","02133","02163","02196", "02199", "02205", "02206", "02212", "02215", "02266", "02283", 
				"02201", "02203", "02204", "02210", "02211", "02217", "02222", "02241", "02284", "02293", "02295", "02297", "02298"]


		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo

		# Connect to mongoDB
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')

		vehicle_tax = repo.mbyim_seanz.vehicle_tax.find()

		#make the vehicle_zip_aggregate
		zip_list = []
		for row in vehicle_tax:
			tax_dict = dict(row)
			tax_dict_modified = tax_dict['zip'].split("-")[0]
			if tax_dict_modified in boston_zips:
				zip_list.append(tax_dict_modified)
			else:
				continue

		vehicle_zip_aggregate = aggregate(zip_list, sum)
		vehicle_zip_aggregate_str = str(vehicle_zip_aggregate).replace("'",'"') #small json fix to convert to strings

		zip_jsons = json.loads(vehicle_zip_aggregate_str)
		s = json.dumps(zip_jsons, sort_keys=True, indent = 2)
		repo.dropCollection("agg_vehicle_tax")
		repo.createCollection("agg_vehicle_tax")

		repo['mbyim_seanz.agg_vehicle_tax'].insert_many(zip_jsons)
		repo['mbyim_seanz.agg_vehicle_tax'].metadata({'complete':True})
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')


		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/stopsbyroute')

		this_script = doc.agent('alg:mbyim_seanz#agg_vehicle_tax', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource_agg_vehicle_tax = doc.entity('bdp:ww9y-x77a', {'prov:label':'Aggregated Vehicle Tax', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


		get_agg_vehicle_tax = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_agg_vehicle_tax, this_script)

		doc.usage(get_agg_vehicle_tax, resource_agg_vehicle_tax, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'?format=json' #not sure what this does
		          }
		)


		agg_vehicle_tax = doc.entity('dat:mbyim_seanz#agg_vehicle_tax', {prov.model.PROV_LABEL:'Aggregated Vehicle Tax', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(agg_vehicle_tax, this_script)
		doc.wasGeneratedBy(agg_vehicle_tax, get_agg_vehicle_tax, endTime)

		repo.logout()
		          
		return doc


agg_vehicle_tax.execute()
doc = agg_vehicle_tax.provenance()
print('finished aggregating vehicle tax')











