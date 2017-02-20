#Transformation #2
#aggregates property value by zip code
#i.e. in 02215, property value totaled 1000000
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class agg_prop_value(dml.Algorithm):
	contributor = 'mbyim_seanz'
	reads = ['mbyim_seanz.property_assessments']
	writes = ['mbyim_seanz.agg_prop_value']

	@staticmethod
	def execute(trial = False):
		#S/O To Professor Lapetz
		def aggregate(R, f):
			keys = {r[0] for r in R}
			return [{key: f([int(v) for (k,v) in R if k == key])} for key in keys]

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

		property_assessments = repo.mbyim_seanz.property_assessments.find()

		#make the aggregated_zip_data
		zip_data = []
		for row in property_assessments:
			prop_dict = dict(row)
			try:
				property_zip_code = prop_dict['zipcode']
				property_value = prop_dict['av_total']
				if property_zip_code in boston_zips:
					zip_data.append([property_zip_code, property_value])
			except:
				print('There was an error with getting a zipcode key')

		aggregated_zip_data = aggregate(zip_data, sum)		
		aggregated_zip_data_str = str(aggregated_zip_data).replace("'",'"')	#small json fix to convert to strings

		zip_jsons = json.loads(aggregated_zip_data_str)
		s = json.dumps(zip_jsons, sort_keys=True, indent = 2)
		repo.dropCollection("agg_prop_value")
		repo.createCollection("agg_prop_value")
		repo['mbyim_seanz.agg_prop_value'].insert_many(zip_jsons)
		repo['mbyim_seanz.agg_prop_value'].metadata({'complete':True})

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

		this_script = doc.agent('alg:mbyim_seanz#agg_prop_value', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource_agg_prop_value = doc.entity('bdp:jsri-cpsq', {'prov:label':'Aggregate Property Assessments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_agg_prop_value = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_agg_prop_value, this_script)
		doc.usage(get_agg_prop_value, resource_agg_prop_value, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'?format=json' #not sure what this does
		          }
		)
		agg_prop_value = doc.entity('dat:mbyim_seanz#agg_prop_value', {prov.model.PROV_LABEL:'Aggregated Property Assessments', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(agg_prop_value, this_script)
		doc.wasGeneratedBy(agg_prop_value, get_agg_prop_value, endTime)

		repo.logout()
		          
		return doc


agg_prop_value.execute()
doc = agg_prop_value.provenance()











