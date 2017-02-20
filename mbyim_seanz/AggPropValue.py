#Transformation #1


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
		#repo = openDb(getAuth("mbyim_seanz"), getAuth("mbyim_seanz"))
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')

		property_assessments = repo.mbyim_seanz.property_assessments.find()

		#print(property_assessments)


		#make the zip_grouped_data
		zip_data = []
		print("here")
		for row in property_assessments:
			prop_dict = dict(row)
			prop_dict_select = prop_dict['owner_mail_zipcode']
			print(prop_dict_select)

			zip_data.append(prop_dict_select)


		zip_grouped_data = aggregate(zip_list, sum)

		print(len(zip_list))
		print(zip_grouped_data)

		
		
		zip_grouped_data_str = str(zip_grouped_data).replace("'",'"')

		zip_jsons = json.loads(zip_grouped_data_str)
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

		this_script = doc.agent('alg:mbyim_seanz#AggPropValue', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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
#doc = agg_prop_value.provenance()











