#Transformation #3
#aggregates number of parking tickets by violation 
#i.e. 20 violations with no license plate

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class agg_parking_tix(dml.Algorithm):
	contributor = 'mbyim_seanz'
	reads = ['mbyim_seanz.parking_tickets']
	writes = ['mbyim_seanz.agg_parking_tix']

	@staticmethod
	def execute(trial = False):
		#S/O To Professor Lapetz
		def aggregate(R, f):
			keys = {r for r in R}
			return [{key: f([1 for k in R if k == key])} for key in keys]

		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo

		# Connect to mongoDB
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')

		parking_tickets = repo.mbyim_seanz.parking_tickets.find()

		#make the aggregated_parking_tix
		parking_violation_data = []
		count = 0
		for row in parking_tickets:
			parking_ticket_dict = dict(row)
			parking_violation = parking_ticket_dict['violation1']
			parking_violation_data.append(parking_violation)
			count+=1
		aggregated_parking_tix_data = aggregate(parking_violation_data, sum)		
		aggregated_parking_tix_data_str = str(aggregated_parking_tix_data).replace("'",'"')	#small json fix to convert to strings

		parking_tix_json = json.loads(aggregated_parking_tix_data_str)
		s = json.dumps(parking_tix_json, sort_keys=True, indent = 2)
		repo.dropCollection("agg_parking_tix")
		repo.createCollection("agg_parking_tix")
		repo['mbyim_seanz.agg_parking_tix'].insert_many(parking_tix_json)
		repo['mbyim_seanz.agg_parking_tix'].metadata({'complete':True})

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

		this_script = doc.agent('alg:mbyim_seanz#agg_parking_tix', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource_agg_parking_tix = doc.entity('bdp:jsri-cpsq', {'prov:label':'Aggregate Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_agg_parking_tix = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_agg_parking_tix, this_script)
		doc.usage(get_agg_parking_tix, resource_agg_parking_tix, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'?format=json' #not sure what this does
		          }
		)
		agg_parking_tix = doc.entity('dat:mbyim_seanz#agg_parking_tix', {prov.model.PROV_LABEL:'Aggregate Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(agg_parking_tix, this_script)
		doc.wasGeneratedBy(agg_parking_tix, get_agg_parking_tix, endTime)

		repo.logout()
		          
		return doc

# agg_parking_tix.execute()
# doc = agg_parking_tix.provenance()

# print('finished aggregating parking tickets')











