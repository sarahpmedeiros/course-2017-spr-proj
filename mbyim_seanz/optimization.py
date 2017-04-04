# Project #2
# This class is used to optimize constraints:

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from z3 import *
from math import radians, cos, sin, asin, sqrt
import sys

class optimization(dml.Algorithm):
	contributor = 'mbyim_seanz'
	reads = ['mbyim_seanz.residential_transit']
	writes = ['mbyim_seanz.optimization']

	@staticmethod
	def execute(trial = False):
		#http://stackoverflow.com/questions/15736995/how-can-i-quickly-estimate-the-distance-between-two-latitude-longitude-points
		def haversine(lat1, lon1, lat2, lon2):
		    """
		    Calculate the great circle distance between two points 
		    on the earth (specified in decimal degrees)
		    """
		    # convert decimal degrees to radians 
		    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
		    # haversine formula 
		    dlon = lon2 - lon1 
		    dlat = lat2 - lat1 
		    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
		    c = 2 * asin(sqrt(a)) 
		    km = 6367 * c
		    return km
		    
		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo

		# Connect to mongoDB
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')

		# Constraints----------------------------------------------------------------
		# Kendall Square -- hardcoded company location
		company_lat = 42.3629
		company_lng = -71.0901

		best_min = {
			'obj_fn_evaluation': float('inf'),
			'walk_time': 0,
			'transit_time': 0,
			'res_cost': 0,
			'stop_name': '',
			'res_address': ''
		}
		parcel_id_test = ''
		stop_lat_test = ''
		stop_lng_test = ''
		# has to add up to 100 %
		walk_priority = 0.15
		transit_priority = 0.35
		res_cost_priority = 0.50

		#Optimize----------------------------------------------------------------
		res_transit_data = repo.mbyim_seanz.residential_transit.find()

		final_res_transit = []
		for row in res_transit_data:
			res_transit_dict = dict(row)
			
			inner_dict = {}
			parcel_id = res_transit_dict['parcel_id']
			res_transit_subdata = res_transit_dict['res_transit_data']

			inner_dict['parcel_id'] = parcel_id
			inner_dict['res_transit_data'] = res_transit_subdata

			final_res_transit.append(inner_dict)

		#for each residential property
		for i in range(len(final_res_transit)):
			res_lat = final_res_transit[i]['res_transit_data']['res_lat']
			res_lng = final_res_transit[i]['res_transit_data']['res_lng']
			res_cost = float(final_res_transit[i]['res_transit_data']['cost'])
			res_address = final_res_transit[i]['res_transit_data']['res_address']
			parcel_id_temp = final_res_transit[i]['parcel_id']
			nearby_stops = final_res_transit[i]['res_transit_data']['nearby_stops']

			if res_cost < 10000 or res_address[0] == '0' or res_address == 'NULL':
				continue

			#walk times, assume person walks 1 km in 10 minutes on average
			walk_times = []

			# for each nearby stop, get the walking distance
			for j in range(len(nearby_stops)):
				transit_lat = nearby_stops[j]['stop_lat']
				transit_lng = nearby_stops[j]['stop_lng']
				stop_name = nearby_stops[j]['stop_name']

				walk_time = haversine(res_lat, res_lng, transit_lat, transit_lng) * 10

				single_dict = {
					'walk_time': walk_time,
					'stop_name': stop_name
				}
				walk_times.append(single_dict)

			#transit times, assume transit travels 1 km in 5 minutes on average
			transit_times = []

			# for each transit station, get the transit distance to the company
			for j in range(len(nearby_stops)):
				transit_lat = nearby_stops[j]['stop_lat']
				transit_lng = nearby_stops[j]['stop_lng']

				transit_time = haversine(transit_lat, transit_lng, company_lat, company_lng) * 5
				transit_times.append(transit_time)



			# print('----walk times')
			# print(walk_times)
			# print('transit times----')
			# print(transit_times)
			# print('residential cost----')
			# print(res_cost)

			for j in range(len(walk_times)):
				walk_time = float(walk_times[j]['walk_time'])
				stop_name = walk_times[j]['stop_name']
				transit_time = float(transit_times[j])

				obj_fn_evalutation = (walk_time * walk_priority) + (transit_time * transit_priority) + (res_cost * res_cost_priority)

				if obj_fn_evalutation < best_min['obj_fn_evaluation']:
					best_min['obj_fn_evaluation'] = obj_fn_evalutation
					best_min['walk_time'] = walk_time
					best_min['transit_time'] = transit_time
					best_min['res_cost'] = res_cost
					best_min['res_address'] = res_address
					best_min['stop_name'] = stop_name
					parcel_id_test = parcel_id_temp

		print(best_min)
		print(parcel_id_test)

		# final_data_string_fixed = json.dumps(final_data_set)
		# final_data_string_fixed.replace("'", '"')
		# print('done')
		
		# r = json.loads(final_data_string_fixed)
		# s = json.dumps(r, sort_keys=True, indent=2)

		# repo.dropCollection("residential_transit")
		# repo.createCollection("residential_transit")
		# repo['mbyim_seanz.residential_transit'].insert_many(r)
		# repo['mbyim_seanz.residential_transit'].metadata({'complete':True})
		# print(repo['mbyim_seanz.residential_transit'].metadata())
	
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
		doc.add_namespace('gmaps', 'https://maps.googleapis.com/maps/api/place/nearbysearch/json')

		this_script = doc.agent('alg:mbyim_seanz#optimization', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource_res_transit = doc.entity('gmaps:type=transit_station', {'prov:label':'Residential Transit Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_res_transit = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_res_transit, this_script)

		doc.usage(get_res_transit, resource_res_transit, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'?format=json' #not sure what this does
		          }
		)

		res_transit = doc.entity('dat:mbyim_seanz#res_transit', {prov.model.PROV_LABEL:'Residential Transit Stations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(res_transit, this_script)
		doc.wasGeneratedBy(res_transit, get_res_transit, endTime)

		repo.logout()
		          
		return doc


optimization.execute()
# doc = optimization.provenance()
print('finished res transit optimization')