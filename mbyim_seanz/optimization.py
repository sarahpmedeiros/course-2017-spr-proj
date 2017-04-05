# Project #2

# This class is used for a constraint satisfaction problem:
# problem: is it possible to find a w,r,t such that a person would be indifferent to living at one location vs another?
# if it's satisfiable, then the weights that we find are the weights that a person would be indifferent for

# i.e. [w = 37/920, t = 7/460, r = 41/920000] means that someone who would indifferent to living in one location vs another in this set would have to be someone who values walking more than the other two

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import z3
from math import radians, cos, sin, asin, sqrt
import sys
import random

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
		# Kendall Square -- hardcoded location
		company_lat = 42.3629
		company_lng = -71.0901

		#Optimize----------------------------------------------------------------
		res_transit_data = repo.mbyim_seanz.residential_transit.find()

		# get residential/transit data from database
		pulled_res_transit = []
		for row in res_transit_data:
			res_transit_dict = dict(row)
			
			inner_dict = {}
			parcel_id = res_transit_dict['parcel_id']
			res_transit_subdata = res_transit_dict['res_transit_data']

			inner_dict['parcel_id'] = parcel_id
			inner_dict['res_transit_data'] = res_transit_subdata

			pulled_res_transit.append(inner_dict)

		zipcodes_used = {}
		final_satisfiable_set = []
		# for each zip code region
		for data_point in pulled_res_transit:
			curr_res_zipcode = data_point['res_transit_data']['res_zipcode']
			
			#loop through unique zipcodes only
			if curr_res_zipcode in zipcodes_used:
				continue
			
			zipcodes_used[curr_res_zipcode] = 1

			# store to use in z3 optimization
			walk_times = []
			transit_times = []
			res_costs = []

			# loop through all residential properties in this zip code region
			for zip_code_data_point in pulled_res_transit:
				# check if it's in the current zip code
				if zip_code_data_point['res_transit_data']['res_zipcode'] != curr_res_zipcode:
					continue

				# otherwise start to get the data 
				res_cost = int(zip_code_data_point['res_transit_data']['cost'])
				res_address = zip_code_data_point['res_transit_data']['res_address']

				# simple initial filter because some of the Boston data was missing or inaccurate
				if res_cost < 10000 or res_address[0] == '0' or res_address == 'NULL':
					continue

				res_lat = zip_code_data_point['res_transit_data']['res_lat']
				res_lng = zip_code_data_point['res_transit_data']['res_lng']
				closest_transit_lat = zip_code_data_point['res_transit_data']['nearby_stops'][0]['stop_lat']
				closest_transit_lng = zip_code_data_point['res_transit_data']['nearby_stops'][0]['stop_lng']

				# Assume a person walks 1 km in 10 minutes on average
				walk_time = int(haversine(res_lat, res_lng, closest_transit_lat, closest_transit_lng)) * 10

				# Assume public transit travels 1 km in 5 minutes on average
				transit_time = int(haversine(closest_transit_lat, closest_transit_lng, company_lat, company_lng)) * 5

				walk_times.append(walk_time)
				transit_times.append(transit_time)
				res_costs.append(res_cost)

			# weights that we are trying to solve for
			w = z3.Real('w') 
			t = z3.Real('t') 
			r = z3.Real('r') 

			opt = z3.Solver()
			opt.add(w>0, r>0, t>0)

			# loop through the residents in the same zip code 
			for i in range(len(walk_times)):
				# provide some leeway in terms of constraint satisfaction, explained in README
				opt.add(w * walk_times[i] +  t * transit_times[i] +  r * res_costs[i] <= 1)
				opt.add(w * walk_times[i] +  t * transit_times[i] +  r * res_costs[i] > 0.99)

			# check if constraint is satisfiable
			opt_check = opt.check()
			sat = z3.CheckSatResult(z3.Z3_L_TRUE)

			# if it was satisfiable, what were the weights?
			satisfiable_set = {}
			if opt_check == sat:
				model = opt.model()

				# weight_key is w,r,t
				# model[weight_key] is the actual fraction/weight
				for weight_key in model:
					weight_value = float((model[weight_key].as_decimal(30))[:-1])
					satisfiable_set[weight_key.name()] = weight_value

				satisfiable_set["res_zipcode"] = curr_res_zipcode
				satisfiable_set["data_points"] = len(walk_times)
			
			# only prepare it for the database if it was satisfiable
			if len(satisfiable_set) != 0:
				final_satisfiable_set.append(satisfiable_set)

		# prepare for json 
		final_data_string_fixed = json.dumps(final_satisfiable_set)
		final_data_string_fixed.replace("'", '"')
		
		r = json.loads(final_data_string_fixed)
		s = json.dumps(r, sort_keys=True, indent=2)

		repo.dropCollection("optimization")
		repo.createCollection("optimization")
		repo['mbyim_seanz.optimization'].insert_many(r)
		repo['mbyim_seanz.optimization'].metadata({'complete':True})
		print(repo['mbyim_seanz.optimization'].metadata())
	
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
		resource_optimization = doc.entity('gmaps:type=transit_station', {'prov:label':'Optimize Residential Transit Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_optimization = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_optimization, this_script)

		doc.usage(get_optimization, resource_optimization, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'}
		)

		optimization = doc.entity('dat:mbyim_seanz#optimization', {prov.model.PROV_LABEL:'Optimize Residential Transit Stations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(optimization, this_script)
		doc.wasGeneratedBy(optimization, get_optimization, endTime)
		doc.wasDerivedFrom(optimization, resource_optimization, get_optimization, get_optimization, get_optimization)

		repo.logout()
		          
		return doc


# optimization.execute()
# doc = optimization.provenance()
# print('finished res transit optimization')