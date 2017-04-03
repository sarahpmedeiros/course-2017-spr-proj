#Statistical Analysis
#Conducts a regression and generates a graph
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from sklearn import datasets, linear_model
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import pandas as pd

class StatisticalAnalysis(dml.Algorithm):
	contributor = 'mbyim_seanz'
	reads = ['mbyim_seanz.property_assessments']
	writes = ['mbyim_seanz.StatisticalAnalysis']

	@staticmethod
	def execute(trial = False):

		#list of boston zipcodes from: http://zipcode.org/city/MA/BOSTON
		boston_zips = ["02108", "02109", "02110", "02111", "02112", "02117", "02118", "02127", "02113", "02114", "02115", "02116", "02123", 
						"02128","02133","02163","02196", "02199", "02205", "02206", "02212", "02215", "02266", "02283", 
						"02201", "02203", "02204", "02210", "02211", "02217", "02222", "02241", "02284", "02293", "02295", "02297", "02298"]

		client = dml.pymongo.MongoClient()
		repo = client.repo


		# Connect to mongoDB
		repo.authenticate('mbyim_seanz', 'mbyim_seanz')

		#Grab Property_Assessment Data and prep for multiple regression
		property_assessments = repo.mbyim_seanz.property_assessments.find()


		###########DATA PREP###########################################################################################

		#List of values that are residential
		residential_properties = ["R1", "R2", "R3", "R4", "A", "CD"]
		#List of styles
		residential_styles = ["BL", "BW", "CL", "CN", "CP", "CV", "DK", "DX", "L", "RE", "RM", "RN", "RR", "SL", "TF", "TD", "SD", "VT", "OT"]

		regression_data = []
		price_data = []
		for row in property_assessments:

			reg_dict = dict(row)

			try:
				empty_zip_dummys = [0 for x in range(0,len(boston_zips))]
				index = 0
				for zipcode in boston_zips:
					index += 1
					if reg_dict['zipcode'] == zipcode:
						empty_zip_dummys[index] = 1
						break
				property_zip_code = empty_zip_dummys

			except:
				continue
			try:
				property_value = int(reg_dict['av_bldg'])
			except:
				continue
			try:
				property_rooms = int(reg_dict['r_total_rms'])
			except:
				continue
			try:
				property_brooms = int(reg_dict['r_bdrms'])
			except:
				continue
			try:
				property_full_baths = int(reg_dict['r_full_bth'])
			except:
				continue 

			try:
				property_bldg_area = int(reg_dict['living_area'])
			except:
				continue
			try:
				style_list = [0 for x in range(0,len(residential_styles))]
				index = 0
				for style in residential_properties:
					index += 1
					if reg_dict['r_bldg_styl'] == style:
						style_list[index] = 1
						break

				property_building_style = style_list
			except:
				continue 
			try:
				property_year = int(reg_dict['yr_built'])
			except:
				continue

			observation = [property_rooms] + [property_brooms] + [property_full_baths] + [property_bldg_area] + [property_year]


			regression_data.append(observation)
			price_data.append(property_value)



		######CREATE MODEL###################################################################################

		regression_data = np.array(regression_data)
		price_data = np.array(price_data)

		results = sm.OLS(price_data, regression_data).fit()


		rsquared = results._results.rsquared.tolist()
		params = results._results.params.tolist()
		conf_int = results._results.conf_int().tolist()

		pvalues = results._results.pvalues.tolist()
		tvalues = results._results.tvalues.tolist()

		stats_jsons = {}
		stats_jsons['rsquared'] = rsquared
		stats_jsons['params'] = params
		stats_jsons['conf_int'] = conf_int
		stats_jsons['pvalues'] = pvalues
		stats_jsons['tvalues'] = tvalues
		

		stats_jsons = [stats_jsons]
		string_fix = json.dumps(stats_jsons)
		string_fix.replace("'", '"')

		r = json.loads(string_fix)
		s = json.dumps(r, sort_keys=True, indent = 2)
		repo.dropCollection("StatisticalAnalysis")
		repo.createCollection("StatisticalAnalysis")
		repo['mbyim_seanz.StatisticalAnalysis'].insert_many(r)
		repo['mbyim_seanz.StatisticalAnalysis'].metadata({'complete':True})


		#####Graph Style by Year##############################################################################

		data_set = repo.mbyim_seanz.property_assessments.find()
		year_area = []
		for row in data_set:
			reg_dict = dict(row)

			try:
				prop_year = int(reg_dict['yr_built'])
				if (prop_year < 1850) or (prop_year > 2010):
					continue
			except:
				continue
			try:
				floors = int(reg_dict['num_floors']) #living_area
				if (floors == 0):
					continue
			except:
				continue

			year_area_obs = [prop_year] + [floors]

			year_area.append(year_area_obs)



		df = pd.DataFrame(year_area, columns=['x', 'y'])
		df = df.groupby(['x']).mean()
		df.reset_index(level=0, inplace=True)
		df.plot(x='x', y='y', title='Average Number of Floors for Buildings Constructed By Year',legend=None)
		plt.xlabel('Year')
		plt.ylabel('Average Number of Floors')
		plt.savefig('number_floors_graph')
	

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


		this_script = doc.agent('alg:mbyim_seanz#StatisticalAnalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource_statistical_analysis = doc.entity('bdp:jsri-cpsq', {'prov:label':'Aggregate Property Assessments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_statistical_analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_statistical_analysis, this_script)
		
		doc.usage(get_statistical_analysis, resource_statistical_analysis, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'?format=json' #not sure what this does
		          }
		)
		
		StatisticalAnalysis = doc.entity('dat:mbyim_seanz#StatisticalAnalysis', {prov.model.PROV_LABEL:'Statistical Analysis', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(StatisticalAnalysis, this_script)
		doc.wasGeneratedBy(StatisticalAnalysis, get_statistical_analysis, endTime)

		repo.logout()
		          
		return doc







