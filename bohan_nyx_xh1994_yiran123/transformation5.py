import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class transformation5(dml.Algorithm):
	contributor = 'bohan_nyx_xh1994_yiran123'
	reads = ['bohan_nyx_xh1994_yiran123.restaurant_correlation_distance_analysis_filtered']
	writes = ['bohan_nyx_xh1994_yiran123.restaurant_score_system']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')

		restaurant_correlation_distance_analysis_filtered = repo.bohan_nyx_xh1994_yiran123.restaurant_correlation_distance_analysis_filtered.find()
		restaurant_correlation_distance_analysis_filtered = [r for r in restaurant_correlation_distance_analysis_filtered]
		#Restaurant_safety = repo.bohan_nyx_xh1994_yiran123.Restaurant_safety.find()
		#Restaurants_safety = [i for i in Restaurant_safety]
		
		repo.dropCollection("restaurant_score_system")
		repo.createCollection("restaurant_score_system")
		

		r_cleanness = []
		for i in restaurant_correlation_distance_analysis_filtered:
			#print(i)
			r_cleanness.append(float(i['clean level']))

		r_safe = []
		for i in restaurant_correlation_distance_analysis_filtered:
			r_safe.append(int(i['crimes within one km']))
		
		#print(max(r_cleanness))
		#print(r_cleanness)
		#print(type(r_cleanness[1]))
		#print(r_safe)
		#print(max(r_safe))

		total_restaurant_score = []
		#print(r_cleanness)
		safescore_max = float(max(r_safe))
		safescore_min = float(min(r_safe))
		score_1_max = max(r_safe)
		score_1_min = min(r_safe)

		for i in restaurant_correlation_distance_analysis_filtered:
			normal_safe = (i['crimes within one km'] - safescore_min)/(safescore_max-safescore_min)
			normal_score1 = (i['clean level'] - score_1_min)/(score_1_max-score_1_min)
			score = normal_score1-normal_safe+1
			insertMaterial = {'Businessname':i['Businessname'], 'location':i['location'],'overall score': score}
			repo['bohan_nyx_xh1994_yiran123.restaurant_score_system'].insert_one(insertMaterial)
			


		'''safe_score_normalize = []
		for i in range(len(r_safe)):
			safe_score = (float(r_safe[i]) - float(min(r_safe)))/(float(max(r_safe)) - float(min(r_safe)))
			safe_score_normalize.append(safe_score)
		#print(safe_score_normalize)

		score_list_1 = []
		for i in range(len(r_safe)):
			score_1 = r_cleanness[i]  - r_safe[i]
			score_list_1.append(score_1)

		score_1_normalize = []
		for i in range(len(score_list_1)):
			score_normalize = (score_list_1[i] - min(score_list_1))/(max(score_list_1) - min(score_list_1))
			score_1_normalize.append(score_normalize)
		#print(score_1_normalize)

		for i in range(len(restaurant_cleanness_level)):
			#print(restaurant_cleanness_level[i])
			insertMaterial = {'Businessname':restaurant_cleanness_level[i]['Businessname'], 'location':restaurant_cleanness_level[i]['location'], 'cleanness level': r_cleanness[i], 'safety level':r_safe[i], 'overall score': score_1_normalize[i]}
			repo['bohan_nyx_xh1994_yiran123.restaurant_score_system'].insert_one(insertMaterial)'''
    	


		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end":endTime}

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/')
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#transformation5', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		resource_restaurant_correlation = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_correlation_distance_analysis_filtered', {'prov:label':'Restaurant Correlation Distance Analysis Filtered', prov.model.PROV_TYPE:'ont:DataSet'})

		get_restaurant_ss = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_restaurant_ss, this_script)

		doc.usage(get_restaurant_ss, resource_restaurant_correlation, startTime, None,
        		  {prov.model.PROV_TYPE: 'ont:Computation'})

		restaurant_ss = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_score_system',
						{prov.model.PROV_LABEL:'Restaurant Score System',
						 prov.model.PROV_TYPE: 'ont:DataSet'})

		doc.wasAttributedTo(restaurant_ss, this_script)

		doc.wasGeneratedBy(restaurant_ss, get_restaurant_ss, endTime)

		doc.wasDerivedFrom(restaurant_ss, resource_restaurant_correlation, get_restaurant_ss, get_restaurant_ss, get_restaurant_ss)

		repo.logout
		return doc

transformation5.execute()