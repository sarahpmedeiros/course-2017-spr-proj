import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class restaurant_score(dml.Algorithm):
	contributor = 'bohan_nyx_xh1994_yiran123'
	reads = ['bohan_nyx_xh1994_yiran123.restaurant_cleanness_level', 'bohan_nyx_xh1994_yiran123.Restaurant_safety']
	writes = ['bohan_nyx_xh1994_yiran123.restaurant_score_system']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')

		restaurant_cleanness_level = repo.bohan_nyx_xh1994_yiran123.restaurant_cleanness_level.find()
		restaurant_cleanness_level = [r for r in restaurant_cleanness_level]
		Restaurant_safety = repo.bohan_nyx_xh1994_yiran123.Restaurant_safety.find()
		Restaurants_safety = [i for i in Restaurant_safety]
		
		repo.dropCollection("restaurant_score_system")
		repo.createCollection("restaurant_score_system")
		

		r_cleanness = []
		for i in restaurant_cleanness_level:
			r_cleanness.append(float(i['cleanness level']))

		r_safe = []
		for i in Restaurant_safety:
			r_safe.append(int(i['crime incidents number within akm']))
		
		#print(max(r_cleanness))
		#print(r_cleanness)
		#print(type(r_cleanness[1]))
		#print(r_safe)
		#print(max(r_safe))

		total_restaurant_score = []



		safe_score_normalize = []
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

		for i in restaurant_cleanness_level:
			insertMaterial = {'Businessname':i['businessname'], 'location':i['location'], 'cleanness level': r_cleanness[i], 'safety level':r_safe[i], 'overall score': score_1_normalize[i]}
			repo['bohan_nyx_xh1994_yiran123.restaurant_score_system'].insert_one(insertMaterial)
    	


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

		this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#restaurant_score', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		r_safety = doc.entity('dat:bohan_nyx_xh1994_yiran123', {'prov:label':'Restaurant Score System', prov.model.PROV_TYPE:'ont:DataSet'})

		r_cleann = doc.entity('dat:bohan_nyx_xh1994_yiran123', {'prov:label': 'Restaurant Score System', prov.model.PROV_TYPE: 'ont:DataSet'})
		get_safety = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_safety, this_script)

		doc.usage(get_safety, r_safety, startTime, None,
        		  {prov.model.PROV_TYPE: 'ont:Computation'})

		R_S = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_score',
						{prov.model.PROV_LABEL:'Restaurant Score System',
						 prov.model.PROV_TYPE: 'ont:DataSet'})

		doc.wasAttributedTo(R_S, this_script)

		doc.wasGeneratedBy(R_S, get_safety, endTime)

		doc.wasDerivedFrom(R_S, r_safety, get_safety, get_safety, get_safety)
		doc.wasDerivedFrom(R_S, r_cleann, get_safety, get_safety, get_safety)


		repo.logout()
		return doc

