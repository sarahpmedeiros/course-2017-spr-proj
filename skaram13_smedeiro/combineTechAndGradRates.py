import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class techAndGradRates(dml.Algorithm):
	contributor = 'skaram13_smedeiro'
	reads = ['skaram13_smedeiro.techRates','skaram13_smedeiro.gradRates']
	writes = ['skaram13_smedeiro.gradAndTechRatesByOrgCode']
	

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		
		
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

		repo.dropCollection("gradAndTechRatesByOrgCode")
		repo.createCollection("gradAndTechRatesByOrgCode")

		techRates = repo.skaram13_smedeiro.techRates.find()
		gradRates = repo.skaram13_smedeiro.gradRates.find()
	
		dictOfTechRates={}
		for entry in techRates:
			dictOfTechRates = (entry)
		# print (dictOfTechRates)
		dictOfGradRates={}
		for entry in gradRates:
			dictOfGradRates = (entry)
		# print (dictOfGradRates)

		keysInTechDict = (dictOfTechRates.keys())
		keysInGradDict = (dictOfGradRates.keys())

		keepList = []
		for x in dictOfTechRates:
			if x in keysInGradDict:
				keepList.append(x)
		for x in dictOfGradRates:
			if x in keysInTechDict:
				keepList.append(x)

		combinedList = []
		for x in keepList:
			combinedList.append({'org_code':x,'tech_rate':dictOfTechRates[x],'grad_rate':dictOfGradRates[x]})

		# print (combinedList)
		repo['skaram13_smedeiro.gradAndTechRatesByOrgCode'].insert_many(combinedList)
		repo['skaram13_smedeiro.gradAndTechRatesByOrgCode'].metadata({'complete':True})
		# print(repo['skaram13_smedeiro.techRates'].metadata())

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
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/skaram13_smedeiro/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/skaram13_smedeiro/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('dmg', 'https://data.mass.gov/resource/')


		#Agents
		# this script gets the tech and rates for 2011 and the corresponding org codes
		this_script = doc.agent('alg:skaram13_smedeiro#combineTechAndGradRates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		# Entities
		# this is the data set from the Grad dataset   
		gradRates = doc.entity('dat:skaram13_smedeiro#gradRates',{'prov:label':'Graduation Rates', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})		
		# this is the data set from the tech dataset   
		techRates = doc.entity('dat:skaram13_smedeiro#techRates',{'prov:label':'Tech Rates', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
		gradAndTechRatesByOrgCode = doc.entity('dat:skaram13_smedeiro#gradAndTechRatesByOrgCode',{'prov:label':'Grad and Tech rates by org-code', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})

		# Activities			
		get_gradAndTechRatesByOrgCodes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_gradAndTechRatesByOrgCodes, this_script)

		# usage(activity, entity=None, time=None, identifier=None, other_attributes=None)
		doc.usage(get_gradAndTechRatesByOrgCodes,gradRates, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=reg4_r,org_code'
				  }
				  )
		doc.usage(get_gradAndTechRatesByOrgCodes,techRates, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=tech_stu_per_comp_cnt,org_code'
				  }
				  )
		doc.wasAttributedTo(gradAndTechRatesByOrgCode, this_script)
		doc.wasGeneratedBy(gradAndTechRatesByOrgCode, get_gradAndTechRatesByOrgCodes, endTime)
		doc.wasDerivedFrom(gradAndTechRatesByOrgCode, gradRates, get_gradAndTechRatesByOrgCodes, get_gradAndTechRatesByOrgCodes, get_gradAndTechRatesByOrgCodes)
		doc.wasDerivedFrom(gradAndTechRatesByOrgCode, techRates, get_gradAndTechRatesByOrgCodes, get_gradAndTechRatesByOrgCodes, get_gradAndTechRatesByOrgCodes)

		repo.logout()
				  
		return doc

# techAndGradRates.execute()
# doc = techAndGradRates.provenance()
# # print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# ## eof
# 