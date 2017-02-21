import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests 

class gradRates(dml.Algorithm):
	contributor = 'skaram13_smedeiro'
	reads = []
	writes = ['skaram13_smedeiro.gradRates']
	

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

		offset = 0
		url = 'https://data.mass.gov/resource/bvpn-kwiy.json?$limit=50000&$offset=' + str(offset)
		res = requests.get(url)
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		while (r!=[]):
			offset+=50000 
			dictOfGradRates = {}
			for item in r:
				if ('org_code' in item and 'reg4_r' in item and item['mat_id']=='5' and item['fy_code']=='2011'):
					org_code = item['org_code']
					reg4_r = item['reg4_r']
					dictOfGradRates[org_code] = (reg4_r)
			url = 'https://data.mass.gov/resource/bvpn-kwiy.json?$limit=50000&$offset=' + str(offset)
			res = requests.get(url)
			response = urllib.request.urlopen(url).read().decode("utf-8")
			r = json.loads(response)	


		repo.dropCollection("gradRates")
		repo.createCollection("gradRates")
		repo['skaram13_smedeiro.gradRates'].insert_one(dictOfGradRates)
		repo['skaram13_smedeiro.gradRates'].metadata({'complete':True})
		# print(repo['skaram13_smedeiro.GradRates'].metadata())

  

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
		# this script gets the tech rates for 2011 and the corresponding org codes
		this_script = doc.agent('alg:skaram13_smedeiro#getGradRatesByOrgCodeFor2011', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		# Entities
		# this is the data set that take the Grad reports from 
		gradReport = doc.entity('dmg:skaram13_smedeiro#Graduation-Rate-Report-by-State-by-District-by-School',{'prov:label':'Graduation Report', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})		
		#this is the data set we create in this script
		gradRates = doc.entity('dat:skaram13_smedeiro#GradRates',{'prov:label':'Regular 4-year rate percent graduated', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})

		# Activities			
		get_gradRates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_gradRates, this_script)
		# usage(activity, entity=None, time=None, identifier=None, other_attributes=None)
		doc.usage(get_gradRates,gradRates, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=reg4_r,org_code'
				  }
				  )
		doc.wasAttributedTo(gradRates, this_script)
		doc.wasGeneratedBy(gradRates, get_gradRates, endTime)
		doc.wasDerivedFrom(gradRates, gradReport, get_gradRates, get_gradRates, get_gradRates)


		repo.logout()
				  
		return doc

# gradRates.execute()
# doc = gradRates.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
#  