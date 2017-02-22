import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import time
import string
import csv
class getTechRate(dml.Algorithm):
	contributor = 'skaram13_smedeiro'
	reads = []
	writes = ['skaram13_smedeiro.techRates']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

			# print("Parsing CSV...")
		dictOfTechRates = {}	
		with open('technology.csv', 'r') as f:
			read_data = csv.reader(f)
			for entry in read_data:
				# org_code = ('{0:0>8}'.format(entry[0]))
				org_code = entry[0]
				name = entry[1][:6]
				# print (name)
				# print (entry[0], 'hi', ('{0:0>8}'.format(entry[0])))
				tech_stu_per_comp = entry[2]
				if (name == 'Boston'):
					dictOfTechRates[org_code] = (tech_stu_per_comp)

				
		
		repo.dropCollection("techRates")
		repo.createCollection("techRates")
		# print (dictOfTechRates )
		repo['skaram13_smedeiro.techRates'].insert_one(dictOfTechRates)
		repo['skaram13_smedeiro.techRates'].metadata({'complete':True})
		# # print(repo['skaram13_smedeiro.techRates'].metadata())

  

		# repo.logout()

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
		this_script = doc.agent('alg:skaram13_smedeiro#getTechRatesByOrgCodeFor2011', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		# Entities
		# this is the data set that take the tech reports from 
		techReport = doc.entity('dmg:skaram13_smedeiro#Technology-Report-by-State-by-School',{'prov:label':'Tech Report', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'csv'})		
		#this is the data set we create in this script
		techRates = doc.entity('dat:skaram13_smedeiro#techRates',{'prov:label':'The percentage of students for each computer in the classroom by org_code', prov.model.PROV_TYPE:'ont:DataSet'})

		#Activities			
		get_techRates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_techRates, this_script)
		# usage(activity, entity=None, time=None, identifier=None, other_attributes=None)
		doc.usage(get_techRates,techRates, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=tech_stu_per_comp_cnt,org_code'
				  }
				  )

		doc.wasAttributedTo(techRates, this_script)
		doc.wasGeneratedBy(techRates, get_techRates, endTime)
		doc.wasDerivedFrom(techRates, techReport, get_techRates, get_techRates, get_techRates)

		repo.logout()
				  
		return doc

# getTechRate.execute()
# doc = techRates.provenance()
# # print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


# offset = 0
		# url = 'https://data.mass.gov/resource/x3d8-cquh.json?$limit=50000&$offset=' + str(offset)
		# res = requests.get(url)
		# response = urllib.request.urlopen(url).read().decode("utf-8")
		# r = json.loads(response)
		# while (r!=[]):
			# print ('here')
			# offset+=50000
			# dictOfTechRates = {}
			# for item in r:
				# if ('tech_stu_per_comp_cnt' in item and 'org_code' in item and item['fy_code']=='2011'):
					# org_code = item['org_code']
					# tech_stu_per_comp_cnt = item['tech_stu_per_comp_cnt']
					# dictOfTechRates[org_code] = (tech_stu_per_comp_cnt)
			# url = 'https://data.mass.gov/resource/x3d8-cquh.json?$limit=50000&$offset=' + str(offset)
			# time.sleep(3)
			# res = requests.get(url)
			# response = urllib.request.urlopen(url).read().decode("utf-8")
			# r = json.loads(response)

#  