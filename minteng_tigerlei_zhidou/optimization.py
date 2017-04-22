import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
import time


class optimization(dml.Algorithm):
	contributor = 'minteng_tigerlei_zhidou'
	reads = ['minteng_tigerlei_zhidou.box_count']
	writes = ['minteng_tigerlei_zhidou.optimization_result']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')


		# user will set the grade they want
		transport=3
		food=4
		safety=3
		rent=4

		#find the fitted area
		def if_fitted(A,requirement):#[t,f,s,r] is the requirment/standred
			[t1,f1,s1,r1]=A
			[t,f,s,r]=requirement
			if r1=='Not found':
				return False
			if t1>=t and f1>=f and s1>=s and r1>=r:
				return True
			return False
		def get_dist(A,requirement):
			[t1,f1,s1,r1]=A
			[t,f,s,r]=requirement
			if r1=='Not found':
				return 1000
			return ((t1-t)**2+(f1-f)**2+(s1-s)**2+(r1-r)**2)**0.5

		res=[]    
		a=repo['minteng_tigerlei_zhidou.box_count'].find()
		for i in a:
			grade=[i['grade']['transport'],i['grade']['food'],i['grade']['safety'],i['grade']['rent']]
			if if_fitted(grade,[transport,food,safety,rent]):
				temp=i
				temp['rating']=sum(i['grade'].values())
				res.append(temp)
			else:
				temp=i
				temp['rating']=get_dist(grade,[transport,food,safety,rent])*-1
				res.append(temp)


		#return top fitted
		result=sorted(res, key=lambda x: x['rating'], reverse=True)

		print('Top fitted areas:')
		top=5
		for i in range(top):
			print("Bound:",result[i]['box'])
			print("Area:",result[i]['area'],result[i]['postal_code'],"   Avg rent:",result[i]['avg_rent'])
			print("Grades:",result[i]['grade'],'\n')

		repo.dropCollection("optimization_result")
		repo.createCollection("optimization_result")
		repo['minteng_tigerlei_zhidou.optimization_result'].insert_many(result)

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
		repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:minteng_tigerlei_zhidou#optimization_result', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		### data : optimization_result
		#derive from location
		optimization_result_resource = doc.entity('dat:minteng_tigerlei_zhidou#box_count', {'prov:label':'box count and grade', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		optimization_result = doc.entity('dat:minteng_tigerlei_zhidou#optimization_result', {prov.model.PROV_LABEL:'optimization result', prov.model.PROV_TYPE:'ont:DataSet'})

		get_optimization_result = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_optimization_result, this_script)
		doc.usage(get_optimization_result, optimization_result_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})

		doc.wasAttributedTo(optimization_result, this_script)
		doc.wasGeneratedBy(optimization_result, get_optimization_result, endTime)
		doc.wasDerivedFrom(optimization_result, optimization_result_resource, get_optimization_result, get_optimization_result, get_optimization_result)
		repo.logout()

		return doc

# optimization.execute()
# doc = optimization.provenance()
# #print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))