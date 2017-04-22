import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
from random import shuffle
from math import radians, cos, sin, asin, sqrt
class cor(dml.Algorithm):
	contributor = 'rengx_ztwu_lwj'
	reads = ["rengx_ztwu_lwj.publicschool_accessibility", "rengx_ztwu_lwj.metadata"]
	writes = ["rengx_ztwu_lwj.result"]
	
	@staticmethod
	def permute(x):
		shuffled = [xi for xi in x]
		shuffle(shuffled)
		return shuffled
	@staticmethod
	def avg(x): # Average
		return sum(x)/len(x)
	@staticmethod
	def stddev(x): # Standard deviation.
		m = cor.avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))
	@staticmethod
	def cov(x, y): # Covariance.
		return sum([(xi-cor.avg(x))*(yi-cor.avg(y)) for (xi,yi) in zip(x,y)])/len(x)
	@staticmethod
	def corr(x, y): # Correlation coefficient.
		if cor.stddev(x)*cor.stddev(y) != 0:
			return cor.cov(x, y)/(cor.stddev(x)*cor.stddev(y))
	@staticmethod
	def p(x, y):
		c0 = cor.corr(x, y)
		corrs = []
		for k in range(0, 2000):
			y_permuted = cor.permute(y)
			corrs.append(cor.corr(x, y_permuted))
		return len([c for c in corrs if abs(c) > c0])/len(corrs)
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("rengx_ztwu_lwj", "rengx_ztwu_lwj")
		pcd_db = repo.publicschool_accessibility
		pcd_find = pcd_db.find()
		pcd = []
		for i in pcd_find:
			pcd.append(i)
		#print(pcd[0])
		ss = []
		ds = []
		#print(pcd[0])
		for i in pcd:
			ss.append(i["access_score"])
			ds.append(i["dist_central"])
		correlation = cor.corr(ss, ds)
		p = cor.p(ss, ds)
		mets = repo.metadata.find()
		mets_data = [i for i in mets]
		repo.result.drop()
		mets_data.append({"Stats_Data": {"P_value": p, "correlation": correlation}})
		repo.result.insert_many(mets_data)
						
		repo.logout()
		endTime = datetime.datetime.now()
		#print("geo complete")
		return {"start":startTime, "end":endTime}
	
	
	@staticmethod


	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
		doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
		# Agent
		this_script = doc.agent('alg:rengx_ztwu_lwj#cor',{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		# Resources
		resource_publicschool_accessibility = doc.entity('dat:rengx_ztwu_lwj#publicschool_accessibility',{'prov:label': 'Number of acessibilities', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
		resource_metadata = doc.entity('dat:rengx_ztwu_lwj#metadata', {'prov:label': 'metadata',prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'json'})


		# Activities
		doStatsAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_LABEL: "Compute coorrelation and p-value between the Distance of Schools and acessibilities ", prov.model.PROV_TYPE: 'ont:Computation'})

		# Activities' Associations with Agent
		doc.wasAssociatedWith(doStatsAnalysis, this_script)

		# Record which activity used which resource
		doc.usage(doStatsAnalysis, resource_publicschool_accessibility, startTime)
		doc.usage(doStatsAnalysis, resource_metadata, startTime)

		# Result dataset entity
		result = doc.entity('dat:rengx_ztwu_lwj#result',{prov.model.PROV_LABEL: 'Statistics between accessibilities and distance', prov.model.PROV_TYPE: 'ont:DataSet'})

		doc.wasAttributedTo(result, this_script)
		doc.wasGeneratedBy(result, doStatsAnalysis, endTime)
		doc.wasDerivedFrom(result, resource_publicschool_accessibility, doStatsAnalysis, doStatsAnalysis, doStatsAnalysis)
		doc.wasDerivedFrom(result, resource_metadata, doStatsAnalysis,doStatsAnalysis, doStatsAnalysis)


		repo.logout()

		return doc
#cor.execute()
#doc = cor.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))