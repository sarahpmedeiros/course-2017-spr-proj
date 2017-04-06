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
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return doc
#cor.execute()