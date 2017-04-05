import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt
import matplotlib.pyplot as plt

class kmeans(dml.Algorithm):
	contributor = 'rengx_ztwu_lwj'
	reads = ["rengx_ztwu_lwj.publicschool"]
	writes = ["rengx_ztwu_lwj.centrals"]
	@staticmethod
	def dist(p, q):
		(x1,y1) = p
		(x2,y2) = q
		return (x1-x2)**2 + (y1-y2)**2
	@staticmethod
	def plus(args):
		p = [0,0]
		for (x,y) in args:
			p[0] += x
			p[1] += y
		return tuple(p)
	@staticmethod
	def scale(p,c):
		(x,y) = p
		return (x/c, y/c)
	@staticmethod 
	def product(R,S):
		return [(t,u) for t in R for u in S]
	@staticmethod
	def aggregate(R,f):
		keys = {r[0] for r in R}
		return [(key, f([v for (k,v) in R if k == key])) for key in keys]
	
	@staticmethod	
	def kmeanF(pcd):
		#print(pcd[0])
		P = []
#		xs = []
#		ys = []
		for i in pcd:
			x = int(i["x"])
			y = int(i["y"])
#			xs.append(x)
#			ys.append(y)
			P.append((x,y))
		#plt.plot(xs,ys, "bo")
		M = [(42350500, 71145500), (42265300, 71126800), (42287500, 71076400), (42231300, 71091500), (42332900, 71049900), (42378600, 71039900)]
		OLD = []
		while OLD != M:
			OLD = M
			MPD = [(m, p, kmeans.dist(m,p)) for (m, p) in kmeans.product(M, P)]
			PDs = [(p, kmeans.dist(m,p)) for (m, p, d) in MPD]
			PD = kmeans.aggregate(PDs, min)
			MP = [(m, p) for ((m,p,d), (p2,d2)) in kmeans.product(MPD, PD) if p==p2 and d==d2]
			MT = kmeans.aggregate(MP, kmeans.plus)
			M1 = [(m, 1) for ((m,p,d), (p2,d2)) in kmeans.product(MPD, PD) if p==p2 and d==d2]
			MC = kmeans.aggregate(M1, sum)
			M = [kmeans.scale(t,c) for ((m,t),(m2,c)) in kmeans.product(MT, MC) if m == m2]
			sorted(M)
#		print(M)
#		xs = []
#		ys = []
#		for i in M:
#			xs.append(i[0])
#			ys.append(i[1])
#		plt.plot(xs,ys,"ro")
#		plt.show()
		return M
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
				
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

		publicschool_find = repo.publicschool.find()
		pcd = []
		for i in publicschool_find:
			pcd.append(i)
		points = kmeans.kmeanF(pcd)
		#print(points)
		res = []
		for t in points:
			item = {}
			i, j = t
			i = str(i)
			j = str(j)
			p = []
			p.append(i)
			p.append(j)
			item["coords"] = p
			res.append(item)
		repo.centrals.drop()
		repo.centrals.insert_many(res)
		
		repo.logout()
		endTime = datetime.datetime.now()
		#print("kmeans complete")
		return {"start": startTime, "end": endTime}
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return doc
kmeans.execute()