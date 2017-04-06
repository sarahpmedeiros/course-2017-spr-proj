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
	writes = ["rengx_ztwu_lwj.centrals", "rengx_ztwu_lwj.kmeans"]
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
		for s in pcd:
			cid = 0
			x = int(s["x"])
			y = int(s["y"])
			mindist = kmeans.dist((x,y), M[0])
			for c in M:
				dis = kmeans.dist((x,y),c)
				if dis < mindist:
					cid = M.index(c)
					mindist = dis
			s["cid"] = str(cid)			
#		print(M)
#		xs = []
#		ys = []
#		for i in M:
#			xs.append(i[0])
#			ys.append(i[1])
#		plt.plot(xs,ys,"ro")
#		plt.show()
		return pcd, M
	
	@staticmethod
	def draw(pcd, points):
		x1 = []
		x2 = []
		x3 = []
		x4 = []
		x5 = []
		y1 = []
		y2 = []
		y3 = []
		y4 = []
		y5 = []
		for i in pcd:
			x = int(i["x"])
			y = int(i["y"])
			cid = int(i["cid"])
			if(cid == 0):
				x1.append(x)
				y1.append(y)	
			elif(cid == 1):
				x2.append(x)
				y2.append(y)	
			elif(cid == 2):
				x3.append(x)
				y3.append(y)	
			elif(cid == 3):
				x4.append(x)
				y4.append(y)	
			elif(cid == 4):
				x5.append(x)
				y5.append(y)
		#print(x1, y1, x2, y2, x3, y3, x4, y4, x5, y5)	
		xs = []
		ys = []
		plt.title("Clusters of Public Schools")
		plt.plot(x1, y1, "go")
		plt.plot(x2, y2, "bo")
		plt.plot(x3, y3, "mo")
		plt.plot(x4, y4, "yo")
		plt.plot(x5, y5, "co")	
		for i in points:
			xs.append(i[0])
			ys.append(i[1])
			plt.plot(xs,ys,"r*")
		plt.show()
			
		
		
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
		if trial:
			pcd = pcd[:50]
		pcd, points = kmeans.kmeanF(pcd)
		#print(pcd[0])
		#draw
		DRAW = False
		if(DRAW):
			kmeans.draw(pcd, points)
		
		repo.kmeans.drop()
		repo.kmeans.insert_many(pcd)
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
		# Set up the database connection
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')
		
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
		#doc.add_namespace('mas', 'https://data.mass.gov')
		doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
		
		this_script = doc.agent('alg:rengx_ztwu_lwj#kmeans', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		publicschool_resource = doc.entity('bdp:492y-i77g', {'prov:label': 'Public School', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
		findSchoolCentroid = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,{prov.model.PROV_LABEL: "Use K-means algorithm to find schools's centroid coordinates"})

		doc.wasAssociatedWith(findSchoolCentroid, this_script)

		doc.usage(findSchoolCentroid, publicschool_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

		# Result dataset entities
		SchoolCentroid = doc.entity('dat:rengx_ztwu_lwj#SchoolCentroid',{prov.model.PROV_LABEL: 'Centroid coordinates of schools', prov.model.PROV_TYPE: 'ont:DataSet'})

		doc.wasAttributedTo(SchoolCentroid, this_script)
		doc.wasGeneratedBy(SchoolCentroid, findSchoolCentroid, endTime)
		doc.wasDerivedFrom(SchoolCentroid, publicschool_resource, findSchoolCentroid, findSchoolCentroid, findSchoolCentroid)

		repo.logout()

		return doc
#kmeans.execute()
#doc = kmeans.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))