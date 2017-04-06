import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
import sodapy
class union(dml.Algorithm):
	contributor = 'rengx_ztwu_lwj'
	reads = ["rengx_ztwu_lwj.market"]
	writes = ["rengx_ztwu_lwj.access"]
	@staticmethod
	def unionF(hospital_data, garden_data, market_data, police_data):
		access = []
		count = 0
		for i in range(len(hospital_data)):
			try:
				item = {}
				item['addr'] = hospital_data[i]['ad']
				item['type'] = 'hospital'
				#x, y len=8 (string)
				item['y'] = hospital_data[i]['xcoord']
				item['x'] = hospital_data[i]['ycoord']
				#database gets the reversed xcoord and ycoord
				access.append(item)
			except:
				count += 1
				#print(count, " mistakes occured.")
				continue
					 #24

		for i in range(1,len(garden_data)):         
			try:
				item = {}
				addr = garden_data[i]['location']
				if(addr[-1:] == "\xa0"):
					addr = addr[:-1]
				item["addr"] = addr.upper()
				item['type'] = 'garden'
				coord = garden_data[i]['coordinates']
				coords = coord.split(",")
				xcoor = coords[0].split(".")
				ycoor = coords[1].split(".")
				xcoord = xcoor[0] + xcoor[1]
				ycoord = ycoor[0][1:] + ycoor[1]
				while(len(xcoord) < 8):
					xcoord += "0"
				while(len(ycoord) < 8):
					ycoord += "0"
				if(len(xcoord) > 8):
					xcoord = xcoord[:8]
				if(len(ycoord) > 8):
					ycoord = ycoord[:8]
				item["x"] = xcoord
				item["y"] = ycoord
				access.append(item)
			except:
				count += 1
				#print(count, " mistakes occured.")
				continue
				 #182

		for i in range(len(market_data)):
			try:
				item = {}
				addr = market_data[i]['addr_1']
				item['addr'] = addr.upper()
				item['type'] = 'market'
				coords = market_data[i]['location']['coordinates']
				#databse gets the reversed xcoord and ycoord
				xcoor = str(coords[1]).split(".")
				ycoor = str(coords[0]).split(".")
				xcoord = xcoor[0]+xcoor[1]
				ycoord = ycoor[0][1:]+ycoor[1]
				while(len(xcoord) < 8):
					xcoord += "0"
				while(len(ycoord) < 8):
					ycoord += "0"
				if(len(xcoord) > 8):
					xcoord = xcoord[:8]
				if(len(ycoord) > 8):
				 	ycoord = ycoord[:8]
				item["x"] = xcoord
				item["y"] = ycoord
				access.append(item)
			except:
				count += 1
				#print(count, " mistakes occured.")
				continue
							
		for i in range(len(police_data)):
			item = {}
			coords = police_data[i]['location']['coordinates']
			xcoor = str(int(coords[1]*1000000))
			ycoor = str(int(coords[0]*(-1000000)))
			item["x"] = xcoor
			item["y"] = ycoor
			item["type"] = "police"
			item["addr"] = police_data[i]['location_location']
			access.append(item)

					
			#288
		return access
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("rengx_ztwu_lwj", "rengx_ztwu_lwj")
		market = repo.market
		market_find = market.find()
		market_data = []
		for i in market_find:
			market_data.append(i)
					
		ss = sodapy.Socrata("data.cityofboston.gov", "x92LG4iaFto5qWQGFk3lDdv6p", username="lwj@bu.edu",password = "KatrinaLu2017")
		response = ss.get("u6fv-m8v4")
		r = json.loads(json.dumps(response, sort_keys=True, indent=2))
		hospital_data = r #26			
		response = ss.get("rdqf-ter7")
		r = json.loads(json.dumps(response, sort_keys=True, indent=2))
		garden_data = r
		#184

		# get Police Station data in Boston
		response = ss.get("pyxn-r3i2")
		r = json.loads(json.dumps(response, sort_keys=True, indent=2))
		police_data = r
		accessdb = union.unionF(hospital_data, garden_data, market_data, police_data)
		repo.access.drop()
		access = repo.access
		access.insert_many(accessdb)
		repo.logout()
		endTime = datetime.datetime.now()
		#print("union complete")
		return {"start":startTime, "end":endTime}
		 
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')
		doc.add_namespace('alg','http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
		doc.add_namespace('mas', 'https://data.mass.gov')

		# agent
		this_script = doc.agent('alg:rengx_ztwu_lwj#retreiveData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		hospital_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label': 'hospital',	prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'json'})
		garden_resource = doc.entity('bdp:rdqf-ter7', {'prov:label': 'market', prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'json'})

		police_resource = doc.entity('bdp:pyxn-r3i2', {'prov:label': 'police',prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})


		# activities
		get_hospital = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		get_garden = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		get_police = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)




		doc.wasAssociatedWith(get_hospital, this_script)
		doc.wasAssociatedWith(get_garden, this_script)
		doc.wasAssociatedWith(get_police, this_script)
		
		doc.usage(get_hospital, hospital_resource, startTime, None,{prov.model.PROV_TYPE: 'ont:Retrieval'})
		doc.usage(get_garden, garden_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

		doc.usage(get_police, police_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

		hospital = doc.entity('dat:rengx_ztwu_lwj#hospital', {prov.model.PROV_LABEL: 'hospital Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(hospital, this_script)
		doc.wasGeneratedBy(hospital, get_hospital, endTime)
		doc.wasDerivedFrom(hospital, hospital_resource)
		
		garden = doc.entity('dat:rengx_ztwu_lwj#garden',{prov.model.PROV_LABEL: 'garden Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
		
		doc.wasAttributedTo(garden, this_script)
		doc.wasGeneratedBy(garden, get_garden, endTime)
		doc.wasDerivedFrom(garden, garden_resource)
		
		police = doc.entity('dat:rengx_ztwu_lwj#police', {prov.model.PROV_LABEL: 'police Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
		
		doc.wasAttributedTo(police, this_script)
		doc.wasGeneratedBy(police, get_police, endTime)
		doc.wasDerivedFrom(police, police_resource)

		#repo.record(doc.serialize())
		repo.logout()
		return doc
														
					
#union.execute()
#doc = union.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
