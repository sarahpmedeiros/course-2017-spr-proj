import datetime
import json
import urllib
import uuid
from urllib.request import urlopen
import dml
import prov.model
import sodapy
import ssl

class retrieve(dml.Algorithm):
	contributor = 'rengx_ztwu_lwj'
	reads = []
	writes = ["rengx_ztwu_lwj.publicschool", "rengx_ztwu_lwj.market"]
	
	@staticmethod
	def selection(market_data):
		citymarket = []
		city = ["allston", "back bay", "boston", "bay village", "beacon village", "brighton", "charlestown", "chinatown", "dorchester", "downtown", "east boston", "fenway", "hyde park",\
			"jamaica plain", "mattapan", "mid-dorchester", "mission hill", "north end", "roslindale", "roxbury", \
			"south boston", "south end", "west end", "west roxbury"]
		for i in range (len(market_data)):
			if (market_data[i]['town'].lower() in city):
				citymarket.append(market_data[i])
		return citymarket
		
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
				
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')
		
		#public school
		repo.publicschool.drop()
		publicschool = repo.publicschool
		ss = sodapy.Socrata("data.cityofboston.gov", "x92LG4iaFto5qWQGFk3lDdv6p", username="lwj@bu.edu",password = "KatrinaLu2017")
		response = ss.get("492y-i77g")
		r = json.loads(json.dumps(response, sort_keys=True, indent=2))
		lis = []
		for i in range(len(r)):
			item = {}
			name = r[i]["sch_name"]
			addr = r[i]["location_location"]
			zipp = r[i]["location_zip"]
			coords = r[i]['location']['coordinates']
			#print(coords)
			xcoor = str(int(coords[1]*1000000))
			ycoor = str(int(coords[0]*(-1000000)))
			item["x"] = xcoor
			item["y"] = ycoor
			item["name"] = name
			item["addr"] = addr
			item["zipp"] = zipp
			item["type"] = "school"
			lis.append(item)
		publicschool.insert_many(lis)
		
		#market
		repo.market.drop()
		market = repo.market
		ss = sodapy.Socrata("data.mass.gov", "x92LG4iaFto5qWQGFk3lDdv6p", username="lwj@bu.edu",password = "KatrinaLu2017")
		response = ss.get("66t5-f563")
		r = json.loads(json.dumps(response, sort_keys=True, indent=2))
		market_data = retrieve.selection(r)
		market.insert_many(market_data)
		
		
		repo.logout()
		endTime = datetime.datetime.now()
		#print("retrieve complete")
		return {"start": startTime, "end": endTime}
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return doc

#retrieve.execute()