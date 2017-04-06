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
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont',
					'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
		doc.add_namespace('mas', 'https://data.mass.gov')


		# agent
		this_script = doc.agent('alg:rengx_ztwu_lwj#retreiveData',
					{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		publicschool_resource = doc.entity('bdp:492y-i77g', {'prov:label': 'Public School',
									prov.model.PROV_TYPE: 'ont:DataResource',
									'ont:Extension': 'json'})
		market_resource = doc.entity('mas:66t5-f563', {'prov:label': 'market',
									 prov.model.PROV_TYPE: 'ont:DataResource',
									 'ont:Extension': 'json'})


		# activities
		get_publicschool = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		get_market = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_publicschool, this_script)
		doc.wasAssociatedWith(get_market, this_script)

		doc.usage(get_publicschool, publicschool_resource, startTime, None,
				{prov.model.PROV_TYPE: 'ont:Retrieval'})
		doc.usage(get_market, market_resource, startTime, None,
				{prov.model.PROV_TYPE: 'ont:Retrieval'})

		publicschool = doc.entity('dat:rengx_ztwu_lwj#publicschool',
						{prov.model.PROV_LABEL: 'Public school Locations',
						 prov.model.PROV_TYPE: 'ont:DataSet'})
		market = doc.entity('dat:rengx_ztwu_lwj#market',
								{prov.model.PROV_LABEL: 'Market db',
								 prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(publicschool, this_script)
		doc.wasGeneratedBy(publicschool, get_publicschool, endTime)
		doc.wasDerivedFrom(publicschool, publicschool_resource)

		policestation = doc.entity('dat:rengx_ztwu_lwj#market',
						 {prov.model.PROV_LABEL: "market Locations",
					 prov.model.PROV_TYPE: "ont:DataSet"})
		doc.wasAttributedTo(market, this_script)
		doc.wasGeneratedBy(market, get_market, endTime)
		doc.wasDerivedFrom(market, market_resource)


		#repo.record(doc.serialize())
		repo.logout()

		return doc

#retrieve.execute()
#doc = retrieve.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))