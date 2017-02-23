import urllib.request
import requests
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class data(dml.Algorithm):
    contributor = 'nyx'
    reads = []
    writes = ['nyx.landmarks', 'nyx.fmarket', 'nyx.sgarden', 'nyx.crime', 'nyx.parking', 'nyx.police']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nyx', 'nyx')

		#Landmarks dataset
        # url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'
        client = sodapy.Socrata("data.cityofboston.gov", None)
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        r1 = client.get("u6fv-m8v4", limit = 10)
        s = json.dumps(r1, sort_keys=True, indent=2)
        repo.dropCollection("landmarks")
        repo.createCollection("landmarks")
        repo['nyx.landmarks'].insert_many(r1)
        repo['nyx.landmarks'].metadata({'complete':True})
        print(repo['nyx.landmarks'].metadata())

        #Farmer markets
        client = sodapy.Socrata("data.mass.gov", None)
       	#url = 'https://data.mass.gov/resource/66t5-f563.json'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        r2 = client.get("66t5-f563", limit = 10)
        #r2 = json.loads(response)
        s = json.dumps(r2, sort_keys=True, indent=2)
        repo.dropCollection("fmarket")
        repo.createCollection("fmarket")
        repo['nyx.fmarket'].insert_many(r1)

        #School Gardens
        #url = 'https://data.cityofboston.gov/resource/pzcy-jpz4.json'
        client = sodapy.Socrata("data.cityofboston.gov", None)
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        r3 = client.get("pzcy-jpz4", limit = 10)
        s = json.dumps(r3, sort_keys=True, indent=2)
        repo.dropCollection("fmarket")
        repo.createCollection("fmarket")
        repo['nyx.fmarket'].insert_many(r2)


        #Crime dataset
        #url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
        client = sodapy.Socrata("data.cityofboston.gov", None)
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        r4 = client.get("29yf-ye7n", limit = 10)
        s = json.dumps(r4, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['nyx.crime'].insert_many(r3)

        #Parking Zone
        #url = 'https://data.cityofboston.gov/resource/gdnf-7hki.json'
        client = sodapy.Socrata("data.cityofboston.gov", None)
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        r5 = client.get("gdnf-7hki", limit = 10)
        s = json.dumps(r5, sort_keys=True, indent=2)
        repo.dropCollection("parking")
        repo.createCollection("parking")
        repo['nyx.parking'].insert_many(r4)

        #Police Station
        #url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        client = sodapy.Socrata("data.cityofboston.gov", None)
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        r6 = client.get("pyxn-r3i2", limit = 10)
        s = json.dumps(r6, sort_keys=True, indent=2)
        repo.dropCollection("police")
        repo.createCollection("police")
        repo['nyx.police'].insert_many(r5)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate('nyx', 'nyx')


    	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
    	doc.add_namespace('dat', 'http://datamachanics.io/data/')
    	doc.add_namespace('ont', 'http://datamachanics.io/ontology#')
    	doc.add_namespace('log', 'http://datamachanics.io/log/')
    	doc.add_namespace('bdp', 'http://data.cityofboston.gov/resource/')

    	this_script = doc.agent('alg:nyx#data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    	landmarks_info = doc.entity('bdp:u6fv-m8v4', {'prov:label': 'Boston Landmarks', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	landmarks_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Landmarks Data'})
    	doc.wasAssociatedWith(landmarks_getInfo, this_script)
    	doc.usage(
    		landmarks_getInfo,
    		landmarks_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	fmarket_info = doc.entity('bdp:66t5-f563', {'prov:label': 'Boston Farmer Markets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	fmarket_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Farmer Markets Data'})
    	doc.wasAssociatedWith(fmarket_getInfo, this_script)
    	doc.usage(
    		fmarket_getInfo,
    		fmarket_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	sgarden_info = doc.entity('bdp:pzcy-jpz4', {'prov:label': 'Boston School Garden', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	sgarden_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston School Garden Data'})
    	doc.wasAssociatedWith(sgarden_getInfo, this_script)
    	doc.usage(
    		sgarden_getInfo,
    		sgarden_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	crime_info = doc.entity('bdp:29yf-ye7n', {'prov:label': 'Boston Crime Info', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	crime_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Crime Info Data'})
    	doc.wasAssociatedWith(crime_getInfo, this_script)
    	doc.usage(
    		crime_getInfo,
    		crime_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	parking_info = doc.entity('bdp:gdnf-7hki', {'prov:label': 'Boston Parking Info', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	parking_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Parking Info Data'})
    	doc.wasAssociatedWith(parking_getInfo, this_script)
    	doc.usage(
    		parking_getInfo,
    		parking_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	police_info = doc.entity('bdp:pyxn-r3i2', {'prov:label': 'Boston Police Station', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	police_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Police Station Data'})
    	doc.wasAssociatedWith(police_getInfo, this_script)
    	doc.usage(
    		police_getInfo,
    		police_info,
    		startTime,
    		None,
    		{prov.model.PROV_TYPE:'ont:Retrieval'}
    		)

    	landmarks = doc.entity('dat:nyx#landmarks', {prov.model.PROV_LABEL:'Boston Landmarks', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(landmarks, this_script)
    	doc.wasGeneratedBy(landmarks, landmarks_getInfo, endTime)
    	doc.wasDerivedFrom(landmarks, landmarks_info, landmarks_getInfo, landmarks_getInfo, landmarks_getInfo)

    	fmarket = doc.entity('dat:nyx#fmarket', {prov.model.PROV_LABEL:'Boston Farmer Market', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(fmarket, this_script)
    	doc.wasGeneratedBy(fmarket, fmarket_getInfo, endTime)
    	doc.wasDerivedFrom(fmarket, fmarket_info, fmarket_getInfo, fmarket_getInfo, fmarket_getInfo)

    	sgarden = doc.entity('dat:nyx#sgarden', {prov.model.PROV_LABEL:'Boston School Garden', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(sgarden, this_script)
    	doc.wasGeneratedBy(sgarden, sgarden_getInfo, endTime)
    	doc.wasDerivedFrom(sgarden, sgarden_info, sgarden_getInfo, sgarden_getInfo, sgarden_getInfo)

    	crime = doc.entity('dat:nyx#crime', {prov.model.PROV_LABEL:'Boston Crime Info', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(crime, this_script)
    	doc.wasGeneratedBy(crime, crime_getInfo, endTime)
    	doc.wasDerivedFrom(crime, crime_info, crime_getInfo, crime_getInfo, crime_getInfo)

    	parking = doc.entity('dat:nyx#parking', {prov.model.PROV_LABEL:'Boston Parking Info', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(parking, this_script)
    	doc.wasGeneratedBy(parking, parking_getInfo, endTime)
    	doc.wasDerivedFrom(parking, parking_info, parking_getInfo, parking_getInfo, parking_getInfo)

    	police = doc.entity('dat:nyx#police', {prov.model.PROV_LABEL:'Boston Police Station', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(police, this_script)
    	doc.wasGeneratedBy(police, police_getInfo, endTime)
    	doc.wasDerivedFrom(police, police_info, police_getInfo, police_getInfo, police_getInfo)

    	#repo.record(doc.serialize())

    	repo.logout()

    	return doc

'''
data.execute()
doc = data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''



## eof













        
