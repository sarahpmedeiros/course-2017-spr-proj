import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty


def union(R, S):
		return R + S


class fmarket_landmarks(dml.Algorithm):
	"""docstring for crime_police"""
	contributor = 'nyx'

	reads = ['nyx.sgarden', 'nyx.fmarket']
	writes = ['nyx.fmarket_landmarks']
	
	@staticmethod

	def map(f, R):
		return [t for (k,v) in R for t in f(k,v)]

	@staticmethod

	def reduce(f,R):
		keys = {k for (k,v) in R}
		return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

	def dist(p, q):
	    (x1,y1) = p
	    (x2,y2) = q
	    return (x1-x2)**2 + (y1-y2)**2

	def plus(args):
	    p = [0,0]
	    for (x,y) in args:
	        p[0] += x
	        p[1] += y
	    return tuple(p)

	def scale(p, c):
	    (x,y) = p
	    return (x/c, y/c)

	def difference(R, S):
	    return [t for t in R if t not in S]

	def intersect(R, S):
	    return [t for t in R if t in S]

	def project(R, p):
	    return [p(t) for t in R]


	 
	def product(R, S):
	    return [(t,u) for t in R for u in S]

	def aggregate(R, f):
	    keys = {r[0] for r in R}
	    return [(key, f([v for (k,v) in R if k == key])) for key in keys]


	

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nyx', 'nyx')

		def getData(db):
			t = []
			for i in repo['nyx.' + db].find():
				t.append(i)
			return t

		def select(R, s):
			return [t for t in R if s(t)]

		def intersect(R, S):
			return [t for t in R if t in S]
	

		def product(R, S):
			return [(t, u) for t in R for u in S]


	    # Transformation 1: Get the union dataset of Landmarks and Farmer Markets.
		# A combination of farmer market and school garden.
		r = getData('fmarket')
		
		'''
		print(r)
		print("Here is data")
		'''
		#print(r)
		s = getData('landmarks')
		

		p = getData('police')
		

		c = getData('crime')
		

		parking = getData('parking')
		

		farmer_market = []
		landmarks = []

		
		listOfTown = []
		for i in r:
			if i["town"] not in listOfTown:
				listOfTown.append(i["zip_code"])
		print(listOfTown)

		listOfZip = []

		for i in s:
			if i["location_zip"] not in listOfZip:
				listOfZip.append(i["location_zip"])
		print(listOfZip)
		
		for j in range(len(listOfTown)):
			for k in r:
				if k['zip_code'] == listOfTown[j]:
					farmer_market.append({k['name'],k['addr_1']})
		print(farmer_market)
		print('   ')

		for j in range(len(listOfZip)):
			for k in s:
				if k['location_zip'] == listOfZip[j]:
					landmarks.append({k['name'],k['ad']})
		print(landmarks)

		union = []
		union = farmer_market + landmarks
		print("Here is the union result of Landmarks and Farmer Markets")
		print(union)




		# Transformation 2: Farmer Markets have police station within two miles.
		farmer_market_cood = []
		for i in s:
			if i['location']['coordinates'] not in farmer_market_cood:
				farmer_market_cood.append(i['location']['coordinates'])
		print(farmer_market_cood)

		parking_cood = []
		for i in parking:
			parking_cood.append(i['location']['coordinates'])
		print(parking_cood)

		distance = s + parking
		
		for i in range(len(distance)):
			distance[i] = (distance[i], (vincenty((distance[i]['location']['coordinates']), (distance[i]['location']['coordinates'])).miles))
		
		result = select(distance, lambda t: t[1] < 2)

		print(' ')
		print("Farmer Markets have police station within two miles")
		print(result)

		



		#Transformation 3: Vistor Suggestion with Parking.
		lst = []
		for i in range(len(r)):
			for j in range(len(parking)):
				if r[i]['location']['coordinates'][0] - parking[j]['location']['coordinates'][0] < 5:
					if r[i]['location']['coordinates'][1] - parking[j]['location']['coordinates'][1] < 5:
						if r[i]['location'] not in lst:
							lst.append({r[i]['name'], r[i]['addr_1']})
		



		lst1 = []

		for i in range(len(s)):
			for j in range(len(parking)):
				if s[i]['location']['coordinates'][0] - parking[j]['location']['coordinates'][0] < 5:
					if s[i]['location']['coordinates'][1] - parking[j]['location']['coordinates'][1] < 5:
						if s[i]['location'] not in lst1:
							lst1.append({s[i]['name'], s[i]['ad']})

		total = lst + lst1
		print(' ')
		print('Vistor Suggestion with Parking')
		print(total)










		


		'''
		for i in r:
			if i["town"] == 'Boston':
				e.append(i['name'])
		print(e)


		
		m = r+s
		print(m)

		db[RS].remove({});
		db.createCollection(RS);
		db[R].find().forEach()

		R = repo['nyx.sgarden']
		S = repo['nyx.fmarket']

		X =    map(lambda k,v: [(k, ('Location', v))], R)\
			 + map(lambda k,v: [(k, ('Zip code'))], S)
		Y = reduce(lambda k,vs:(k,(vs[0][1], vs[1][1]) if vs[0][0] == 'Location' else (vs[1][1], vs[0][1])),X)

		print(Y)




		
		place = []

		for i in x.find():
			place.append((i['area'], i['place']))

		
		
		
		police = [doc for doc in repo['nyx.police'].find()]

		crime_police = []

		for p in police:
			c = repo['nyx.crime'].find({'geometry':{'$near':[police['CENTROIDY'], police['CENTROIDX']]}})
			crime_police.push({'location':Station['NAME'], 'c':c})

		for p in crime_police:

		districts = ['A1', 'D4', 'E13', 'B3', 'E18', 'D14', 'Boston Police Headquarters', 'A7', 'C6', 'B2','E5','C11']
		count = 0

		for doc in repo['nyx.police'].find():
			d = districts[count]
			print(d)
			num = repo['nyx.crime'].count({'d':d})
			print(num)
			repo['nyx.police'].update({'_id': doc['_id']}, {'$set':{'num':num},}, upsert=False)
			count += 1
		'''



		repo.logout()
		endTime = datetime.datetime.now()
		return{"start":startTime, "end": endTime}

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

		crime = doc.entity('dat:nyx#crime', {prov.model.PROV_LABEL:'Boston Crime Info', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crime, this_script)
		doc.wasGeneratedBy(crime, crime_getInfo, endTime)
		doc.wasDerivedFrom(crime, crime_info, crime_getInfo, crime_getInfo, crime_getInfo)

		police = doc.entity('dat:nyx#police', {prov.model.PROV_LABEL:'Boston Police Station', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(police, this_script)
		doc.wasGeneratedBy(police, police_getInfo, endTime)
		doc.wasDerivedFrom(police, police_info, police_getInfo, police_getInfo, police_getInfo)

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

		parking = doc.entity('dat:nyx#parking', {prov.model.PROV_LABEL:'Boston Parking Info', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(parking, this_script)
		doc.wasGeneratedBy(parking, parking_getInfo, endTime)
		doc.wasDerivedFrom(parking, parking_info, parking_getInfo, parking_getInfo, parking_getInfo)

		repo.logout()
		return doc
'''

crime_police.execute()
doc = crime_police.provenance()
'''

