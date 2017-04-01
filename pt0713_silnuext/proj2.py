import shapefile
from pyproj import Proj, transform
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


# implementation of schemas from course notes

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]


# getting zipcode data from local directory/internet

myshp = open("zipcodes_nt/ZIPCODES_NT_POLY.shp", "rb")
mydbf = open("zipcodes_nt/ZIPCODES_NT_POLY.dbf", "rb")
r = shapefile.Reader(shp=myshp, dbf=mydbf)
shapes = r.shapes()

zipcode = [x[0] for x in r.records()]
coor = [x.points for x in shapes]

inProj = Proj(init='epsg:26986')
outProj = Proj(init='epsg:4326')
zip_to_coor = {}
print(zip_to_coor)

for i in range(len(zipcode)):
	for j in range(len(coor[i])):
		x1, y1 = coor[i][j][0], coor[i][j][1]
		x2, y2 = transform(inProj, outProj, x1, y1)
		coor[i][j] = (y2, x2)
		zip_to_coor[zipcode[i]] = coor[i]
#print(zip_to_coor)


# function of checking whether a point is inside a polygon
# implemented online on

def polygon(x, y, poly):
    n = len(poly)
    inside = False
    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x, p2y = poly[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xints = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside





class proj2(dml.Algorithm):
    contributor = 'pt0713_silnuext'
    reads = ['pt0713_silnuext.proj2']
    writes = ['pt0713_silnuext.proj2']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        repo.dropCollection("property_crime")
        repo.createCollection("property_crime")

        # import crime data
        client1 = sodapy.Socrata("data.cityofboston.gov", None)
        response1 = []
        limits = [0, 50001, 100001, 150001, 200001, 250001]
        for limit in limits:
            response1 += client1.get("crime", limit=50000, offset=limit)
        s = json.dumps(response1, sort_keys=True, indent=2)

        # getting coordination of crimes happened in 2014
        crime_coordination = project(response1,lambda x:(x["year"], x["location"]["latitude"],x["location"]["longitude"]))
        crime_14 = [crime_2014 for crime_2014 in crime_coordination if crime_2014[0] == "2014"]
        crime_14coordination = [(float(latitude), float(longitude)) for (year, latitude, longitude) in crime_14]
        
        # getting coordination of crimes happened in 2015
        crime_15 = [crime_2015 for crime_2015 in crime_coordination if crime_2015[0] == "2015"]
        crime_15coordination = [(float(latitude), float(longitude)) for (year, latitude, longitude) in crime_15]

        repo['pt0713_silnuext.property_crime'].insert_many(response1)
        repo['pt0713_silnuext.property_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.property_crime'].metadata())

        # import property2014 data
        client2014 = sodapy.Socrata("data.cityofboston.gov", None)
        response2014 = client2014.get("jsri-cpsq")
        s = json.dumps(response2014, sort_keys=True, indent=2)

        # getting coordination of properties that in the dataset of property assessment of 2014
        property14_price_coordination = project(response2014, lambda x: (x["av_total"],x["location"]))
        property14_coordination = [eval(a[1]) for a in property14_price_coordination]
        property14_price_coordination_float = [(int(price[0]), coordination) for price in property14_price_coordination for coordination in property14_coordination]


        #print(property14_price_coordination_float)
        repo['pt0713_silnuext.property_crime'].insert_many(response2014)
        repo['pt0713_silnuext.property_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.property_crime'].metadata())


        # correspond zipcode to property_14 (project2)

        zipcode_property14 = [{} for i in range(len(property14_price_coordination_float)) if polygon(property14_price_coordination_float[i][1][0],property14_price_coordination_float[i][1][1],)]

        def zip_code_property14():
            mylist = []
            return 0


        # import property2015 data
        client2015 = sodapy.Socrata("data.cityofboston.gov", None)
        response2015 = client2015.get("n7za-nsjh")
        s = json.dumps(response2015, sort_keys=True, indent=2)

        # getting coordination of properties that in the dataset of property assessment of 2015
        property15_price_coordination = project(response2015, lambda x: (x["av_total"],x["location"]))
        property15_coordination = [eval(a[1]) for a in property15_price_coordination]
        property15_price_coordination_float = [(int(price[0]), coordination) for price in property15_price_coordination for coordination in property15_coordination]
        print(property15_coordination)
        repo['pt0713_silnuext.property_crime'].insert_many(response2015)
        repo['pt0713_silnuext.property_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.property_crime'].metadata())
 
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
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        

        this_script = doc.agent('alg:pt0713_silnuext#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('bdp:crime', {'prov:label':'crime_district1415', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:jsri-cpsq', {'prov:label':'property14', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('bdp:n7za-nsjh', {'prov:label':'property15', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_property_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_crime, this_script)

        doc.usage(get_property_crime, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        doc.usage(get_property_crime, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        doc.usage(get_property_crime, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        property14 = doc.entity('dat:pt0713_silnuext#property_14', {prov.model.PROV_LABEL:'property_2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property14, this_script)
        doc.wasGeneratedBy(property14, get_property_crime, endTime)
        doc.wasDerivedFrom(property14, resource2, get_property_crime, get_property_crime, get_property_crime)

        property15 = doc.entity("dat:pt0713_silnuext#property_15", {prov.model.PROV_LABEL:"property_2015", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(property15, this_script)
        doc.wasGeneratedBy(property15, get_property_crime, endTime)
        doc.wasDerivedFrom(property15, resource3, get_property_crime, get_property_crime, get_property_crime)

        crime = doc.entity("dat:pt0713_silnuext#crime", {prov.model.PROV_LABEL:"crime", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_property_crime, endTime)
        doc.wasDerivedFrom(crime, resource1, get_property_crime, get_property_crime, get_property_crime)

        repo.logout()
                  
        return doc

proj2.execute()
doc = proj2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
