import shapefile
from pyproj import Proj, transform
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import rtree
import json
import geojson
import geopy.distance
import shapely.geometry
from tqdm import tqdm
from geojson import Polygon


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

for i in range(len(zipcode)):
	for j in range(len(coor[i])):
		x1, y1 = coor[i][j][0], coor[i][j][1]
		x2, y2 = transform(inProj, outProj, x1, y1)
		coor[i][j] = (y2, x2)
		zip_to_coor[zipcode[i]] = coor[i]

# function of checking whether a point is inside a polygon
# implemented online on http://geospatialpython.com/2011/01/point-in-polygon.html

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
    reads = ['pt0713_silnuext.property_2015',
             'pt0713_silnuext.crime']
    writes = ['pt0713_silnuext.proj2']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        crime = repo.pt0713_silnuext.crime.find()
        property_2015 = repo.pt0713_silnuext.property_2015.find()

        # getting coordination of crimes happened in 2015
        crime_coordination = project(crime, lambda x:(x["year"], x["month"], x["location"]["latitude"],x["location"]["longitude"]))     
        crime_15 = [crime_2015 for crime_2015 in crime_coordination if crime_2015[0] == "2015" and crime_2015[1] == "8"]
        crime_15coordination = [(float(latitude), float(longitude)) for (year, month, latitude, longitude) in crime_15]

        # getting coordination of properties that in the dataset of property assessment of 2015
        property15_price_coordination = project(property_2015, lambda x: (int(x["av_total"]), eval(str(x.get("location")))))
        # shorting data in order to run it in a racheable time
        property15_price_short_coordination = property15_price_coordination[:50]

        # function of classifying properties in 2015 into differnet zipcodes by using their coordinates
        def zip_code_propertydata(zip_to_coor, coor):
            coor_zip = {}
            for zipcode in zip_to_coor:
                for i in range(len(coor)):
                    if polygon(coor[i][1][0], coor[i][1][1], zip_to_coor[zipcode]):
                        if zipcode not in coor_zip:
                            coor_zip[zipcode] = [coor[i]]
                        else:
                            coor_zip[zipcode] += [coor[i]]
            return coor_zip

        # function of classifying crime incidents in 2015 into different zipcodes by using their coordinates
        def zip_code_crimedata(zip_to_coor, coor):
            coor_zip = {}
            for zipcode in zip_to_coor:
                for i in range(len(coor)):
                    if polygon(coor[i][0], coor[i][1], zip_to_coor[zipcode]):
                        if zipcode not in coor_zip:
                            coor_zip[zipcode] = [coor[i]]
                        else:
                            coor_zip[zipcode] += [coor[i]]
            return coor_zip

        #property15_zipcode = zip_code_propertydata(zip_to_coor, property15_price_short_coordination_float)
        #crime15_zipcode = zip_code_crimedata(zip_to_coor, crime_15short_coordination)

        # inserting zipcode as polygons into R-Tree
        def property_zipcode():
            zip_shapes = [(zipcode, (shapely.geometry.asShape(Polygon(zip_to_coor[zipcode])))) for zipcode in zip_to_coor]
            property_zip = {}
            rtidx = rtree.index.Index()
            for i in tqdm(range(len(zip_to_coor))):
                (zipcode, shape) = zip_shapes[i]
                rtidx.insert(zipcode, shape.bounds)

            for i in range(len(property15_price_short_coordination)):
                (lat, lon) = property15_price_short_coordination[i][1]
                for (zipcode, shape) in [zip_shapes[i] for i in rtidx.nearest((lon, lat, lon, lat), 1)]:
                    if shape.contains(shapely.geometry.Point(lon, lat)):
                        if zipcode not in property_zip:
                            property_zip[zipcode] = [property15_price_short_coordination_float[i]]
                        else:
                            property_zip[zipcode] += [property15_price_short_coordination_float[i]]

            return property_zip

        print(property_zipcode())

        # function of calculating average property prices within each zipcode
        def avgprice_zipcode(property_zip):
            price15_zipcode = {}
            for zipcode in property_zip:
                for i in range(len(property_zip[zipcode])):
                    if zipcode not in price15_zipcode:
                        price15_zipcode[zipcode] = [property_zip[zipcode][i][0]]
                    else:
                        price15_zipcode[zipcode] += [property_zip[zipcode][i][0]]

            for zipcode in price15_zipcode:
                price15_zipcode[zipcode] = [sum(price15_zipcode[zipcode]) / len(price15_zipcode[zipcode])]
            
            return price15_zipcode


        # function of calculating amount of crime incidents within each zipcode
        def length_zipcode(crime_zip):
            crime_number = {}
            for zipcode in crime_zip:
                if zipcode not in crime_number:
                    crime_number[zipcode] = len(crime_zip[zipcode])
                else:
                    crime_number[zipcode] += len(crime_zip[zipcode])

            return crime_number


        #print(avgprice_zipcode(property15_zipcode))

 
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
        resource1 = doc.entity('bdp:crime', {'prov:label':'crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:n7za-nsjh', {'prov:label':'property15', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_property_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_crime, this_script)

        doc.usage(get_property_crime, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        doc.usage(get_property_crime, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        property14 = doc.entity('dat:pt0713_silnuext#property_14', {prov.model.PROV_LABEL:'property_2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property14, this_script)
        doc.wasGeneratedBy(property14, get_property_crime, endTime)
        doc.wasDerivedFrom(property14, resource2, get_property_crime, get_property_crime, get_property_crime)

        property15 = doc.entity("dat:pt0713_silnuext#property_15", {prov.model.PROV_LABEL:"property_2015", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(property15, this_script)
        doc.wasGeneratedBy(property15, get_property_crime, endTime)
        doc.wasDerivedFrom(property15, resource3, get_property_crime, get_property_crime, get_property_crime)

        repo.logout()
                  
        return doc

proj2.execute()
#doc = proj2.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
