import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

# functions implemented from lecture notes
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

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]


class police_crime(dml.Algorithm):
    contributor = 'pt0713_silnuext'
    reads = ['pt0713_silnuext.police_crime']
    writes = ['pt0713_silnuext.police_crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        repo.dropCollection("police_crime")
        repo.createCollection("police_crime")

        # import police districts data
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/9a3a8c427add450eaf45a470245680fc_5.geojson"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r = [r['features'][i]['properties'] for i in range(11)]

        districts = project(r, lambda t: (t['DISTRICT']))
        district_key = [('DISTRICT', dis) for dis in districts]
        print(districts)

        # repo['pt0713_silnuext.police_crime'].insert_many(districts)
        # repo['pt0713_silnuext.police_crime'].metadata({'complete':True})
        # print(repo['pt0713_silnuext.police_crime'].metadata())




        # import crime data      
        client1 = sodapy.Socrata("data.cityofboston.gov", None)
        response1 = client1.get("crime")
        s = json.dumps(response1, sort_keys=True, indent=2)


        crime_district = project(response1,lambda t:(t['reptdistrict']))
        print(crime_district)

        crime_in_police_district = 0
        for district in crime_district:
            if district in districts:
                crime_in_police_district += 1

        percentage_crime_in_police_district = crime_in_police_district / len(crime_district)
        print("The percentage of crime happens in police district is: ", percentage_crime_in_police_district)



        repo['pt0713_silnuext.police_crime'].insert_many(response1)
        repo['pt0713_silnuext.police_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.police_crime'].metadata())
        
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
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_police_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_police_crime, this_script)

        doc.usage(get_police_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+police_crime&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        police_crime = doc.entity('dat:pt0713_silnuext#police_crime', {prov.model.PROV_LABEL:'Animals police_crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(police_crime, this_script)
        doc.wasGeneratedBy(police_crime, get_police_crime, endTime)
        doc.wasDerivedFrom(police_crime, resource, get_police_crime, get_police_crime, get_police_crime)


        repo.logout()
                  
        return doc

police_crime.execute()
doc = police_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof