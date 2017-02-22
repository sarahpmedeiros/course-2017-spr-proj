import urllib.request
from urllib.parse import quote
import json
import dml
import prov.model
import datetime
import uuid
import csv
import time
with open("auth.json") as jsonFile:
    data = json.load(jsonFile)
    api_key = data['services']['Census Data']['key']

class getSchools(dml.Algorithm):
    contributor = 'skaram13_smedeiro'
    reads = []
    writes = ['skaram13_smedeiro.school']


    def requestCensusTract(city, state, zipcode, street):
        time.sleep(5)
        try:
            url = "https://geocoding.geo.census.gov/geocoder/geographies/address?street=" + quote(street) + "&city=" + quote(city) + "&state=" + quote(state) + "&zip="+ quote(zipcode) + "&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=14&format=json&key=" + api_key
            req = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'})
            response = urllib.request.urlopen(req).read().decode("utf-8")
            r = json.loads(response)
            return (r)
        except:
            return(-1)

    def requestOrgCodesAndSchoolNames():
        orgcodeDict = {}
        dbEntries = []
        addressNotFound = []
        schoolName = ""
        splitSchoolName = []
        failures = []
        print("Parsing CSV...")
        with open('search.csv', 'r') as f:
            read_data = csv.reader(f)
            i = 0
            for entry in read_data:
                i += 1
                schoolName = entry[0]
                splitSchoolName = entry[0].split(": ")

                if "Boston" == splitSchoolName[0][:6]:
                    orgcode = entry[1][-4:] 
                    street = entry[5]
                    city = entry[7]
                    state = entry[8]
                    zipcode = entry[9]
                    census = getSchools.requestCensusTract(city, state, zipcode, street)
                    if census != -1:
                        if len(census['result']['addressMatches']) == 0:
                            addressNotFound.append({'city':city, 'state':state, 'zipcode': zipcode, 'street': street})
                        else:
                            censusTract = census['result']['addressMatches'][0]['geographies']['Census Blocks'][0]['TRACT']
                            dbEntry = {'schoolName':splitSchoolName[1], 'orgcode': orgcode,'city':city, 'state':state, 'zipcode': zipcode, 'street': street, 'censusTract': censusTract }
                            #print(dbEntry)
                            dbEntries.append(dbEntry)
                    else:
                        failures.append([orgcode,street,city,state,zipcode])

        #print(failures)
        #print(len(failures))
        return (dbEntries)



    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

       
        dbEntries = getSchools.requestOrgCodesAndSchoolNames()

        repo.dropCollection("school")
        repo.createCollection("school")
        
        repo['skaram13_smedeiro.school'].insert_many(dbEntries)

        #test and print from database
        results = repo['skaram13_smedeiro.school'].find()
        print (results)
        for each in results:
            print (each)

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
        repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('geo', 'https://geocoding.geo.census.gov/geocoder/geographies/')
        doc.add_namespace('ser', 'search.csv')

        this_script = doc.agent('alg:skaram13_smedeiro#getSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        schools = doc.entity('dat:skaram13_smedeiro#schools', {prov.model.PROV_LABEL:'Dataset with school names, orgcodes, census tracts and addresses', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        school_resource = doc.entity('ser:', {'prov:label':' Dataset with school names, orgcodes, and addresses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        census_resource = doc.entity('geo:', {'prov:label':' Census tract based off of address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, school_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, census_resource, get_schools)
        doc.wasDerivedFrom(schools, school_resource, get_schools)
        repo.logout()
                  
        return doc

#getSchools.execute()
#print("Done!")
#doc = getSchools.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
