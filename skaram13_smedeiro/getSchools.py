import urllib.request
from urllib.parse import quote
import json
import dml
import prov.model
import datetime
import uuid
import csv
import time

class getSchools(dml.Algorithm):
    contributor = 'skaram13_smedeiro'
    reads = []
    writes = ['skaram13_smedeiro.school']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')


        def requestCensusTract(city, state, zipcode, street):
            url = "https://geocoding.geo.census.gov/geocoder/geographies/address?street=" + quote(street) + "&city=" + quote(city) + "&state=" + quote(state) + "&zip="+ quote(zipcode) + "&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=14&format=json"
            req = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'})
            response = urllib.request.urlopen(req).read().decode("utf-8")
            r = json.loads(response)
            return (r)

        def requestOrgCodesAndSchoolNames():
            orgcodeDict = {}
            dbEntries = []
            addressNotFound = []
            schoolName = ""
            splitSchoolName = []
            # print("Parsing CSV...")
            with open('search.csv', 'r') as f:
                read_data = csv.reader(f)

                for entry in read_data:
                    schoolName = entry[0]
                    splitSchoolName = entry[0].split(": ")
                    if splitSchoolName[0] == "Boston":
                        orgcode = entry[1] 
                        street = entry[5]
                        city = entry[7]
                        state = entry[8]
                        zipcode = entry[9]
                        time.sleep(2)
                        census = requestCensusTract(city, state, zipcode, street)

                        if len(census['result']['addressMatches']) == 0:
                            addressNotFound.append({'city':city, 'state':state, 'zipcode': zipcode, 'street': street})
                        else:
                            censusTract = census['result']['addressMatches'][0]['geographies']['Census Blocks'][0]['TRACT']
                            dbEntry = {'schoolName':splitSchoolName[1], 'orgcode': orgcode,'city':city, 'state':state, 'zipcode': zipcode, 'street': street, 'censusTract': censusTract }
                            dbEntries.append(dbEntry)
            return (dbEntries)
            
        dbEntries = requestOrgCodesAndSchoolNames()

        repo.dropCollection("school")
        repo.createCollection("school")
        
        # print(dbEntries)
        repo['skaram13_smedeiro.school'].insert_many(dbEntries)

        #test and print from database
        #print(orgcodeLookup)

        # results = repo['skaram13_smedeiro.school'].find()
        # print (results)
        # for each in results:
        #     print (each)

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
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')
        # doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        # doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        # doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # this_script = doc.agent('alg:skaram13_smedeiro#getSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # schools = doc.entity('dat:skaram13_smedeiro#schools', {prov.model.PROV_LABEL:'Dataset with school names, orgcodes, and addresses', prov.model.PROV_TYPE:'ont:DataSet'})

        # this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        # school_resource = doc.entity('', {'prov:label':' Dataset with school names, orgcodes, and census tract', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

        # get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        # doc.wasAssociatedWith(get_income, this_script)

        # doc.usage(get_income, income_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        # income = doc.entity('dat:skaram13_smedeiro#income', {prov.model.PROV_LABEL:'Income', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(income, this_script)
        # doc.wasGeneratedBy(income, get_income, endTime)
        # doc.wasDerivedFrom(income, income_resource, get_income, get_income, get_income)
        # repo.logout()
                  
        return doc

getSchools.execute()
# print("Done!")
#doc = getIncomeByCensusTract.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
