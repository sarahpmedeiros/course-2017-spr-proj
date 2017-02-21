import urllib.request
import json
import dml
import prov.model
import datetime
import time
class getIncomeByCensusTract(dml.Algorithm):
    contributor = 'skaram13_smedeiro'
    reads = []
    writes = ['skaram13_smedeiro.income']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

        #income by census tract api - JSON
        req = urllib.request.Request('https://api.datausa.io/api/?sort=desc&show=geo&required=income%2Cincome_moe&sumlevel=tract&year=all&where=geo%3A16000US2507000',headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req).read().decode("utf-8")

        r = json.loads(response)

        dbEntries = []
        for entry in r['data']:
            #checks if income entry exists
            if entry[2] is not None:
                #gets the last 6 digits of the entry which is the census tract number
                censusTract = entry[1][-6:]
                #make entry - note: moe is margin of error
                dbEntry = {"censusTract": censusTract,"year":entry[0], "income": entry[2], "moe": entry[3]}
                dbEntries.append(dbEntry)
        

        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("income")
        repo.createCollection("income")
        
        # print (dbEntries)
        repo['skaram13_smedeiro.income'].insert_many(dbEntries)

        #test and print from database
        results = repo['skaram13_smedeiro.income'].find()
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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('skaram13_smedeiro', 'skaram13_smedeiro')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dus', 'https://api.datausa.io/api/')
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:skaram13_smedeiro#getIncomeByCensusTract', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        income = doc.entity('dat:skaram13_smedeiro#income', {prov.model.PROV_LABEL:'Dataset  with income, year, census tract, and moe', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        this_script = doc.agent('alg:skaram13_smedeiro#getIncomeByCensusTract', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        income_resource = doc.entity('dus:?sort=desc&show=geo&required=income%2Cincome_moe&sumlevel=tract&year=all&where=geo%3A16000US2507000', {'prov:label':'Dataset with income, year, extended census tract, and moe', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_income, this_script)

        doc.usage(get_income, income_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        income = doc.entity('dat:skaram13_smedeiro#income', {prov.model.PROV_LABEL:'Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, income_resource, get_income, get_income, get_income)

        repo.logout()
                  
        return doc

# getIncomeByCensusTract.execute()
# doc = getIncomeByCensusTract.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
