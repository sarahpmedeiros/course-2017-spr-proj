import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class getData(dml.Algorithm):
    contributor = 'jw0208'
    reads = []
    writes = ['jw0208.education', 'jw0208.poverty','jw0208.tax','jw0208.health','jw0208.medicare']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jw0208',"jw0208") #username, password
## done----------------------------------------------------------
        url = 'http://datamechanics.io/data/jw0208/medicare.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("medicare")
        repo.createCollection("medicare")
        repo['jw0208.medicare'].insert_many(r)
        repo['jw0208.medicare'].metadata({'complete':True})
        #print(json.dumps(s, sort_keys=True, indent=2))

## done----------------------------------------------------------
        url = 'http://datamechanics.io/data/jw0208/poverty.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("poverty")
        repo.createCollection("poverty")
        repo['jw0208.poverty'].insert_many(r)
        repo['jw0208.poverty'].metadata({'complete':True})
        #print(json.dumps(s, sort_keys=True, indent=2))

# ## done----------------------------------------------------------
        url = 'http://datamechanics.io/data/jw0208/education.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("education")
        repo.createCollection("education")
        repo['jw0208.education'].insert_many(r)
        repo['jw0208.education'].metadata({'complete':True})
        #print(repo['jw0208.education'].metadata())
        #print(json.dumps(s, sort_keys=True, indent=2))
#         with open('education.json') as r:
#             s = json.loads(r.read())
#         repo.dropCollection("education")
#         repo.createCollection("education")
#         repo['jw0208.education'].insert_many(s)
#         repo['jw0208.education'].metadata({'complete': True})
#         # print(json.dumps(s, sort_keys=True, indent=2))

## done----------------------------------------------------------
        url = 'http://datamechanics.io/data/jw0208/income.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("income")
        repo.createCollection("income")
        repo['jw0208.income'].insert_many(r)
        repo['jw0208.income'].metadata({'complete':True})
        # print(json.dumps(s, sort_keys=True, indent=2))

## ----------------------------------------------------------
        client = sodapy.Socrata("chronicdata.cdc.gov", None)
        response = client.get("fq5d-abxc", limit=52)
        r = json.loads(json.dumps(response)) #load twice to convert list to str
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("health")
        repo.createCollection("health")
        repo['jw0208.health'].insert_many(r)
        repo['jw0208.health'].metadata({'complete':True})
        #print(json.dumps(response, sort_keys=True, indent=2))


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
        repo.authenticate('jw0208', 'jw0208')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mdg', 'https://data.mass.gov/resource/')
        doc.add_namespace('cdg', 'https://chronicdata.cdc.gov/resource/')

        this_script = doc.agent('alg:jw0208#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cdg:fq5d-abxc', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_medicare = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        this_poverty = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_education = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        this_income = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        this_health = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_medicare, this_script)
        doc.wasAssociatedWith(this_poverty, this_script)
        doc.wasAssociatedWith(this_education, this_script)
        doc.wasAssociatedWith(this_income, this_script)
        doc.wasAssociatedWith(this_health, this_script)
        doc.usage(this_medicare, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(this_poverty, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(this_education, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(this_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(this_health, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        medicare = doc.entity('dat:medicare', {prov.model.PROV_LABEL:'medicare', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(medicare, this_script)
        doc.wasGeneratedBy(medicare, this_medicare, endTime)
        doc.wasDerivedFrom(medicare, resource, this_medicare, this_medicare, this_medicare)

        poverty = doc.entity('dat:poverty', {prov.model.PROV_LABEL:'poverty', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(poverty, this_script)
        doc.wasGeneratedBy(poverty, this_poverty, endTime)
        doc.wasDerivedFrom(poverty, resource, this_poverty, this_poverty, this_poverty)

        education = doc.entity('dat:education', {prov.model.PROV_LABEL:'education', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(education, this_script)
        doc.wasGeneratedBy(education, this_education, endTime)
        doc.wasDerivedFrom(education, resource, this_education, this_education, this_education)

        income = doc.entity('dat:income', {prov.model.PROV_LABEL:'income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, this_education, endTime)
        doc.wasDerivedFrom(income, resource, this_income, this_income, this_income)

        health = doc.entity('dat:health', {prov.model.PROV_LABEL:'health', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(health, this_script)
        doc.wasGeneratedBy(health, this_health, endTime)
        doc.wasDerivedFrom(health, resource, this_health, this_health, this_health)

        repo.logout()
                  
        return doc

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4)) #a nicer format to see

## eof
