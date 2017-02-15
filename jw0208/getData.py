import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import csv

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
        with open('tax.json') as r:
            s= json.loads(r.read())
        repo.dropCollection("tax")
        repo.createCollection("tax")
        repo['jw0208.tax'].insert_many(s)
        repo['jw0208.tax'].metadata({'complete':True})
        #print(json.dumps(s, sort_keys=True, indent=2))

## done----------------------------------------------------------
        with open('poverty.json') as r:
            s= json.loads(r.read())
        repo.dropCollection("poverty")
        repo.createCollection("poverty")
        repo['jw0208.poverty'].insert_many(s)
        repo['jw0208.poverty'].metadata({'complete':True})
        #print(json.dumps(s, sort_keys=True, indent=2))

## done----------------------------------------------------------
        with open('education.json') as r:
            s = json.loads(r.read())
        repo.dropCollection("education")
        repo.createCollection("education")
        repo['jw0208.education'].insert_many(s)
        repo['jw0208.education'].metadata({'complete': True})
        # print(json.dumps(s, sort_keys=True, indent=2))
## ----------------------------------------------------------
        client = sodapy.Socrata("chronicdata.cdc.gov", None)
        response = client.get("fq5d-abxc", limit=10)
        r = json.loads(json.dumps(response)) #load twice to convert list to str
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("health")
        repo.createCollection("health")
        repo['jw0208.health'].insert_many(r)
        repo['jw0208.health'].metadata({'complete':True})
        print(json.dumps(response, sort_keys=True, indent=2))


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

        this_script = doc.agent('alg:getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_tax = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_health = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_tax, this_script)
        doc.wasAssociatedWith(this_health, this_script)
        doc.usage(this_tax, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(this_health, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        tax = doc.entity('dat:tax', {prov.model.PROV_LABEL:'tax', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tax, this_script)
        doc.wasGeneratedBy(tax, this_tax, endTime)
        doc.wasDerivedFrom(tax, resource, this_tax, this_tax, this_tax)

        health = doc.entity('dat:health', {prov.model.PROV_LABEL:'health', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(health, this_script)
        doc.wasGeneratedBy(health, this_health, endTime)
        doc.wasDerivedFrom(health, resource, this_health, this_health, this_health)

        repo.logout()
                  
        return doc

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
