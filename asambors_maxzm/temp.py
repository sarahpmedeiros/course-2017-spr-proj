import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
from requests import request as rq

class temp(dml.Algorithm):
    contributor = 'asambors_maxzm'
    reads = []
    writes = ['asambors_maxzm.cdc']


    @staticmethod
    def execute(trial = False):
        #retrieve cdc dataset    
        startTime = datetime.datetime.now()
        #set up conection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asambors_maxzm','asambors_maxzm')

        url = 'http://datamechanics.io/data/asambors_maxzm/bostonTempData.json'

        print("MAKING REQUEST")
        response = urllib.request.urlopen(url).read().decode("utf-8")
        print("REQUEST DONE")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)

        # repo.dropCollection("cdc")
        # repo.createCollection("cdc")
        # repo['asambors_maxzm.cdc'].insert_many(r)
        # repo['asambors_maxzm.cdc'].metadata({'complete':True})
        # print(repo['asambors_maxzm.cdc'].metadata())

        # repo.logout()

        # endTime = datetime.datetime.now()

        # return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        ''' Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asambors_maxzm', 'asambors_maxzm')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
        )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
        )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
        repo.logout()
        return doc
    
temp.execute()
    
    
    
    
    
