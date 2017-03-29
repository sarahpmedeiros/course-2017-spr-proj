import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = []
    writes = ['cxiao_jchew1.firearm_recovery']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')

        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("firearm_recovery")
        repo.createCollection("firearm_recovery")
        repo['cxiao_jchew1.firearm_recovery'].insert_many(r)
        repo['cxiao_jchew1.firearm_recovery'].metadata({'complete':True})
        print(repo['cxiao_jchew1.firearm_recovery'].metadata())

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
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cxiao_jchew1#firearmrecovery', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_log = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_log, this_script)
        doc.usage(get_log, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'$select=CollectionDate,CrimeGunsRecovered,GunsSurrenderedSafeguarded,BuybackGunsRecovered'
                  }
                  )

        firearm = doc.entity('dat:cxiao_jchew1#firearmrecovery', {prov.model.PROV_LABEL:'Firearm Recovery', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(firearm, this_script)
        doc.wasGeneratedBy(firearm, get_log, endTime)
        doc.wasDerivedFrom(firearm, resource, get_log, get_log, get_log)

        repo.logout()
                  
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
