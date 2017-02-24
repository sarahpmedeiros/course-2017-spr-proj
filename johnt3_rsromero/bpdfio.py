import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class bpdfio(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = []
    writes = ['johnt3_rsromero.bpdfio']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')


        url = 'https://data.cityofboston.gov/resource/2pem-965w.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bpdfio")
        repo.createCollection("bpdfio")
        repo['johnt3_rsromero.bpdfio'].insert_many(r)
        repo['johnt3_rsromero.bpdfio'].metadata({'complete':True})
        print(repo['johnt3_rsromero.bpdfio'].metadata())

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
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:johnt3_rsromero#bpdfio', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:2pem-965w', {'prov:label':'Boston Police Department FIO Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bpdfio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bpdfio, this_script)
        doc.usage(get_bpdfio, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
        			
                  }
                  )

        bpdfio = doc.entity('dat:johnt3_rsromero#bpdfio', {prov.model.PROV_LABEL:'Boston Police Department FIO Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpdfio, this_script)
        doc.wasGeneratedBy(bpdfio, get_bpdfio, endTime)
        doc.wasDerivedFrom(bpdfio, resource, get_bpdfio, get_bpdfio, get_bpdfio)

        repo.logout()
                  
        return doc

##bpdfio.execute()
##doc = bpdfio.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
