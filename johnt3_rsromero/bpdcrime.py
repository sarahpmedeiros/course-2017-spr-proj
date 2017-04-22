import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class bpdcrime(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = []
    writes = ['johnt3_rsromero.bpdcrime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')


        url = 'http://datamechanics.io/data/johnt3_rsromero/bpdcrime.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bpdcrime")
        repo.createCollection("bpdcrime")
        repo['johnt3_rsromero.bpdcrime'].insert_many(r)
        repo['johnt3_rsromero.bpdcrime'].metadata({'complete':True})
        print(repo['johnt3_rsromero.bpdcrime'].metadata())

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/') #city of boston data portal

        this_script = doc.agent('alg:johnt3_rsromero#bpdcrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        bpdcrime_resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Boston Police Department Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_bpdcrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_bpdcrime, this_script)
        
        doc.usage(get_bpdcrime, bpdcrime_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Boston+Crime'
                  }
                  )

        bpdcrime = doc.entity('dat:johnt3_rsromero#bpdcrime', {prov.model.PROV_LABEL:'Boston Police Department Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpdcrime, this_script)
        doc.wasGeneratedBy(bpdcrime, get_bpdcrime, endTime)
        doc.wasDerivedFrom(bpdcrime, bpdcrime_resource, get_bpdcrime, get_bpdcrime, get_bpdcrime)

        repo.logout()
                  
        return doc

##bpdcrime.execute()
##doc = bpdcrime.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
