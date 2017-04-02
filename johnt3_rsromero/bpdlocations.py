import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class bpdlocations(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = []
    writes = ['johnt3_rsromero.bpdlocations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')        

        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bpdlocations")
        repo.createCollection("bpdlocations")
        repo['johnt3_rsromero.bpdlocations'].insert_many(r)
        repo['johnt3_rsromero.bpdlocations'].metadata({'complete':True})
        print(repo['johnt3_rsromero.bpdlocations'].metadata())

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

        this_script = doc.agent('alg:johnt3_rsromero#bpdlocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Boston Police Department Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bpdlocations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bpdlocations, this_script)
        doc.usage(get_bpdlocations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        bpdlocations = doc.entity('dat:johnt3_rsromero#bpdlocations', {prov.model.PROV_LABEL:'Boston Police Department Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpdlocations, this_script)
        doc.wasGeneratedBy(bpdlocations, get_bpdlocations, endTime)
        doc.wasDerivedFrom(bpdlocations, resource, get_bpdlocations, get_bpdlocations, get_bpdlocations)

        repo.logout()
                  
        return doc

##bpdlocations.execute()
##doc = bpdlocations.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
