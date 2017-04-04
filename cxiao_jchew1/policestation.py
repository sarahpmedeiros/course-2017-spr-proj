import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = []
    writes = ['cxiao_jchew1.police_stations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient() #starts the client
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1') #authenticates directory containing data

        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json' # API key that contains the data
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("police_stations")
        repo.createCollection("police_stations")
        repo['cxiao_jchew1.police_stations'].insert_many(r)
        repo['cxiao_jchew1.police_stations'].metadata({'complete':True})
        print(repo['cxiao_jchew1.police_stations'].metadata())

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
        client = dml.pymongo.MongoClient() # some thingys that format the data
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cxiao_jchew1#policestation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}) # name of script that needs to be changed depending on data set
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_police = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) # change variable based on topic pertaining to data (in this case: police)
        doc.wasAssociatedWith(get_police, this_script) #get_police
        doc.usage(get_police, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'$select=NAME,FTSQFT,CENTROIDX,CENTROIDY,Location'
                  } # ^the column names for the tables (be specific and accurate!)
                  )

        police = doc.entity('dat:cxiao_jchew1#policestation', {prov.model.PROV_LABEL:'Police Station', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, get_police, endTime)
        doc.wasDerivedFrom(police, resource, get_police, get_police, get_police)

        repo.logout()
                  
        return doc
#################
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

