# Download all the corner stores from City of Boston Data Portal
# PROV DONE

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class cornerstores(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.allcornerstores']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        # Download list of all corner stores
        total_url = 'https://data.cityofboston.gov/resource/vwsn-4yhi.json'
        response = urllib.request.urlopen(total_url).read().decode("utf-8")
        r = json.loads(response)

        repo.dropCollection("allcornerstores")
        repo.createCollection("allcornerstores")
        repo['jguerero_mgarcia7.allcornerstores'].insert_many(r)
        repo['jguerero_mgarcia7.allcornerstores'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.allcornerstores'].metadata())

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
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jguerero_mgarcia7#cornerstores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:vwsn-4yhi', {'prov:label':'Corner Stores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cornerstores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cornerstores, this_script)
        doc.usage(get_cornerstores, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )


        allcornerstores = doc.entity('dat:jguerero_mgarcia7#allcornerstores', {prov.model.PROV_LABEL:'Corner Stores in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(allcornerstores, this_script)
        doc.wasGeneratedBy(allcornerstores, get_cornerstores, endTime)
        doc.wasDerivedFrom(allcornerstores, resource, get_cornerstores, get_cornerstores, get_cornerstores)

        repo.logout()
                  
        return doc


## eof
