# Downloads the Boston neighborhood geojson

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class neighborhoods(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.neighborhoods']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        # Download neighborhood geojson
        url = 'https://data.cityofboston.gov/resource/pbfk-2wv3.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo['jguerero_mgarcia7.neighborhoods'].insert_many(r)
        repo['jguerero_mgarcia7.neighborhoods'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.neighborhoods'].metadata())

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

        this_script = doc.agent('alg:jguerero_mgarcia7#neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:pbfk-2wv3', {'prov:label':'Boston Neighborhood Shapefile', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighborhoods, this_script)
        doc.usage(get_neighborhoods, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )


        neighborhoods = doc.entity('dat:jguerero_mgarcia7#neighborhoods', {prov.model.PROV_LABEL:'Boston Neighborhood Shapefile', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, get_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, resource, get_neighborhoods, get_neighborhoods, get_neighborhoods)

        repo.logout()
                  
        return doc


'''
neighborhoods.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
