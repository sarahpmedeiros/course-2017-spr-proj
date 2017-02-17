import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps

class clean_trees(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = ['ajr10_williami.trees_cambridge',\
             'ajr10_williami.trees_boston']
    writes = ['ajr10_williami.cleaned_trees_cambridge',\
              'ajr10_williami.cleaned_trees_boston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # Perform cleaning transformation here
        
        repo.dropCollection('ajr10_williami.cleaned_trees_cambridge')
        repo.createCollection('ajr10_williami.cleaned_trees_cambridge')

        repo.dropCollection('ajr10_williami.cleaned_trees_boston')
        repo.createCollection('ajr10_williami.cleaned_trees_boston')

        trees_cambridge = repo["ajr10_williami.trees_cambridge"].find()
        trees_boston = repo["ajr10_williami.trees_boston"].find().limit(50)

        for cambridge_tree in trees_cambridge:
            coords = cambridge_tree['the_geom']['coordinates']

            new_tree = {}
            new_tree["longitude"] = coords[0]
            new_tree["latitude"] = coords[1]
            repo['ajr10_williami.cleaned_trees_cambridge'].insert(new_tree)

        for boston_tree in trees_boston:
            coords = boston_tree['geometry']['coordinates']

            new_tree = {}
            new_tree["longitude"] = coords[0]
            new_tree["latitude"] = coords[1]
            repo['ajr10_williami.cleaned_trees_boston'].insert(new_tree)

        # logout and return start and end times
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
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('cob', 'data.cityofboston.gov/')

        '''
        this_script = doc.agent('alg:ajr10_williami#clean_trees', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        trees_cambridge_resource = doc.entity('cdp:q83f-7quz', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        trees_boston_resource = doc.entity('bod:ce863d38db284efe83555caf8a832e2a_1.geojson', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_trees_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trees_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_trees_cambridge, this_script)
        doc.wasAssociatedWith(get_trees_boston, this_script)

        doc.usage(get_trees_cambridge, trees_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Trees+Cambridge'
                  }
                  )
        doc.usage(get_trees_boston, trees_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Trees+Boston'
                  }
                  )

        trees_cambridge = doc.entity('dat:ajr10_williami#trees_cambridge', {prov.model.PROV_LABEL:'Trees Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trees_cambridge, this_script)
        doc.wasGeneratedBy(trees_cambridge, get_trees_cambridge, endTime)
        doc.wasDerivedFrom(trees_cambridge, trees_cambridge_resource, get_trees_cambridge, get_trees_cambridge, get_trees_cambridge)

        trees_boston = doc.entity('dat:ajr10_williami#trees_boston', {prov.model.PROV_LABEL:'Trees Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trees_boston, this_script)
        doc.wasGeneratedBy(trees_boston, get_trees_boston, endTime)
        doc.wasDerivedFrom(trees_boston, trees_boston_resource, get_trees_boston, get_trees_boston, get_trees_boston)

        '''
        repo.logout()

        return doc

clean_trees.execute()

doc = clean_trees.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
