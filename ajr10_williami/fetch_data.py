import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid

class fetch_data(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = []
    writes = ['ajr10_williami.open_spaces_cambridge',\
              'ajr10_williami.trees_cambridge',\
              'ajr10_williami.open_spaces_boston',\
              'ajr10_williami.trees_boston',\
              'ajr10_williami.energy_boston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        
        # open_spaces_cambridge

        print("retrieving open spaces data from data.cambridgema.gov")

        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas", limit=10)
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        
        repo.dropCollection("ajr10_williami.open_spaces_cambridge")
        repo.createCollection("ajr10_williami.open_spaces_cambridge")

        print("inserting data into target: ", "open_spaces_cambridge")
        repo["ajr10_williami.open_spaces_cambridge"].insert_many(r)

        # trees_cambridge

        print("retrieving tree data from data.cambridgema.gov")

        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("q83f-7quz", limit=10)
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        
        repo.dropCollection("ajr10_williami.trees_cambridge")
        repo.createCollection("ajr10_williami.trees_cambridge")

        print("inserting data into target: ", "trees_cambridge")
        repo["ajr10_williami.trees_cambridge"].insert_many(r)

        # open_spaces_boston

        print("retrieving tree data from bostonopendata-boston.opendata.arcgis")

        response = urllib.request.urlopen\
           ("http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson")\
           .read().decode("utf-8")
        
        r = json.loads(response)

        repo.dropCollection("ajr10_williami.open_spaces_boston")
        repo.createCollection("ajr10_williami.open_spaces_boston")

        print("inserting into target: ", "open_spaces_boston")
        repo["ajr10_williami.open_spaces_boston"].insert_many(r["features"])

        # trees_boston

        print("retrieving tree data from bostonopendata-boston.opendata.arcgis")

        response = urllib.request.urlopen\
           ('http://bostonopendata-boston.opendata.arcgis.com/datasets/ce863d38db284efe83555caf8a832e2a_1.geojson')\
           .read().decode("utf-8")
        
        r = json.loads(response)
        
        repo.dropCollection("ajr10_williami.trees_boston")
        repo.createCollection("ajr10_williami.trees_boston")

        print("inserting data into target: ", "trees_boston")
        repo["ajr10_williami.trees_boston"].insert_many(r["features"])

        # energy_boston
        
        print("retrieving tree data from data.cityofboston..gov")

        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("exmd-natm", limit=10)
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        
        repo.dropCollection("ajr10_williami.energy_boston")
        repo.createCollection("ajr10_williami.energy_boston")

        print("inserting data into target: ", "energy_boston")
        repo["ajr10_williami.energy_boston"].insert_many(r)
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

        this_script = doc.agent('alg:ajr10_williami#fetch_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        open_spaces_cambridge_resource = doc.entity('cdp:5ctr-ccas', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        trees_cambridge_resource = doc.entity('cdp:q83f-7quz', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        open_spaces_boston_resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7.geojson', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        trees_boston_resource = doc.entity('bod:ce863d38db284efe83555caf8a832e2a_1.geojson', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


        get_open_spaces_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trees_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_open_spaces_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trees_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_open_spaces_cambridge, this_script)
        doc.wasAssociatedWith(get_trees_cambridge, this_script)
        doc.wasAssociatedWith(get_open_spaces_boston, this_script)
        doc.wasAssociatedWith(get_trees_boston, this_script)

        doc.usage(get_open_spaces_cambridge, open_spaces_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Open+Spaces+Cambridge'
                  }
                  )
        doc.usage(get_trees_cambridge, trees_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Trees+Cambridge'
                  }
                  )
        doc.usage(get_open_spaces_boston, open_spaces_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Open+Spaces+Boston'
                  }
                  )
        doc.usage(get_trees_boston, trees_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Trees+Boston'
                  }
                  )

        open_spaces_cambridge = doc.entity('dat:ajr10_williami#open_spaces_cambridge', {prov.model.PROV_LABEL:'Open spaces Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_spaces_cambridge, this_script)
        doc.wasGeneratedBy(open_spaces_cambridge, get_open_spaces_cambridge, endTime)
        doc.wasDerivedFrom(open_spaces_cambridge, open_spaces_cambridge_resource, get_open_spaces_cambridge, get_open_spaces_cambridge, get_open_spaces_cambridge)

        trees_cambridge = doc.entity('dat:ajr10_williami#trees_cambridge', {prov.model.PROV_LABEL:'Trees Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trees_cambridge, this_script)
        doc.wasGeneratedBy(trees_cambridge, get_trees_cambridge, endTime)
        doc.wasDerivedFrom(trees_cambridge, trees_cambridge_resource, get_trees_cambridge, get_trees_cambridge, get_trees_cambridge)

        open_spaces_boston = doc.entity('dat:ajr10_williami#open_spaces_boston', {prov.model.PROV_LABEL:'Open spaces Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_spaces_boston, this_script)
        doc.wasGeneratedBy(open_spaces_boston, get_open_spaces_boston, endTime)
        doc.wasDerivedFrom(open_spaces_boston, open_spaces_boston_resource, get_open_spaces_boston, get_open_spaces_boston, get_open_spaces_boston)

        trees_boston = doc.entity('dat:ajr10_williami#trees_boston', {prov.model.PROV_LABEL:'Trees Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trees_boston, this_script)
        doc.wasGeneratedBy(trees_boston, get_trees_boston, endTime)
        doc.wasDerivedFrom(trees_boston, trees_boston_resource, get_trees_boston, get_trees_boston, get_trees_boston)


        repo.logout()
                  
        return doc

fetch_data.execute()

'''
doc = fetch_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
