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
    writes = ['ajr10_williami.open_space_cambridge',\
              'ajr10_williami.open_space_boston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        
        # open_space_cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas", limit=2)
        print("response")
        print(response)
        print()
        r = json.dumps(response, sort_keys=True, indent=2)
        print("r")
        print(r)
        print()

        r = json.loads(r)
        print("loaded r")
        print(r)
        print()

        print("type of r")
        print(type(r))
        print()     

        print("type of element r")
        print(type(r[0]))
        print()   
        
        repo.dropCollection("open_space_cambridge")
        repo.createCollection("open_space_cambridge")

        print("inserting into target: ", "open_space_cambridge")
        repo["open_space_cambridge"].insert_many(r)
        
        # # open_space_boston
        # response = urllib.request.urlopen\
        #            ("http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson")\
        #            .read().decode("utf-8")
        
        # r = json.loads(json.dumps(response, sort_keys=True, indent=2))

        # print(r)
        # repo.dropCollection("open_space_boston")
        # repo.createCollection("open_space_boston")

        # print("inserting into target: ", "open_space_boston")
        # print(type(r))

        # repo["open_space_boston"].insert_many(r)

        # # mbta

        # response = urllib.request.urlopen\
        #            ("http://realtime.mbta.com/developer/api/v2/<query>?api_key=<your api key>&format=json&<parameter>=<required/optional parameters>")



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

        this_script = doc.agent('alg:ajr10_williami#fetch_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cdp:5ctr-ccas', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_open_space_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_open_space_cambridge, this_script)
        doc.usage(get_open_space_cambridge, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  # Need to update this with correct information. Otherwise, this seems to be a valid provenance for open_space_cambridge
                  'ont:Query':'?type=Open+Spaces'
                  }
                  )

        open_space_cambridge = doc.entity('dat:ajr10_williami#open_space_cambridge', {prov.model.PROV_LABEL:'Open Space Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_space_cambridge, this_script)
        doc.wasGeneratedBy(open_space_cambridge, get_open_space_cambridge, endTime)
        doc.wasDerivedFrom(open_space_cambridge, resource, get_open_space_cambridge, get_open_space_cambridge, get_open_space_cambridge)

        repo.logout()
                  
        return doc

#fetch_data.execute()
'''
doc = fetch_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
