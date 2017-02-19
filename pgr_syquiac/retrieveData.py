'''
Pauline Ramirez & Carlos Syquia

'''


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class retrieveData(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = []
    writes = ['pgr_syquiac.hospitals', 'pgr_syquiac.cdc', 'pgr_syquiac.schools','pgr_syquiac.pools']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        # Get data for hospitals
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("u6fv-m8v4", limit=30)
        repo.dropCollection("hospitals")
        repo.createCollection("hospitals")
        repo['pgr_syquiac.hospitals'].insert_many(response)

        # Get data for CDC 500 cities
        client = sodapy.Socrata("chronicdata.cdc.gov", None)
        response = client.get("csmm-fdhi", CityName="Boston",
         GeographicLevel="Census Tract", limit=5000)
        print(len(response))
        repo.dropCollection("cdc")
        repo.createCollection("cdc")
        repo['pgr_syquiac.cdc'].insert_many(response)

        # Get data for all universities in the US
        url = 'http://datamechanics.io/data/pgr_syquiac/universities.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['pgr_syquiac.schools'].insert_many(r)


        # Get data for Open Swimming Pools in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("5jxx-wfpr", limit=1)
        #print(response)
        repo.dropCollection("pools")
        repo.createCollection("pools")
        repo['pgr_syquiac.pools'].insert_many(response)

        # Need to add the healthy corner stores, this will be combined with pools for the obesity rates



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
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')

        this_script = doc.agent('alg:pgr_syquiac#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        hospitalsResource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getHospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getHospitals, this_script)
        doc.usage(getHospitals, hospitalsResource, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval'
        	}
        )

        cdcResource = doc.entity('cdc:csmm-fdhi', {'prov:label':'500 Cities: Local Data for Better Health', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getCDC = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getCDC, this_script)
        doc.usage(getCDC, cdcResource, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval'
        	}
        )

        openspacesResource = doc.entity('cdp:5ctr-ccas', {'prov:label':'Open Space', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getOpenSpaces = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getOpenSpaces, this_script)
        doc.usage(getOpenSpaces, openspacesResource, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval'
        	}
        )
        '''
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        # This is for when u create new data

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        '''
        repo.logout()
                  
        return doc

retrieveData.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof