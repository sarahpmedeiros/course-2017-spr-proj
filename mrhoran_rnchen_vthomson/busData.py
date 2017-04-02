import json
import dml
import prov.model
import datetime
import uuid
import ast
import sodapy

class getData(dml.Algorithm):

    contributor = 'mrhoran_rnchen_vthomson'
    reads = []
    writes = ['mrhoran_rnchen.community_gardens',
              'mrhoran_rnchen.food_pantries',
              'mrhoran_rnchen.demographics',
              'mrhoran_rnchen.medical_events',
              'mrhoran_rnchen.farmers_market']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()

        repo = client.repo
        
        repo.authenticate('mrhoran_rnchen_vthomson', 'mrhoran_rnchen_vthomson')
    
        #with open('../auth.json') as json_data:
            #credentials = json.load(json_data)

        cred = dml.auth
        
        bus_datasets = {

            "buses": "buses",
            "schools":"schools",     
            "students": "students"

	}

	### DATASETS UPLOADS #################################
        
        for dataset in city_of_boston_datasets:

            client = sodapy.Socrata("http://datamechanics.io/data/_bps_transportation_challenge", None)
            response = (client.get(bus_datasets[dataset], limit=1000))
            
            print(json.dumps(response, sort_keys=True, indent=2))
            s = json.dumps(response, sort_keys=True, indent=2)
        
            repo.dropCollection(dataset)
            repo.createCollection(dataset)
            repo['mrhoran_rnchen_vthomson.' + dataset].insert_many(response)
            repo['mrhoran_rnchen_vthomson.'+ dataset].metadata({'complete':True})
            print(type(repo['mrhoran_rnchen_vthomson.'+dataset].metadata()))

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
"""
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mrhoran_rnchen', 'mrhoran_rnchen')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:mrhoran_rnchen#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # label section might be wrong
        resource1 = doc.entity('bdp:rdqf-ter7', {'prov:label':'Community Gardens', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_community_gardens = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_community_gardens, this_script)

        doc.usage(get_community_gardens, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        resource2 = doc.entity('bdp:vwsn-4yhi', {'prov:label':'Corner Stores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_corner_stores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_corner_stores, this_script)

        doc.usage(get_corner_stores, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        resource3 = doc.entity('bdp:phr4-6r29', {'prov:label':'2010 Census Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_demographics = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_demographics, this_script)

        doc.usage(get_demographics, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        resource4 = doc.entity('bdp:x4ex-qvpn', {'prov:label':'Medical Events', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_medical_events = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_medical_events, this_script)

        doc.usage(get_medical_events, resource4, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        resource5 = doc.entity('bdp:66t5-f563', {'prov:label':'Farmers Markets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_farmers_markets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_farmers_markets, this_script)

        doc.usage(get_farmers_markets, resource5, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
    
        community_gardens = doc.entity('dat:mrhoran_rnchen#community_gardens', {prov.model.PROV_LABEL:'Community Gardens', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(community_gardens, this_script)
        doc.wasGeneratedBy(community_gardens, get_community_gardens, endTime)
        doc.wasDerivedFrom(community_gardens, resource1, get_community_gardens, get_community_gardens, get_community_gardens)

        corner_stores = doc.entity('dat:mrhoran_rnchen#corner_stores', {prov.model.PROV_LABEL:'Corner Stores', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(corner_stores, this_script)
        doc.wasGeneratedBy(corner_stores, get_community_gardens, endTime)
        doc.wasDerivedFrom(corner_stores, resource2, get_corner_stores, get_corner_stores, get_corner_stores)

        demographics = doc.entity('dat:mrhoran_rnchen#demographics', {prov.model.PROV_LABEL:'2010 Census Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(demographics, this_script)
        doc.wasGeneratedBy(demographics, get_demographics, endTime)
        doc.wasDerivedFrom(demographics, resource3, get_demographics, get_demographics, get_demographics)

        medical_events = doc.entity('dat:mrhoran_rnchen#medical_events', {prov.model.PROV_LABEL:'2010 Census Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(medical_events, this_script)
        doc.wasGeneratedBy(medical_events, get_medical_events, endTime)
        doc.wasDerivedFrom(medical_events, resource4, get_medical_events, get_medical_events, get_medical_events)

        farmers_market = doc.entity('dat:mrhoran_rnchen#farmers_market', {prov.model.PROV_LABEL:'Farmers Markets', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(farmers_market, this_script)
        doc.wasGeneratedBy(farmers_market, get_farmers_markets, endTime)
        doc.wasDerivedFrom(farmers_market, resource4, get_farmers_markets, get_farmers_markets, get_farmers_markets)

        repo.logout()
  """
                
        return doc

getData.execute()
doc = getData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
