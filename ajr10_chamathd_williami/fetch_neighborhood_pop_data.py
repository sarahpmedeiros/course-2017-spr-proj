import requests
import sodapy
import json
import dml
import prov.model
import datetime
import uuid

class fetch_neighborhood_pop_data(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = []
    writes = ['ajr10_chamathd_williami.neighborhood_pop_boston', \
              'ajr10_chamathd_williami.neighborhood_pop_cambridge']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Neighborhood population data for the Boston area from collected data sources
        print("Fetching Boston population data from Data Mechanics resource")

        colName = "ajr10_chamathd_williami.neighborhood_pop_boston"

        url = "http://datamechanics.io/data/ajr10_chamathd_williami/boston_neighborhood_census.json"
        response = requests.get(url).text
        
        r = json.loads(response)
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many(r["neighborhoods"])
        print("Finished writing data to", colName)
        print()

        # Neighborhood population data for the Cambridge area from Cambridge Open Data
        print("Fetching Cambridge population data from Cambridge Open Data")

        colName = "ajr10_chamathd_williami.neighborhood_pop_cambridge"

        socrataClient = sodapy.Socrata("data.cambridgema.gov", None)
        response = socrataClient.get("vacj-bzri", limit=50)
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many(r)
        print("Finished writing data to", colName)
        print()

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
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cma', 'https://data.cambridgema.gov/resource/')

        this_script = doc.agent('alg:ajr10_chamathd_williami#fetch_neighborhood_pop_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_pop_boston_res = doc.entity('dat:ajr10_chamathd_williami/boston_neighborhood_census.json', {'prov:label':'Boston Neighborhood Census', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_pop_cambridge_res = doc.entity('cma:vacj-bzri', {'prov:label':'2010 Cambridge Census Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_pop_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood_pop_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_pop_boston, this_script)
        doc.wasAssociatedWith(get_neighborhood_pop_cambridge, this_script)
        
        doc.usage(get_neighborhood_pop_boston, neighborhood_pop_boston_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Pop+Boston'
                  }
                  )
        doc.usage(get_neighborhood_pop_cambridge, neighborhood_pop_cambridge_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Pop+Cambridge'
                  }
                  )

        neighborhood_pop_boston = doc.entity('dat:ajr10_chamathd_williami#neighborhood_pop_boston', {prov.model.PROV_LABEL:'Boston Open Budget - Neighborhood Boundaries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_pop_boston, this_script)
        doc.wasGeneratedBy(neighborhood_pop_boston, get_neighborhood_pop_boston, endTime)
        doc.wasDerivedFrom(neighborhood_pop_boston, neighborhood_pop_boston_res, get_neighborhood_pop_boston, get_neighborhood_pop_boston, get_neighborhood_pop_boston)

        neighborhood_pop_cambridge = doc.entity('dat:ajr10_chamathd_williami#neighborhood_pop_cambridge', {prov.model.PROV_LABEL:'Cambridge Neighborhood Polygons', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_pop_cambridge, this_script)
        doc.wasGeneratedBy(neighborhood_pop_cambridge, get_neighborhood_pop_cambridge, endTime)
        doc.wasDerivedFrom(neighborhood_pop_cambridge, neighborhood_pop_cambridge_res, get_neighborhood_pop_cambridge, get_neighborhood_pop_cambridge, get_neighborhood_pop_cambridge)

        repo.logout()
                  
        return doc

##fetch_neighborhood_pop_data.execute()
##doc = fetch_neighborhood_pop_data.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
