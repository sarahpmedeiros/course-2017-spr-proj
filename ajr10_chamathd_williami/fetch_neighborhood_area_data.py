import requests
import json
import dml
import prov.model
import datetime
import uuid

class fetch_neighborhood_area_data(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = []
    writes = ['ajr10_chamathd_williami.neighborhood_area_boston', \
              'ajr10_chamathd_williami.neighborhood_area_cambridge']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Neighborhood geospatial data for the Boston area from City of Boston Data Portal
        print("Fetching Boston neighborhood geospatial data from City of Boston Data Portal")

        colName = "ajr10_chamathd_williami.neighborhood_area_boston"

        url = "https://data.cityofboston.gov/api/geospatial/mcme-sgsz?method=export&format=GeoJSON"
        response = requests.get(url).text
        
        r = json.loads(response)
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many(r["features"])
        print("Finished writing data to", colName)
        print()

        # Neighborhood geospatial data for the Cambridge area from Cambridge Open Data
        print("Fetching Cambridge neighborhood geospatial data from Cambridge Open Data")

        colName = "ajr10_chamathd_williami.neighborhood_area_cambridge"
        
        url = "https://data.cambridgema.gov/api/geospatial/4ys2-ebga?method=export&format=GeoJSON"
        response = requests.get(url).text
        
        r = json.loads(response)
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many(r["features"])
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
        doc.add_namespace('cob', 'https://data.cityofboston.gov/api/geospatial/')
        doc.add_namespace('cod', 'https://data.cambridgema.gov/api/geospatial/')

        this_script = doc.agent('alg:ajr10_chamathd_williami#fetch_neighborhood_area_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_area_boston_res = doc.entity('cob:mcme-sgsz?method=export&format=GeoJSON', {'prov:label':'Boston Open Budget - Neighborhood Boundaries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_area_cambridge_res = doc.entity('cod:4ys2-ebga?method=export&format=GeoJSON', {'prov:label':'Cambridge Neighborhood Polygons', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_area_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood_area_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_area_boston, this_script)
        doc.wasAssociatedWith(get_neighborhood_area_cambridge, this_script)
        
        doc.usage(get_neighborhood_area_boston, neighborhood_area_boston_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Boston'
                  }
                  )
        doc.usage(get_neighborhood_area_cambridge, neighborhood_area_cambridge_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Cambridge'
                  }
                  )

        neighborhood_area_boston = doc.entity('dat:ajr10_chamathd_williami#neighborhood_area_boston', {prov.model.PROV_LABEL:'Boston Open Budget - Neighborhood Boundaries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_area_boston, this_script)
        doc.wasGeneratedBy(neighborhood_area_boston, get_neighborhood_area_boston, endTime)
        doc.wasDerivedFrom(neighborhood_area_boston, neighborhood_area_boston_res, get_neighborhood_area_boston, get_neighborhood_area_boston, get_neighborhood_area_boston)

        neighborhood_area_cambridge = doc.entity('dat:ajr10_chamathd_williami#neighborhood_area_cambridge', {prov.model.PROV_LABEL:'Cambridge Neighborhood Polygons', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_area_cambridge, this_script)
        doc.wasGeneratedBy(neighborhood_area_cambridge, get_neighborhood_area_cambridge, endTime)
        doc.wasDerivedFrom(neighborhood_area_cambridge, neighborhood_area_cambridge_res, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge)

        repo.logout()
                  
        return doc

##fetch_neighborhood_area_data.execute()
##doc = fetch_neighborhood_area_data.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
