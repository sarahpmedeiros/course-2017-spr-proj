import requests
import json
import dml
import prov.model
import datetime
import uuid

class fetch_sea_level_data(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = []
    writes = ['ajr10_chamathd_williami.sea_level_five', \
              'ajr10_chamathd_williami.sea_level_seven']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Sea level data for 5-foot rise
        print("Fetching five-foot sea level data from Boston ArcGIS Open Data")

        colName = "ajr10_chamathd_williami.sea_level_five"
        
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/4ebe1cce95c04125a89315615724f4b9_0.geojson"
        response = requests.get(url).text
        
        r = json.loads(response)
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many([{'geometry': {'type': 'Polygon', 'coordinates': polygon}} for polygon in r["features"][0]["geometry"]["coordinates"]])
        print("Finished writing data to", colName)
        print()

        # Sea level data for 7.5-foot rise
        print("Fetching seven-foot sea level data from Boston ArcGIS Open Data")

        colName = "ajr10_chamathd_williami.sea_level_seven"
        
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/bfe3e93c27004a69921b629b92cd1f9f_0.geojson"
        response = requests.get(url).text
        
        r = json.loads(response)
        
        repo.dropCollection(colName)
        repo.createCollection(colName)

        print("Inserting JSON data into collection", colName)
        repo[colName].insert_many([{'geometry': {'type': 'Polygon', 'coordinates': polygon}} for polygon in r["features"][0]["geometry"]["coordinates"]])
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
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:ajr10_chamathd_williami#fetch_sea_level_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        sea_level_five_res = doc.entity('bod:4ebe1cce95c04125a89315615724f4b9_0.geojson', {'prov:label':'Sea Level Rise Plus 5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        sea_level_seven_res = doc.entity('bod:bfe3e93c27004a69921b629b92cd1f9f_0.geojson', {'prov:label':'Sea Level Rise Plus 7.5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_sea_level_five = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_sea_level_seven = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_sea_level_five, this_script)
        doc.wasAssociatedWith(get_sea_level_seven, this_script)
        
        doc.usage(get_sea_level_five, sea_level_five_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Sea+Level+Five'
                  }
                  )
        doc.usage(get_sea_level_seven, sea_level_seven_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Sea+Level+Seven'
                  }
                  )

        sea_level_five = doc.entity('dat:ajr10_chamathd_williami#sea_level_five', {prov.model.PROV_LABEL:'Sea Level Rise Plus 5 Feet', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sea_level_five, this_script)
        doc.wasGeneratedBy(sea_level_five, get_sea_level_five, endTime)
        doc.wasDerivedFrom(sea_level_five, sea_level_five_res, get_sea_level_five, get_sea_level_five, get_sea_level_five)

        sea_level_seven = doc.entity('dat:ajr10_chamathd_williami#sea_level_seven', {prov.model.PROV_LABEL:'Sea Level Rise Plus 7.5 Feet', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sea_level_seven, this_script)
        doc.wasGeneratedBy(sea_level_seven, get_sea_level_seven, endTime)
        doc.wasDerivedFrom(sea_level_seven, sea_level_seven_res, get_sea_level_seven, get_sea_level_seven, get_sea_level_seven)

        repo.logout()
                  
        return doc

##fetch_sea_level_data.execute()
##doc = fetch_sea_level_data.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
