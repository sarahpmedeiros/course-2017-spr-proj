import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
from shapely.geometry import shape

class append_polygon_and_centerpoint(dml.Algorithm):
    contributor = 'chamathd'
    reads = ['chamathd.neighborhood_pop', \
             'chamathd.neighborhood_area_boston', \
             'chamathd.neighborhood_area_cambridge']
    writes = ['chamathd.neighborhood_info']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chamathd', 'chamathd')

        # Perform initialization for the new repository
        colName = "chamathd.neighborhood_info"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Retrieve data from the neighborhood collection
        print("Retrieving data from the neighborhood collection")
        nhood_col = repo["chamathd.neighborhood_pop"].find().limit(50)
        print()

        # Retrieve polygon data for Boston neighborhoods
        print("Retrieving polygon data from the Boston neighborhood area collection")
        boston_area_col = repo["chamathd.neighborhood_area_boston"].find().limit(50)
        print("Unionizing Boston polygon data into collection", colName)
        for polygon in boston_area_col:
            polyName = polygon["properties"]["name"]

            # Get centerpoint information using Shapely library
            shapelyPoly = shape(polygon["geometry"])
            shapelyCenter = shapelyPoly.centroid
            x, y = shapelyCenter.x, shapelyCenter.y

            # Iterate through our neighborhood collection to find a matching name
            for nhood in nhood_col:
                if nhood["name"] == polyName:
                    newDict = nhood
                    newDict["geometry"] = polygon["geometry"]
                    newDict["center_x"] = x
                    newDict["center_y"] = y
                    repo[colName].insert(newDict)
            
        print("Finished unionizing Boston data to", colName)
        print()

        # Retrieve polygon data for Boston neighborhoods
        print("Retrieving polygon data from the Cambridge neighborhood area collection")
        cambridge_area_col = repo["chamathd.neighborhood_area_cambridge"].find().limit(50)
        print("Unionizing Cambridge polygon data into collection", colName)
        for polygon in cambridge_area_col:
            polyName = polygon["properties"]["name"]

            # Get centerpoint information using Shapely library
            shapelyPoly = shape(polygon["geometry"])
            shapelyCenter = shapelyPoly.centroid
            x, y = shapelyCenter.x, shapelyCenter.y

            # Iterate through our neighborhood collection to find a matching name
            for nhood in nhood_col:
                if nhood["name"] == polyName:
                    newDict = nhood
                    newDict["geometry"] = polygon["geometry"]
                    newDict["center_x"] = x
                    newDict["center_y"] = y
                    repo[colName].insert(newDict)
            
        print("Finished unionizing Cambridge data to", colName)
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
        repo.authenticate('chamathd', 'chamathd')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cha', 'chamathd')

        this_script = doc.agent('alg:chamathd#append_polygon_and_centerpoint', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_area_boston_res = doc.entity('cha:neighborhood_area_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_area_cambridge_res = doc.entity('cha:neighborhood_area_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

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

        neighborhood_info = doc.entity('dat:chamathd#neighborhood_info', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_info, this_script)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_boston, endTime)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_cambridge, endTime)
        
        repo.logout()
                  
        return doc

append_polygon_and_centerpoint.execute()
doc = append_polygon_and_centerpoint.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
