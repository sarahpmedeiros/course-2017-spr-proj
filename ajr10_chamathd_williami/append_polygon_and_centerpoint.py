import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
from shapely.geometry import shape

class append_polygon_and_centerpoint(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.neighborhood_pop', \
             'ajr10_chamathd_williami.neighborhood_area_boston', \
             'ajr10_chamathd_williami.neighborhood_area_cambridge']
    writes = ['ajr10_chamathd_williami.neighborhood_info']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.neighborhood_info"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Retrieve polygon data for Boston neighborhoods
        print("Retrieving polygon data from the Boston neighborhood area collection")
        boston_area_col = repo["ajr10_chamathd_williami.neighborhood_area_boston"].find().limit(5) if trial else repo["ajr10_chamathd_williami.neighborhood_area_boston"].find().limit(50) 
        print("Unionizing Boston polygon data into collection", colName)
        for polygon in boston_area_col:
            polyName = polygon["properties"]["name"]

            # Get centerpoint information using Shapely library
            shapelyPoly = shape(polygon["geometry"])
            shapelyCenter = shapelyPoly.centroid
            x, y = shapelyCenter.x, shapelyCenter.y

            # Iterate through our neighborhood collection to find a matching name
            # Retrieve data from the neighborhood collection
            nhood_col = repo["ajr10_chamathd_williami.neighborhood_pop"].find().limit(5) if trial else repo["ajr10_chamathd_williami.neighborhood_pop"].find().limit(50)
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
        cambridge_area_col = repo["ajr10_chamathd_williami.neighborhood_area_cambridge"].find().limit(5) if trial else repo["ajr10_chamathd_williami.neighborhood_area_cambridge"].find().limit(50)
        print("Unionizing Cambridge polygon data into collection", colName)
        for polygon in cambridge_area_col:
            polyName = polygon["properties"]["name"]

            # Get centerpoint information using Shapely library
            shapelyPoly = shape(polygon["geometry"])
            shapelyCenter = shapelyPoly.centroid
            x, y = shapelyCenter.x, shapelyCenter.y

            # Iterate through our neighborhood collection to find a matching name
            # Retrieve data from the neighborhood collection
            nhood_col = repo["ajr10_chamathd_williami.neighborhood_pop"].find().limit(5) if trial else repo["ajr10_chamathd_williami.neighborhood_pop"].find().limit(50)
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
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('acw', 'ajr10_chamathd_williami')

        this_script = doc.agent('alg:ajr10_chamathd_williami#append_polygon_and_centerpoint', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_area_boston_res = doc.entity('acw:neighborhood_area_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_area_cambridge_res = doc.entity('acw:neighborhood_area_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

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

        neighborhood_info = doc.entity('dat:ajr10_chamathd_williami#neighborhood_info', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_info, this_script)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_boston, endTime)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_cambridge, endTime)
        doc.wasDerivedFrom(neighborhood_info, neighborhood_area_boston_res, get_neighborhood_area_boston, get_neighborhood_area_boston, get_neighborhood_area_boston)
        doc.wasDerivedFrom(neighborhood_info, neighborhood_area_cambridge_res, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge, get_neighborhood_area_cambridge)

        
        repo.logout()
                  
        return doc

##append_polygon_and_centerpoint.execute()
##doc = append_polygon_and_centerpoint.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
