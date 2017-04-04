import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
from shapely.geometry import shape

class aggregate_sea_level_rise(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.neighborhood_info', \
             'ajr10_chamathd_williami.sea_level_five', \
             'ajr10_chamathd_williami.sea_level_seven']
    writes = ['ajr10_chamathd_williami.neighborhood_sea_level_data']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.neighborhood_sea_level_data"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Retrieve data from the neighborhood info collection
        print("Retrieving data from the neighborhood info collection")
        nhood_info_col = repo["ajr10_chamathd_williami.neighborhood_info"].find().limit(50)
        print()

        # Retrieve polygon data for 7.5-foot sea level rise
        print("Retrieving polygon data from the Boston neighborhood area collection")
        sea_level_seven_col = repo["ajr10_chamathd_williami.sea_level_seven"].find().limit(0)
        print()

        # Iterate through each neighborhood
        print("Calculating polygon overlap information into collection", colName)
        print()
        print("===============================")
        print(" THIS MAY TAKE SEVERAL MINUTES ")
        print("===============================")
        print()
        for nhood in nhood_info_col:
            # Initialize our severity index to zero for now
            severity_index = 0
            
            # Generate a Shapely polygon for the neighborhood
            nhoodPoly = shape(nhood["geometry"])
            nhoodArea = nhoodPoly.area

            # Retrieve polygon data for 5-foot sea level rise
            print("Testing", nhood["name"], "against 5-foot sea level rise")
            sea_level_five_col = repo["ajr10_chamathd_williami.sea_level_five"].find().limit(0)
            intersect = False
            intersectArea = 0
            intersectRatio = 0.0
            estPeopleAffectedFive = 0
            for polygon in sea_level_five_col:
                seaPoly = shape(polygon["geometry"])
                if nhoodPoly.intersects(seaPoly):
                    intersectPoly = nhoodPoly.intersection(seaPoly)
                    intersectArea += intersectPoly.area
                    intersect = True
            if intersect:
                intersectRatio = intersectArea / nhoodArea
                # Normalize for dataset overlap
                if intersectRatio > 1:
                    intersectRatio = 1.0
                estPeopleAffectedFive = int(nhood["population"] * intersectRatio)
                print(nhood["name"], "IN", nhood["city"], "WILL EXPERIENCE FLOODING @ 5 FEET WITH RATIO", \
                     round(intersectRatio, 3), "AND ABOUT", estPeopleAffectedFive, "PEOPLE AFFECTED")

            newDict = nhood
            newDict["intersect_ratio_five"] = intersectRatio
            newDict["estimated_people_affected_five"] = estPeopleAffectedFive

            # Retrieve polygon data for 7.5-foot sea level rise
            print("Testing", nhood["name"], "against 7.5-foot sea level rise")
            sea_level_seven_col = repo["ajr10_chamathd_williami.sea_level_seven"].find().limit(0)
            intersect = False
            intersectArea = 0
            intersectRatio = 0.0
            estPeopleAffectedSeven = 0
            for polygon in sea_level_seven_col:
                seaPoly = shape(polygon["geometry"])
                if nhoodPoly.intersects(seaPoly):
                    intersectPoly = seaPoly.intersection(nhoodPoly)
                    intersectArea += intersectPoly.area
                    intersect = True
            if intersect:
                intersectRatio = intersectArea / nhoodArea
                # Normalize for dataset overlap
                if intersectRatio > 1:
                    intersectRatio = 1.0
                estPeopleAffectedSeven = int(nhood["population"] * intersectRatio)
                print(nhood["name"], "IN", nhood["city"], "WILL EXPERIENCE FLOODING @ 7.5 FEET WITH RATIO", \
                      round(intersectRatio, 3), "AND ABOUT", estPeopleAffectedSeven, "PEOPLE AFFECTED")
            
            newDict["intersect_ratio_seven"] = intersectRatio
            newDict["estimated_people_affected_seven"] = estPeopleAffectedSeven

            # Calculate severity index with greater weight on people who
            # get flooded at 5 feet
            severity_index = ((estPeopleAffectedFive * 2) + (estPeopleAffectedSeven - estPeopleAffectedFive)) / 1000
            newDict["severity_index"] = severity_index
            print("SEVERITY INDEX OF", nhood["name"], "FLOODING IS", severity_index)
            print()

            repo[colName].insert(newDict)
            
        print("Finished aggregating sea level overlap to", colName)
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

        this_script = doc.agent('alg:ajr10_chamathd_williami#aggregate_sea_level_rise', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_info_res = doc.entity('acw:neighborhood_info', {'prov:label':'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        sea_level_five_res = doc.entity('acw:sea_level_five', {'prov:label':'Sea Level Rise Plus 5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        sea_level_seven_res = doc.entity('acw:sea_level_seven', {'prov:label':'Sea Level Rise Plus 7.5 Feet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_info = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_sea_level_five = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_sea_level_seven = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_info, this_script)
        doc.wasAssociatedWith(get_sea_level_five, this_script)
        doc.wasAssociatedWith(get_sea_level_seven, this_script)

        doc.usage(get_neighborhood_info, neighborhood_info_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Info'
                  }
                  )
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

        neighborhood_sea_level_data = doc.entity('dat:ajr10_chamathd_williami#neighborhood_sea_level_data', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Sea Level Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_sea_level_data, this_script)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_neighborhood_info, endTime)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_sea_level_five, endTime)
        doc.wasGeneratedBy(neighborhood_sea_level_data, get_sea_level_seven, endTime)
        doc.wasDerivedFrom(neighborhood_sea_level_data, neighborhood_info_res, get_neighborhood_info, get_neighborhood_info, get_neighborhood_info)
        doc.wasDerivedFrom(neighborhood_sea_level_data, sea_level_five_res, get_sea_level_five, get_sea_level_five, get_sea_level_five)
        doc.wasDerivedFrom(neighborhood_sea_level_data, sea_level_seven_res, get_sea_level_seven, get_sea_level_seven, get_sea_level_seven)

        
        repo.logout()
                  
        return doc

##aggregate_sea_level_rise.execute()
##doc = aggregate_sea_level_rise.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
