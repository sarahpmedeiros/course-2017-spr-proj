import json
import dml
import prov.model
import datetime
import random
import uuid
from numpy.random import choice
from shapely.geometry import Polygon, Point, LinearRing, shape

def closest_point_on_border(border, pt):
    poly = Polygon(border)
    point = Point(pt)

    pol_ext = LinearRing(poly.exterior.coords)
    d = pol_ext.project(point)
    p = pol_ext.interpolate(d)
    closest_point_coords = list(p.coords)[0]
    return closest_point_coords

class safe_points(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.k_means']
    writes = ['ajr10_chamathd_williami.safe_points']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.safe_points"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Operational code here:

        means_data = repo["ajr10_chamathd_williami.k_means"]

        safe_points_five = []
        safe_points_seven = []
        
        print("Calculating safe points for five foot rise...")
        kmeans = [[-71.01576279, 42.37262541], [-71.10382837, 42.36778836] ,[-71.13631852, 42.2733972 ], [-71.1414882, 42.379961  ], [-71.06424473, 42.35630297], [-71.10116486, 42.32545693], [-70.9692239, 42.32885585], [-71.07482066, 42.28979033]]
        sea_level_five_col = repo["ajr10_chamathd_williami.sea_level_five"].find().limit(0)
        for polygon in sea_level_five_col:
            seaPoly = shape(polygon["geometry"])
            for mean in kmeans:
                kmean = Point(mean[0], mean[1])
                if seaPoly.contains(kmean):
                    safe_point = closest_point_on_border(seaPoly, kmean)
                    safe_points_five += [safe_point]
                    print("Safe point", safe_point, "Original", kmean.xy)

        print("Calculating safe points for seven foot rise...")
        sea_level_seven_col = repo["ajr10_chamathd_williami.sea_level_seven"].find().limit(0)
        for polygon in sea_level_seven_col:
            seaPoly = shape(polygon["geometry"])
            for mean in kmeans:
                kmean = Point(mean[0], mean[1])
                if seaPoly.contains(kmean):
                    safe_point = closest_point_on_border(seaPoly, kmean)
                    safe_points_seven += [safe_point]
                    print("Safe point", safe_point, "Original", kmean.xy)

        print("Saving data to safe_points")

        pts_five = {"safe_points_five": safe_points_five}
        pts_seven = {"safe_points_seven": safe_points_seven}

        repo["ajr10_chamathd_williami.safe_points"].insert(pts_five)
        repo["ajr10_chamathd_williami.safe_points"].insert(pts_seven)

        # Logout and end
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
        
        repo.logout()
                  
        return doc

safe_points.execute()
##doc = safe_points.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))                                                                                                                                           print("Safe point", safe_point, "Original", kmean.xy

## eof
