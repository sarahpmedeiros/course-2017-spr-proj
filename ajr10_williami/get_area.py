import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps

class get_area(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = ['ajr10_williami.open_spaces_cambridge',\
             'ajr10_williami.open_spaces_boston']
    writes = ['ajr10_williami.area_spaces_cambridge',\
              'ajr10_williami.area_spaces_boston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # Perform cleaning transformation here
        
        repo.dropCollection('ajr10_williami.area_spaces_cambridge')
        repo.createCollection('ajr10_williami.area_spaces_cambridge')

        repo.dropCollection('ajr10_williami.area_spaces_boston')
        repo.createCollection('ajr10_williami.area_spaces_boston')

        open_spaces_cambridge = repo["ajr10_williami.open_spaces_cambridge"].find().limit(50)
        open_spaces_boston = repo["ajr10_williami.open_spaces_boston"].find().limit(50)

        for cambridge_area in open_spaces_cambridge:
            area = cambridge_area['shape_area']

            new_area = {}
            new_area["area"] = area
            repo['ajr10_williami.area_spaces_cambridge'].insert(new_area)

        for boston_area in open_spaces_boston:
            area = boston_area['properties']['ShapeSTArea']

            new_area = {}
            new_area["area"] = area
            print(new_area)
            repo['ajr10_williami.area_spaces_boston'].insert(new_area)

        # logout and return start and end times
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
        doc.add_namespace('awc', 'ajr10_williami')

        this_script = doc.agent('alg:ajr10_williami#get_area', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        areas_cambridge_resource = doc.entity('awc:open_spaces_cambridge', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        areas_boston_resource = doc.entity('awc:open_spaces_boston', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_areas_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_areas_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_areas_cambridge, this_script)
        doc.wasAssociatedWith(get_areas_boston, this_script)

        doc.usage(get_areas_cambridge, areas_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Areas+Cambridge'
                  }
                  )
        doc.usage(get_areas_boston, areas_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Areas+Boston'
                  }
                  )

        areas_cambridge = doc.entity('dat:ajr10_williami#area_spaces_cambridge', {prov.model.PROV_LABEL:'Areas Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(areas_cambridge, this_script)
        doc.wasGeneratedBy(areas_cambridge, get_areas_cambridge, endTime)
        doc.wasDerivedFrom(areas_cambridge, areas_cambridge_resource, get_areas_cambridge, get_areas_cambridge, get_areas_cambridge)

        areas_boston = doc.entity('dat:ajr10_williami#area_spaces_boston', {prov.model.PROV_LABEL:'Areas Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(areas_boston, this_script)
        doc.wasGeneratedBy(areas_boston, get_areas_boston, endTime)
        doc.wasDerivedFrom(areas_boston, areas_boston_resource, get_areas_boston, get_areas_boston, get_areas_boston)

        repo.logout()

        return doc

'''
get_area.execute()

doc = get_area.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
