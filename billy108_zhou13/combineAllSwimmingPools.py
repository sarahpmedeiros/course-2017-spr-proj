import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps

class combineAllSwimmingPools(dml.Algorithm):

    contributor = 'billy108_zhou13'
    reads = ['billy108_zhou13.seasonalSwimPools','billy108_zhou13.commCenterPools']
    writes = ['billy108_zhou13.allPoolsInBoston']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')

        # Get the collections
        seasonalPools = repo['billy108_zhou13.seasonalSwimPools']
        commCenterPools = repo['billy108_zhou13.commCenterPools']

        #Get names, neighborhood and zipcode of all seasonal pools and put them into (key, value) form
        allPools_list = []
        for entry in seasonalPools.find({ "location_1_city" : {"$exists": True}}):
            allPools_list.append(
                {"name": entry['business_name'], "value": {'neighborhood': entry['location_1_city'].lower(),
                                                           'zip': entry['location_1_zip']}}
            )


        for tuple in allPools_list:
            if(tuple.get('value').get('neighborhood') == 'allston' or tuple.get('value').get('neighborhood') == 'brighton'):
                tuple.update({'value':{'neighborhood':'allston / brighton'}})



        for entry in commCenterPools.find():
            if(entry['properties'].get('NEIGH').lower() == 'jamaica plai'):
                allPools_list.append(
                    {"name": entry['properties'].get('SITE'),
                     "value": {'neighborhood': 'jamaica plain',
                               "zip": (str(0) + str(entry['properties'].get('ZIP')))}}
                )
            else:
                allPools_list.append(
                    {"name": entry['properties'].get('SITE'),
                     "value": {'neighborhood': entry['properties'].get('NEIGH').lower(),
                               "zip": (str(0) + str(entry['properties'].get('ZIP')))}}
                )

        # Create a new collection and insert the result data set
        repo.dropCollection('allPoolsInBoston')
        repo.createCollection('allPoolsInBoston')
        repo['billy108_zhou13.allPoolsInBoston'].insert_many(allPools_list)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        return

combineAllSwimmingPools.execute()