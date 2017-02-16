import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps

class combineAllOpenSpaces(dml.Algorithm):

    contributor = 'billy108_zhou13'
    reads = ['billy108_zhou13.communityGardens','billy108_zhou13.openSpaceCambridge','billy108_zhou13.openSpaceBoston']
    writes = ['billy108_zhou13.allOpenSpacesInBoston']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')

        # Get the collections
        openSpaceCambridge = repo['billy108_zhou13.openSpaceCambridge']
        openSpaceBoston = repo['billy108_zhou13.openSpaceBoston']
        communityGardens = repo['billy108_zhou13.communityGardens']

        #Get names, neighborhood and zipcode of all seasonal pools and put them into (key, value) form
        allOpenSpaces_list = []
        for entry in openSpaceCambridge.find():
            allOpenSpaces_list.append(
                {"name": entry['name'], 'neighborhood': 'cambridge'}
            )

        for entry in openSpaceBoston.find():
            allOpenSpaces_list.append(
                {"name": entry['properties'].get('SITE_NAME'), 'neighborhood': entry['properties'].get('DISTRICT').lower()}
            )

        for entry in communityGardens.find():
            if entry['site'] != 'Site name' and entry['area'] != 'Area':
                allOpenSpaces_list.append(
                    {"name": entry['site'], 'neighborhood': entry['area']}
                )




        # Create a new collection and insert the result data set
        repo.dropCollection('allOpenSpacesInBoston')
        repo.createCollection('allOpenSpacesInBoston')
        repo['billy108_zhou13.allOpenSpacesInBoston'].insert_many(allOpenSpaces_list)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        return

combineAllOpenSpaces.execute()