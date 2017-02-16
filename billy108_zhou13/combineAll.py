import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps

class combineAll(dml.Algorithm):

    contributor = 'billy108_zhou13'
    reads = ['billy108_zhou13.waterplayCambridge','billy108_zhou13.allOpenSpacesInBoston','billy108_zhou13.allPoolsInBoston']
    writes = ['billy108_zhou13.allRecreationalPlaces']

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

        #Get names, neighborhood of all open spaces in Cambridge
        allOpenSpaces_list = []
        for entry in openSpaceCambridge.find():
            allOpenSpaces_list.append(
                {"name": entry['name'], 'neighborhood': 'cambridge'}
            )

        #get names, neighborhood of all open spaces in Boston excluding Cambrdige and Brookline
        for entry in openSpaceBoston.find():

            if entry['properties'].get('DISTRICT').lower() == 'allston-brighton':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'allston/brighton'})
            elif entry['properties'].get('DISTRICT').lower() == 'north dorchester':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'dorchester'})
            elif entry['properties'].get('DISTRICT').lower() == 'central boston':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'boston'})
            else:
                allOpenSpaces_list.append(
                    {"name": entry['properties'].get('SITE_NAME'),
                     'neighborhood': entry['properties'].get('DISTRICT').lower()}
                )

        #get names, neighborhood of all community gardens in Boston
        for entry in communityGardens.find():
            if entry['site'] != 'Site name' and entry['area'] != 'Area':
                if entry['area'].lower() == 'allston' or entry['area'].lower() == 'brighton':
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'allston/brighton'}
                    )
                elif entry['area'].lower() == 'back bay' or entry['area'].lower() == 'beacon hill' :
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'back bay/beacon hill'}
                    )
                elif entry['area'].lower() == 'mattapan or dorchester':
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'mattapan'}
                    )
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'dorchester'}
                    )
                else:
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': entry['area'].lower()}
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

combineAll.execute()