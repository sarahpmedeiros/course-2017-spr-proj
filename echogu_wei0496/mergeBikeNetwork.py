# mergeBikeNetwork - merge city of boston and cambridge bike network data sets

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from echogu_wei0496 import transformData

class mergeBikeNetwork(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.BostonNetwork', 'echogu_wei0496.CambridgeNetwork']
    writes = ['echogu_wei0496.BikeNetwork']

    @staticmethod
    def execute(trial = False):
        ''' Clean up and merge some data sets.
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # loads the collection
        rawBostonNetwork = repo['echogu_wei0496.BostonNetwork'].find()
        rawCambridgeNetwork = repo['echogu_wei0496.CambridgeNetwork'].find()

        # projection
        BostonNetwork = []
        for item in rawBostonNetwork:
            try:
                BostonNetwork.append({'_id': item['_id'],
                                      'street': item['properties']['STREET_NAM'],
                                      'geometry': item['geometry'],
                                      'shape_len': item['properties']['Shape_Leng']})
            except:
                pass

        CambridgeNetwork = []
        for item in rawCambridgeNetwork:
            try:
                CambridgeNetwork.append({'_id': item['_id'],
                                     'street': item['street'],
                                     'geometry': item['the_geom'],
                                     'shape_len': item['shape_len']})
            except:
                pass

        # aggregation
        BikeNetwork = transformData.union(BostonNetwork, CambridgeNetwork)

        repo.dropCollection("BikeNetwork")
        repo.createCollection("BikeNetwork")
        repo['echogu_wei0496.BikeNetwork'].insert_many(BikeNetwork)
        repo['echogu_wei0496.BikeNetwork'].metadata({'complete': True})
        print("Saved BikeNetwork", repo['echogu_wei0496.BikeNetwork'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:echogu_wei0496#mergeBikeNetwork',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        # get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # doc.wasAssociatedWith(get_found, this_script)
        # doc.wasAssociatedWith(get_lost, this_script)
        # doc.usage(get_found, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )
        # doc.usage(get_lost, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )
        #
        # lost = doc.entity('dat:echogu_wei0496#lost',
        #                   {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
        #
        # found = doc.entity('dat:echogu_wei0496#found',
        #                    {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(found, this_script)
        # doc.wasGeneratedBy(found, get_found, endTime)
        # doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
        #
        repo.logout()

        return doc

mergeBikeNetwork.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
