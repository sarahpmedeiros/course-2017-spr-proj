# mergeSchoolsHubway.py
# clean up and merge Boston schools data set with Hubway Stations data set
# to determine

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from echogu_wei0496 import transformData
from geopy.distance import vincenty

class mergeSchoolsHubway(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.MASchools', 'echogu_wei0496.HubwayStations']
    writes = ['echogu_wei0496.mergeSchoolsHubway']

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
        rawMASchools = repo['echogu_wei0496.MASchools'].find()
        rawHubwayStations = repo['echogu_wei0496.HubwayStations'].find()

        # projection
        BostonZIP = ['02108', '02109', '02110', '02111', '02113', '02114', '02115', '02116', '02118', '02119', '02120', '02121', '02122', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132', '02133', '02134', '02135', '02136', '02163', '02199', '02203', '02210', '02215', '02222']
        CambridgeZIP = ['02138', '02139', '02140', '02141', '02142']
        zip = BostonZIP + CambridgeZIP
        grades = "03,04,05,06,07,08,09,10,11,12"

        BostonSchools = []
        for item in rawMASchools:
            # select schools located in Boston and eliminate schools with only Pre-K, K, 01, 02 grades
            try:
                if item['properties']['ZIP'] in zip and item['properties']['GRADES'] in grades:
                    BostonSchools.append({'_id': item['_id'],
                                          'properties': item['properties'],
                                          'location': item['geometry']})
            except:
                pass

        HubwayStations = []
        for item in rawHubwayStations:
            try:
                HubwayStations.append({'stations': item['geometry']['coordinates']})
            except:
                pass

        # product and rearrange (id, properties, location, station)
        product = transformData.product(BostonSchools, HubwayStations)
        # product = [{'_id': item[0]['_id'],
        #             'properties': item[0]['properties'],
        #             'location': item[0]['location'],                        # school locations coordinates
        #             'stations': item[1]['stations']} for item in product]   # Hubway stations location coordinates
        #


        # Aggregation: for each school, count number of Hubway stations within 5/10 minutes walk (500m)
        # selection = []
        # # keys = [item['_id'] for id in project]
        # for item in product:
        #     school = item['location']['coordinates']
        #     station = item['stations']
        #     d = vincenty(school, station).meters
        #     if d < 500:
        #         selection.append(item)
        # print(selection[0])


        # def aggregate(R, f):
        #     keys = {r[0] for r in R}
        #     return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        # repo.dropCollection("SchoolsHubway")
        # repo.createCollection("SchoolsHubway")
        # repo['echogu_wei0496.SchoolsHubway'].insert_many(SchoolsHubway)
        # repo['echogu_wei0496.SchoolsHubway'].metadata({'complete': True})
        # print("Saved SchoolsHubway", repo['echogu_wei0496.SchoolsHubway'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        ''' Create the provenance document describing everything happening
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

mergeSchoolsHubway.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
