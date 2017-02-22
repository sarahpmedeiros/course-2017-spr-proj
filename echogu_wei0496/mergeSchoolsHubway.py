# mergeSchoolsHubway.py
# clean up and merge Boston schools data set with Hubway Stations data set
# for each school, determine the number of Hubway stations nearby

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty

class mergeSchoolsHubway(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = ['echogu_wei0496.MASchools', 'echogu_wei0496.HubwayStations']
    writes = ['echogu_wei0496.SchoolsHubway']

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
        def product(R, S):
            return [(t, u) for t in R for u in S]

        p = product(BostonSchools, HubwayStations)
        p = [{'_id': item[0]['_id'],
                    'properties': item[0]['properties'],
                    'location': item[0]['location'],                        # school locations
                    'stations': item[1]['stations']} for item in p]   # Hubway stations location coordinates

        # aggregation: for each school, count the number of hubway stations within 500m walk
        SchoolsHubway = []
        keys = {item['_id'] for item in p}
        for key in keys:
            count = 0
            for item in p:
                if item['_id'] == key:
                    school = item['location']['coordinates'][1], item['location']['coordinates'][0]
                    station = item['stations'][1], item['stations'][0]
                    d = vincenty(school, station).meters
                    if d < 500:
                        count += 1
            SchoolsHubway.append({'_id': key,
                                  'properties': item['properties'],
                                  'location': item['location'],
                                  'CountStations': count})

        repo.dropCollection("SchoolsHubway")
        repo.createCollection("SchoolsHubway")
        repo['echogu_wei0496.SchoolsHubway'].insert_many(SchoolsHubway)
        repo['echogu_wei0496.SchoolsHubway'].metadata({'complete': True})
        print("Saved SchoolsHubway", repo['echogu_wei0496.SchoolsHubway'].metadata())

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
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:echogu_wei0496#mergeSchoolsHubway',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_MASchools = doc.entity('dat:echogu_wei0496#MASchools',
                                             {'prov:label': 'MA Schools',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_HubwayStations = doc.entity('dat:echogu_wei0496#HubwayStations',
                                               {'prov:label': 'Hubway Stations',
                                                prov.model.PROV_TYPE: 'ont:DataSet'})

        get_SchoolsHubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_SchoolsHubway, this_script)
        doc.usage(get_SchoolsHubway, resource_MASchools, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_SchoolsHubway, resource_HubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        SchoolsHubway = doc.entity('dat:echogu_wei0496#SchoolsHubway',
                                 {prov.model.PROV_LABEL: 'Schools Hubway Stations',
                                  prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(SchoolsHubway, this_script)
        doc.wasGeneratedBy(SchoolsHubway, get_SchoolsHubway, endTime)
        doc.wasDerivedFrom(SchoolsHubway, resource_MASchools, get_SchoolsHubway, get_SchoolsHubway, get_SchoolsHubway)
        doc.wasDerivedFrom(SchoolsHubway, resource_HubwayStations, get_SchoolsHubway, get_SchoolsHubway, get_SchoolsHubway)

        repo.logout()

        return doc

# mergeSchoolsHubway.execute()
# doc = mergeSchoolsHubway.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
