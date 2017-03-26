import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class getCrashes(dml.Algorithm):
    contributor = 'jgrishey'
    reads = []
    writes = ['jgrishey.crashes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        client = sodapy.Socrata("data.cambridgema.gov", dml.auth['services']['cityofcambridgedataportal']['token'])
        response = client.get("ybny-g9cv", limit=500)

        crashes = []

        ID = 0

        for crash in response:
            if 'latitude' in crash and 'longitude' in crash:
                lat = crash['latitude']
                lon = crash['longitude']
                crashes.append({'_id': ID, 'lat': lat, 'long': lon}) if (lat != '' and lon != '') else ()
                ID += 1

        repo.dropCollection("crashes")
        repo.createCollection("crashes")

        for crash in crashes:
            repo['jgrishey.crashes'].insert(crash)

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
        repo.authenticate('jgrishey', 'jgrishey')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cdp', 'http://data.cambridgema.gov/')

        this_script = doc.agent('alg:jgrishey#getCrashes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cdp:ybny-g9cv', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crashes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crashes, this_script)
        doc.usage(get_crashes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(get_crashes, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:DataSet',
                    'ont:Computation': 'Apply ID, get latitude, and get longitude'})

        crashes = doc.entity('dat:jgrishey#crashes', {prov.model.PROV_LABEL:'Cambridge Crashes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crashes, this_script)
        doc.wasGeneratedBy(crashes, get_crashes, endTime)
        doc.wasDerivedFrom(crashes, resource, get_crashes, get_crashes, get_crashes)

        repo.logout()

        return doc
'''
getCrashes.execute()
doc = getCrashes.provenance()
with open('getCrashesProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
    '''

## eof
