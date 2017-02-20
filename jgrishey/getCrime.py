import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class getCrime(dml.Algorithm):
    contributor = 'jgrishey'
    reads = []
    writes = ['jgrishey.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        client = sodapy.Socrata("data.cityofboston.gov", dml.auth["services"]["cityofbostondataportal"]["token"])
        response = client.get("29yf-ye7n", limit=10000)

        crimes = []

        ID = 0

        for crime in response:
            lat = crime['location']['coordinates'][1]
            lon = crime['location']['coordinates'][0]
            crimes.append({'_id': ID, 'lat': lat, 'long': lon}) if (lat != '0' and lon != 0) else ()
            ID += 1

        repo.dropCollection("crime")
        repo.createCollection("crime")

        for crime in crimes:
            repo['jgrishey.crime'].insert(crime)

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jgrishey#getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_crime, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:DataSet',
                    'ont:Computation': 'Apply ID, get latitude, and get longitude'})

        crime = doc.entity('dat:jgrishey#crime', {prov.model.PROV_LABEL:'Boston Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc
'''
getCrime.execute()
doc = getCrime.provenance()
with open('getCrimeProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
    '''

## eof
