import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class redlineStreets(dml.Algorithm):
    contributor = 'jgrishey'
    reads = ['jgrishey.redlineStations']
    writes = ['jgrishey.redlineStreets']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        stations = list(repo['jgrishey.redlineStations'].find(None,['_id', 'lat', 'lon']))

        for station in stations:
            url = "http://api.geonames.org/findNearbyStreetsJSON?lat=%s&lng=%s&username=%s" % (station['lat'], station['lon'], dml.auth['services']['geonamesdataportal']['username'])
            response = urllib.request.urlopen(url).read().decode("utf-8")
            station['streets'] = []
            if 'streetSegment' in json.loads(response):
                for street in json.loads(response)['streetSegment']:
                    station['streets'].append(street['name']) if street['name'] != '' else ()

        repo.dropCollection('redlineStreets')
        repo.createCollection('redlineStreets')

        with open('redlineStreets.json', 'w') as file:
            json.dump(stations, file, indent=4)

        for station in stations:
            repo['jgrishey.redlineStreets'].insert(station)


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
        doc.add_namespace('geo', 'http://api.geonames.org/')

        this_script = doc.agent('alg:jgrishey#redlineStreets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('geo:findNearbyStreetsJSON', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        redlineStations = doc.entity('dat:jgrishey#redlineStations', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, redlineStations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                    'ont:Query': 'lat=&lng=&username=',
                    'ont:Computation': 'Get Streets and append to corresponding station'})

        streetsStations = doc.entity('dat:jgrishey#redlineStreets', {prov.model.PROV_LABEL:'Red Line Nearby Streets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetsStations, this_script)
        doc.wasGeneratedBy(streetsStations, this_run, endTime)
        doc.wasDerivedFrom(streetsStations, resource, redlineStations, this_run, this_run)

        repo.logout()

        return doc
'''
redlineStreets.execute()
doc = redlineStreets.provenance()
with open('redlineStreetsProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
'''

## eof
