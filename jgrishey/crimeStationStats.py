import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

def findClosest (x, y, stations):
    res = ""
    curr = float('inf')
    for station in stations:
        dist = math.sqrt(abs(y - float(station['lon']))**2 + abs(x - float(station['lat']))**2)
        if dist < curr:
            curr = dist
            res = station['_id']
    return res

class crimeStationStats(dml.Algorithm):
    contributor = 'jgrishey'
    reads = ['jgrishey.redlineStations', 'jgrishey.crime']
    writes = ['jgrishey.crimeStationStats']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        stations = list(repo['jgrishey.redlineStations'].find(None, ['_id', 'lat', 'lon']))
        crimes = list(repo['jgrishey.crime'].find(None, ['_id', 'lat', 'long']))

        res = {}

        for station in stations:
            res[station['_id']] = 0

        for crime in crimes:
            closest = findClosest(crime['lat'], crime['long'], stations)
            res[closest] += 1

        repo.dropCollection('crimeStationStats')
        repo.createCollection('crimeStationStats')

        with open('crimeStationStats.json', 'w') as file:
            json.dump(res, file, indent=4)

        repo['jgrishey.crimeStationStats'].insert(res)

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

        this_script = doc.agent('alg:jgrishey#crimeStationStats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        redlineStations = doc.entity('dat:jgrishey#redlineStations', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        crime = doc.entity('dat:jgrishey#crime', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, redlineStations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, redlineStations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})

        crimeStations = doc.entity('dat:jgrishey#crimeStationStats', {prov.model.PROV_LABEL:'Red Line Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeStations, this_script)
        doc.wasGeneratedBy(crimeStations, this_run, endTime)
        doc.wasDerivedFrom(crimeStations, crime, redlineStations, this_run, this_run)

        repo.logout()

        return doc
'''
crimeStationStats.execute()
doc = crimeStationStats.provenance()
with open('crimeStationStatsProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
    '''

## eof
