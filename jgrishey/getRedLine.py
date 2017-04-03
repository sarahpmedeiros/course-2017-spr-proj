import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class getRedLine(dml.Algorithm):
    contributor = 'jgrishey'
    reads = []
    writes = ['jgrishey.redlineStations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        url = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=%s&route=Red&format=json" % (dml.auth['services']['mbtadeveloperportal']['token'])
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        line = r['direction'][0]['stop']
        res = []
        for stop in line:
            res.append({'_id': stop['parent_station_name'], 'lat': stop['stop_lat'], 'lon': stop['stop_lon']})

        checked = []
        for x in res:
            if x not in checked:
                checked.append(x)

        res = checked

        repo.dropCollection("redlineStations")
        repo.createCollection("redlineStations")

        for dic in res:
            repo['jgrishey.redlineStations'].insert(dic)

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
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

        this_script = doc.agent('alg:jgrishey#getRedLine', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbta:stopsbyroute', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': '?api_key=&route=Red&format=json'})
        doc.usage(get_stations, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:DataSet',
                    'ont:Computation': 'Get ID, get latitude, and get longitude'})

        stations = doc.entity('dat:jgrishey#redlineStations', {prov.model.PROV_LABEL:'Red Line Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, resource, get_stations, get_stations, get_stations)

        repo.logout()

        return doc
'''
getRedLine.execute()
doc = getRedLine.provenance()
with open('getRedLineProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
    '''

## eof
