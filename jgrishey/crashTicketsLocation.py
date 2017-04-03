import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class crashTicketsLocation(dml.Algorithm):
    contributor = 'jgrishey'
    reads = ['jgrishey.tickets', 'jgrishey.crashes']
    writes = ['jgrishey.crashTicketsLocation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        tickets = list(repo['jgrishey.tickets'].find(None, ['_id', 'lat', 'long']))
        crashes = list(repo['jgrishey.crashes'].find(None, ['_id', 'lat', 'long']))

        res = {}

        newTickets = []

        for ticket in tickets:
            url = "http://api.geonames.org/findNearbyPlaceNameJSON?lat=%s&lng=%s&username=%s&style=SHORT" % (ticket['lat'], ticket['long'], dml.auth['services']['geonamesdataportal']['username'])
            response = urllib.request.urlopen(url).read().decode("utf-8")
            response = json.loads(response)
            if 'geonames' in response:
                ticket['place'] = response['geonames'][0]['name']
                res[ticket['place']] = {'tickets': 0, 'crashes': 0}
                newTickets.append(ticket)

        newCrashes = []

        for crash in crashes:
            url = "http://api.geonames.org/findNearbyPlaceNameJSON?lat=%s&lng=%s&username=%s&style=SHORT" % (crash['lat'], crash['long'], dml.auth['services']['geonamesdataportal']['username'])
            response = urllib.request.urlopen(url).read().decode("utf-8")
            response = json.loads(response)
            if 'geonames' in response:
                crash['place'] = response['geonames'][0]['name']
                res[crash['place']] = {'tickets': 0, 'crashes': 0}
                newCrashes.append(crash)

        for ticket in newTickets:
            res[ticket['place']]['tickets'] += 1

        for crash in newCrashes:
            res[crash['place']]['crashes'] += 1

        repo.dropCollection('crashTicketsLocation')
        repo.createCollection('crashTicketsLocation')

        with open('crashTicketsLocation.json', 'w') as file:
            json.dump(res, file, indent=4)

        repo['jgrishey.crashTicketsLocation'].insert(res)

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

        this_script = doc.agent('alg:jgrishey#crashTicketsLocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('geo:findNearbyPlaceNameJSON', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        tickets = doc.entity('dat:jgrishey#tickets', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        crashes = doc.entity('dat:jgrishey#crashes', {'prov:label':'MongoDB Request', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'mongoDB'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, crashes, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, tickets, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': 'lat=&lng=&username=',
                  'ont:Computation': 'Find closest place to each crash and ticket.'})

        crashTicketsLocation = doc.entity('dat:jgrishey#crashTicketsLocation', {prov.model.PROV_LABEL:'Cambridge Crash Tickets Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crashTicketsLocation, this_script)
        doc.wasGeneratedBy(crashTicketsLocation, this_run, endTime)
        doc.wasDerivedFrom(crashTicketsLocation, tickets, resource, crashes, this_run)

        repo.logout()

        return doc
'''
crashTicketsLocation.execute()
doc = crashTicketsLocation.provenance()
with open('crashTicketsLocationProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
    '''

## eof
