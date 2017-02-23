import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class getTickets(dml.Algorithm):
    contributor = 'jgrishey'
    reads = []
    writes = ['jgrishey.tickets']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jgrishey', 'jgrishey')

        client = sodapy.Socrata("data.cambridgema.gov", dml.auth['services']['cityofcambridgedataportal']['token'])
        response = client.get("vnxa-cuyr", limit=500)

        tickets = []

        ID = 0

        for ticket in response:
            if 'location' in ticket:
                lat = ticket['location']['latitude']
                lon = ticket['location']['longitude']
                tickets.append({'_id': ID, 'lat': lat, 'long': lon}) if (lat != '0' and lon != '0') else ()
                ID += 1

        repo.dropCollection("tickets")
        repo.createCollection("tickets")

        for ticket in tickets:
            repo['jgrishey.tickets'].insert(ticket)

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

        this_script = doc.agent('alg:jgrishey#getTickets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cdp:vnxa-cuyr', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_tickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_tickets, this_script)
        doc.usage(get_tickets, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(get_tickets, resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:DataSet',
                    'ont:Computation': 'Apply ID, get latitude, and get longitude'})
        tickets = doc.entity('dat:jgrishey#tickets', {prov.model.PROV_LABEL:'Cambridge Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tickets, this_script)
        doc.wasGeneratedBy(tickets, get_tickets, endTime)
        doc.wasDerivedFrom(tickets, resource, get_tickets, get_tickets, get_tickets)

        repo.logout()

        return doc
'''
getTickets.execute()
doc = getTickets.provenance()
with open('getTicketsProvenance.json', 'w') as file:
    json.dump(json.loads(doc.serialize()), file, indent=4)
'''

## eof
