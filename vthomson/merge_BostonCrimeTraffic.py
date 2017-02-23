import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_BostonCrimeTraffic(dml.Algorithm):
    contributor = 'vthomson'
    reads = ['vthomson.boston_traffic', 'vthomson.boston_crime']
    writes = ['vthomson.bostonCrimeTraffic']


    '''
    Find the common denominator between the two sets --> Boston
    Aggregate data based on Boston
    Both Commonwealth Connect and Waze Traffic have street addresses
    Combine the same streets and show their corresponding Commonwealth Connect complaints
    '''

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vthomson', 'vthomson')
        repo.dropPermanent('vthomson.bostonCrimeTraffic')
        repo.createPermanent('vthomson.bostonCrimeTraffic')


        # START OF TRANSFORMATION

        traffic = repo['vthomson.boston_traffic']
        crime = repo['vthomson.boston_crime']

        traffic_array = []
        count = 0
        for document in traffic.find():
            if count == 0:
                continue
            traffic_array.append({"City":document['city'], "Street":document['street']})
            repo['vthomson.bostonCrimeTraffic'].insert_one(traffic_array)
            count += 1
        
        crime_array = []
        for document in crime.find():
            crime_array.append({"Incident":document['incident_type_description'], "Street":document['streetname'], }) #"Weapon":document['weapontype'], "Shooting":document['shooting'],
            repo['vthomson.bostonCrimeTraffic'].insert_one(crime_array)

        # END OF TRANSFORMATION


        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format. 
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format. 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log. 
        doc.add_namespace('bdp', 'https://data.city.ofboston.gov/resource/')

        this_script = doc.agent('alg:vthomson#bostonCrimeTraffic', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_bostonCrimeTraffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonCrimeTraffic, this_script)

        resource1 = doc.entity('dat:vthomson#boston_crime', {'prov:label':'Boston Crime Incident Reports', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_bostonCrimeTraffic, resource1, startTime)

        resource2 = doc.entity('dat:vthomson#boston_traffic', {'prov:label':'Waze Jam Data', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_bostonCrimeTraffic, resource2, startTime)

        bostonCrimeTraffic = doc.entity('dat:vthomson#bostonCrimeTraffic', {'prov:label':'Traffic Jams and Crime in Boston', prov.model.PROV_TYPE:'ont:Dataset'})
        
        doc.wasAttributedTo(bostonCrimeTraffic, this_script)
        doc.wasGeneratedBy(bostonCrimeTraffic, get_bostonCrimeTraffic, endTime)
        doc.wasDerivedFrom(bostonCrimeTraffic, resource1, get_bostonCrimeTraffic, get_bostonCrimeTraffic, get_bostonCrimeTraffic)
        doc.wasDerivedFrom(bostonCrimeTraffic, resource2, get_bostonCrimeTraffic, get_bostonCrimeTraffic, get_bostonCrimeTraffic)

        #repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

merge_BostonCrimeTraffic.execute()
doc = merge_BostonCrimeTraffic.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))    