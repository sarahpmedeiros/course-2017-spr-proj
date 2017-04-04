import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_BostonConnectTraffic(dml.Algorithm):
    contributor = 'vthomson'
    reads = ['vthomson.commonwealth_connect', 'vthomson.boston_traffic']
    writes = ['vthomson.bostonConnectTraffic']

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
        repo.dropPermanent('vthomson.bostonConnectTraffic')
        repo.createPermanent('vthomson.bostonConnectTraffic')


        # START OF TRANSFORMATION

        connect = repo['vthomson.commonwealth_connect']
        traffic = repo['vthomson.boston_traffic']

        connect_array = []
        for document in connect.find():
            connect_array.append({"city":document['city'], "complaint":document['issue_type'], "details":document['issue_description'], "street":document['address']})
            repo['vthomson.bostonConnectTraffic'].insert_one(connect_array)

        traffic_array = []
        count = 0
        for document in traffic.find():
            if count == 0:
                continue
            traffic_array.append({"city":document['city'], "street":document['street'], "time":document['inject_date']})
            repo['vthomson.bostonConnectTraffic'].insert_one(traffic_array) 
            count +=1

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

        this_script = doc.agent('alg:vthomson#bostonConnectTraffic', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_bostonConnectTraffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonConnectTraffic, this_script)

        resource1 = doc.entity('dat:vthomson#commonwealth_connect', {'prov:label':'Commonwealth Connect reports within Massachusetts State', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_bostonConnectTraffic, resource1, startTime)

        resource2 = doc.entity('dat:vthomson#boston_traffic', {'prov:label':'Waze Jam Data', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_bostonConnectTraffic, resource2, startTime)

        bostonConnectTraffic = doc.entity('dat:vthomson#bostonConnectTraffic', {'prov:label':'Traffic Jams and Street Complaints in Boston', prov.model.PROV_TYPE:'ont:Dataset'})
        
        doc.wasAttributedTo(bostonConnectTraffic, this_script)
        doc.wasGeneratedBy(bostonConnectTraffic, get_bostonConnectTraffic, endTime)
        doc.wasDerivedFrom(bostonConnectTraffic, resource1, get_bostonConnectTraffic, get_bostonConnectTraffic, get_bostonConnectTraffic)
        doc.wasDerivedFrom(bostonConnectTraffic, resource2, get_bostonConnectTraffic, get_bostonConnectTraffic, get_bostonConnectTraffic)

        #repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

merge_BostonConnectTraffic.execute()
doc = merge_BostonConnectTraffic.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




