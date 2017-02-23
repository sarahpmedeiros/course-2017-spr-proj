import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_CambridgeConnectTraffic(dml.Algorithm):
    contributor = 'vthomson'
    reads = ['vthomson.commonwealth_connect', 'vthomson.cambridge_traffic']
    writes = ['vthomson.cambridgeConnectTraffic']


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
        repo.dropPermanent('vthomson.cambridgeConnectTraffic')
        repo.createPermanent('vthomson.cambridgeConnectTraffic')


        # START OF TRANSFORMATION

        connect = repo['vthomson.commonwealth_connect']
        traffic = repo['vthomson.cambridge_traffic']

        connect_array = []
        for document in connect.find():
            connect_array.append({"city":document['city'], "complaint":document['issue_type'], "details":document['issue_description'], "street":document['address']})
            repo['vthomson.cambridgeConnectTraffic'].insert_one(connect_array)

        traffic_array = []
        for document in traffic.find():
            traffic_array.append({"street":document['primarystreet'], "AM peak":document['AMPeak2014'], "PM peak":document['PMPeak2014']})
            repo['vthomson.cambridgeConnectTraffic'].insert_one(traffic_array) 

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
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')

        this_script = doc.agent('alg:vthomson#cambridgeConnectTraffic', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_cambridgeConnectTraffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cambridgeConnectTraffic, this_script)

        resource1 = doc.entity('dat:vthomson#commonwealth_connect', {'prov:label':'Commonwealth Connect reports within Massachusetts State', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_cambridgeConnectTraffic, resource1, startTime)

        resource2 = doc.entity('dat:vthomson#cambridge_traffic', {'prov:label':'Average Daily Traffic Counts', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(get_cambridgeConnectTraffic, resource2, startTime)

        cambridgeConnectTraffic = doc.entity('dat:vthomson#cambridgeConnectTraffic', {'prov:label':'Traffic Jams and Street Complaints in Cambridge', prov.model.PROV_TYPE:'ont:Dataset'})
        
        doc.wasAttributedTo(cambridgeConnectTraffic, this_script)
        doc.wasGeneratedBy(cambridgeConnectTraffic, get_cambridgeConnectTraffic, endTime)
        doc.wasDerivedFrom(cambridgeConnectTraffic, resource1, get_cambridgeConnectTraffic, get_cambridgeConnectTraffic, get_cambridgeConnectTraffic)
        doc.wasDerivedFrom(cambridgeConnectTraffic, resource2, get_cambridgeConnectTraffic, get_cambridgeConnectTraffic, get_cambridgeConnectTraffic)

        #repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

merge_CambridgeConnectTraffic.execute()
doc = merge_CambridgeConnectTraffic.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))