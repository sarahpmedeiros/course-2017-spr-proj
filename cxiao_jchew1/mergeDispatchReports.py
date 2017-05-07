# mergeDispatchReports.py

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeDispatchReports(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = ['cxiao_jchew1.crime_reports', 'cxiao_jchew1.dispatch_counts']
    writes = ['cxiao_jchew1.dispatch_data']

    @staticmethod
    def execute(trial = False):
        ' Merge data sets'
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')

        # loads the collection
        CR = repo['cxiao_jchew1.crime_reports'].find()
        DC = repo['cxiao_jchew1.dispatch_counts'].find()
        
        # projection
        crimeReports = []
        for i in CR:
            try:
                crimeReports.append({'offense_code': i['offense_code'],
                                      'occurred_on_date': i['occurred_on_date'],
                                     'day_of_week': i['day_of_week']})
            except:
                pass

        dispatchCounts = []
        for i in DC:
            try:
                dispatchCounts.append({'date': i['date'],
                                      'bpd': i['bpd'],
                                       'bfd': i['bfd']})
            except:
                pass

        # aggregation
        dispatchData = crimeReports + dispatchCounts

        repo.dropCollection("dispatch_data")
        repo.createCollection("dispatch_data")
        repo['cxiao_jchew1.dispatch_data'].insert_many(dispatchData)
        repo['cxiao_jchew1.dispatch_data'].metadata({'complete': True})
        print("Saved dispatch_data", repo['cxiao_jchew1.dispatch_data'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:cxiao_jchew1#mergeDispatchReports',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_crimeReports = doc.entity('dat:cxiao_jchew1#crime_reports',
                                             {'prov:label': 'Crime Reports',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_dispatchCounts = doc.entity('dat:cxiao_jchew1#dispatch_counts',
                                             {'prov:label': 'Dispatch Counts',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_dispatchData = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_dispatchData, this_script)
        doc.usage(get_dispatchData, resource_crimeReports, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_dispatchData, resource_dispatchCounts, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        dispatchData = doc.entity('dat:cxiao_jchew1#dispatch_data',
                          {prov.model.PROV_LABEL: 'Dispatch Data',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(dispatchData, this_script)
        doc.wasGeneratedBy(dispatchData, get_dispatchData, endTime)
        doc.wasDerivedFrom(dispatchData, resource_crimeReports, get_dispatchData, get_dispatchData, get_dispatchData)
        doc.wasDerivedFrom(dispatchData, resource_dispatchCounts, get_dispatchData, get_dispatchData, get_dispatchData)
        
        repo.logout()

        return doc

mergeDispatchReports.execute()
doc = mergeDispatchReports.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

