# mergePolice.py

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergePolice(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = ['cxiao_jchew1.police_stations', 'cxiao_jchew1.police_fio']
    writes = ['cxiao_jchew1.police_data']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')

        # loads the collection
        PS = repo['cxiao_jchew1.police_stations'].find()
        PF = repo['cxiao_jchew1.police_fio'].find()
        
        # projection
        policeStations = []
        for i in PS:
            try:
                policeStations.append({'name': i['name'],
                                      'location': i['location']})
            except:
                pass

        policeFIO = []
        for i in PF:
            try:
                policeFIO.append({'city': i['city']})
            except:
                pass

        # aggregation
        policeData = policeStations + policeFIO

        repo.dropCollection("police_data")
        repo.createCollection("police_data")
        repo['cxiao_jchew1.police_data'].insert_many(policeData)
        repo['cxiao_jchew1.police_data'].metadata({'complete': True})
        print("Saved police_data", repo['cxiao_jchew1.police_data'].metadata())

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

        this_script = doc.agent('alg:cxiao_jchew1#mergePolice',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_policeStations = doc.entity('dat:cxiao_jchew1#police_stations',
                                             {'prov:label': 'Police Stations',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_policeFIO = doc.entity('dat:cxiao_jchew1#police_fio',
                                             {'prov:label': 'Police FIO',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_policeData = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_policeData, this_script)
        doc.usage(get_policeData, resource_policeStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_policeData, resource_policeFIO, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        policeData = doc.entity('dat:cxiao_jchew1#police_data',
                          {prov.model.PROV_LABEL: 'Police Data',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(policeData, this_script)
        doc.wasGeneratedBy(policeData, get_policeData, endTime)
        doc.wasDerivedFrom(policeData, resource_policeStations, get_policeData, get_policeData, get_policeData)
        doc.wasDerivedFrom(policeData, resource_policeFIO, get_policeData, get_policeData, get_policeData)
        
        repo.logout()

        return doc

mergePolice.execute()
doc = mergePolice.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
