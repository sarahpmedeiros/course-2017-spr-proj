# mergeCrime.py

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeCrime(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = ['cxiao_jchew1.crime_reports', 'cxiao_jchew1.firearm_recovery']
    writes = ['cxiao_jchew1.crime_data']

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
        CR = repo['cxiao_jchew1.crime_reports'].find()
        FR = repo['cxiao_jchew1.firearm_recovery'].find()

        # projection
        crimeRate = []
        for i in CR:
            try:
                crimeRate.append({'offense_code_group': i['offense_code_group'],
                                    'offense_description': i['offense_description']})
            except:
                pass

        firearmRecovery = []
        for i in FR:
            try:
                firearmRecovery.append({'crimegunsrecovered': i['crimegunsrecovered'],
                                        'gunssurrenderedsafeguarded': i['gunssurrenderedsafeguarded']})
            except:
                pass

        # aggregation
        crimeData = crimeRate + firearmRecovery

        repo.dropCollection("crime_data")
        repo.createCollection("crime_data")
        repo['cxiao_jchew1.crime_data'].insert_many(crimeData)
        repo['cxiao_jchew1.crime_data'].metadata({'complete': True})
        print("Saved crime_data", repo['cxiao_jchew1.crime_data'].metadata())

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

        this_script = doc.agent('alg:cxiao_jchew1#mergeCrime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_crimeRate = doc.entity('dat:cxiao_jchew1#crime_rate',
                                             {'prov:label': 'Crime Rate',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_firearmRecovery = doc.entity('dat:cxiao_jchew1#firearm_recovery',
                                             {'prov:label': 'Firearm Recovery',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crimeData = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimeData, this_script)
        doc.usage(get_crimeData, resource_crimeRate, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_crimeData, resource_firearmRecovery, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        crimeData = doc.entity('dat:cxiao_jchew1#crime_data',
                          {prov.model.PROV_LABEL: 'Crime Data',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crimeData, this_script)
        doc.wasGeneratedBy(crimeData, get_crimeData, endTime)
        doc.wasDerivedFrom(crimeData, resource_crimeRate, get_crimeData, get_crimeData, get_crimeData)
        doc.wasDerivedFrom(crimeData, resource_firearmRecovery, get_crimeData, get_crimeData, get_crimeData)
        
        repo.logout()

        return doc

mergeCrime.execute()
doc = mergeCrime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
