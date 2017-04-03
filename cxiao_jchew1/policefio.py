import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = []
    writes = ['cxiao_jchew1.police_fio']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')

        url = 'https://data.cityofboston.gov/resource/2pem-965w.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("police_fio")
        repo.createCollection("police_fio")
        repo['cxiao_jchew1.police_fio'].insert_many(r)
        repo['cxiao_jchew1.police_fio'].metadata({'complete':True})
        print(repo['cxiao_jchew1.police_fio'].metadata())

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
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cxiao_jchew1#policefio', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_police = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_police, this_script)
        doc.usage(get_police, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'$select=SEQ_NUM,FIO_ID,SEX,LOCATION,DIST,DIST_ID,FIO_DATE,FIO_TIME,PRIORS,DESCRIPTION,CLOTHING,COMPLEXION,FIOFS_TYPE,TERRORISM,SEARCH,BASIS,STOP_REASONS,ENTEREDBY,FIOFS_REASONS,OUTCOME,VEH_MAKE,VEH_YEAR_NUM,VEH_COLOR,VEH_MODEL,VEH_OCCUPANT,VEH_STATE,SUPERVISOR_ID,OFFICER_ID,SUPERVISOR,OFF_DIST_ID,OFF_DIST,OFFICER,SUP_ENTRYDATE,LAST_UPDATEBY,LAST_UPDATETIME,ETHNICITY,FIRST_INSERTTIME,ACTIVE_ID,RACE_ID,RACE_DESC,FIO_DATE_CORRECTED,AGE_AT_FIO_CORRECTED,STREET_ID,CITY'
                  }
                  )

        fio = doc.entity('dat:cxiao_jchew1#policefio', {prov.model.PROV_LABEL:'Police FIO', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fio, this_script)
        doc.wasGeneratedBy(fio, get_police, endTime)
        doc.wasDerivedFrom(fio, resource, get_police, get_police, get_police)

        repo.logout()
                  
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
