import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class transformation0(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.bpdcrime', 'johnt3_rsromero.bpdfio']
    writes = ['johnt3_rsromero.bpdcrimefio']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        bpdcrimerepo = repo.johnt3_rsromero.bpdcrime
        bpdfiorepo = repo.johnt3_rsromero.bpdfio

        ##Run Projection on Arrays
        crime = []
        fio = []
        
        for file in bpdcrimerepo.find():
            crime.append({"district":file['reptdistrict'], 'date':file['fromdate'], 'location':file['streetname'], 'description':file['incident_type_description']})
    
        for file in bpdfiorepo.find():
            fio.append({"district":file['dist'], 'date':file['fio_date'], 'location':file['location'], 'description':file['fiofs_reasons']})

        ##This is the Union Step

            
        repo.dropPermanent("bpdcrimefio")
        repo.createPermanent("bpdcrimefio")
        repo['johnt3_rsromero.bpdcrimefio'].insert_many(crime)
        repo['johnt3_rsromero.bpdcrimefio'].insert_many(fio)


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
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:johnt3_rsromero#transformation0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
##        resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Tra', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bpdcrime_fio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bpdcrime_fio, this_script)
        doc.usage(get_bpdcrime_fio, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        bpdcrimefio = doc.entity('dat:johnt3_rsromero#transformation0', {prov.model.PROV_LABEL:'Transformation0', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpdcrimefio, this_script)
        doc.wasGeneratedBy(bpdcrimefio, get_bpdcrime_fio, endTime)
        doc.wasDerivedFrom(bpdcrimefio, get_bpdcrime_fio, get_bpdcrime_fio, get_bpdcrime_fio)

        repo.logout()
                  
        return doc
    

##transformation0.execute()
##doc = bpdcrimefio.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
